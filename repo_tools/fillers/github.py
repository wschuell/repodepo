import datetime
import os
import psycopg2
import subprocess
import shutil
import copy
import pygit2
import logging
import numpy as np
import glob
import github
import calendar
import time
import random

import concurrent.futures
from concurrent.futures import ThreadPoolExecutor


from repo_tools import fillers
from repo_tools.fillers import generic
import repo_tools as rp

class GithubFiller(fillers.Filler):
	"""
	class to be inherited from, contains github credentials management
	"""
	def __init__(self,querymin_threshold=50,per_page=100,workers=1,api_keys_file='github_api_keys.txt',fail_on_wait=False,**kwargs):
		self.querymin_threshold = querymin_threshold
		self.per_page = per_page
		self.workers = workers
		self.api_keys_file = api_keys_file
		self.fail_on_wait = fail_on_wait
		fillers.Filler.__init__(self,**kwargs)

	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder

		self.set_github_requesters()

	def set_github_requesters(self):
		'''
		Setting github requesters
		api keys file syntax, per line: API#notes
		'''
		api_keys_file = os.path.join(self.data_folder,self.api_keys_file)
		if os.path.exists(api_keys_file):
			with open(api_keys_file,'r') as f:
				api_keys = [l.split('#')[0] for l in f.read().split('\n')]
		else:
			api_keys = []

		try:
			api_keys.append(os.environ['GITHUB_API_KEY'])
		except KeyError:
			pass

		self.github_requesters = [github.Github(per_page=self.per_page)]
		for ak in set(api_keys):
			g = github.Github(ak,per_page=self.per_page)
			try:
				g.get_rate_limit()
			except:
				self.logger.info('API key starting with "{}" and of length {} not valid'.format(ak[:5],len(ak)))
			else:
				self.github_requesters.append(g)

	def get_github_requester(self):
		'''
		Going through requesters respecting threshold of minimum remaining api queries
		'''
		if not hasattr(self,'github_requesters'):
			self.set_github_requesters()
		while True:
			for i,rq in enumerate(self.github_requesters):
				self.logger.debug('Using github requester {}, {} queries remaining'.format(i,rq.get_rate_limit().core.remaining))
				# time.sleep(0.5)
				while rq.get_rate_limit().core.remaining > self.querymin_threshold:
					yield rq
			if any(((rq.get_rate_limit().core.remaining > self.querymin_threshold) for rq in self.github_requesters)):
				continue
			elif self.fail_on_wait:
				raise IOError('All {} API keys are below the min remaining query threshold'.format(len(self.github_requesters)))
			else:
				earliest_reset = min([calendar.timegm(rq.get_rate_limit().core.reset.timetuple()) for rq in self.github_requesters])
				time_to_reset =  earliest_reset - calendar.timegm(time.gmtime())
				self.logger.info('Waiting for reset of at least one github requester, sleeping {} seconds'.format(time_to_reset+1))
				time.sleep(time_to_reset+1)


class StarsFiller(GithubFiller):
	"""
	Fills in star information
	"""
	def __init__(self,force=False,retry=False,repo_list=None,**kwargs):
		self.force = force
		self.retry = retry
		self.repo_list = repo_list
		GithubFiller.__init__(self,**kwargs)


	def apply(self):
		self.fill_stars(force=self.force,retry=self.retry,repo_list=self.repo_list,workers=self.workers)
		self.db.connection.commit()

	def fill_stars(self,force=False,retry=False,repo_list=None,workers=1,in_thread=False):
		'''
		Filling stars (only from github for the moment)
		force can be True, or an integer representing an acceptable delay in seconds for age of last update

		Checking if an entry exists in table_updates with repo_id and table_name stars

		repo syntax: (source,owner,name,repo_id,star_update)
		'''

		if repo_list is None:
			#build repo list
			repo_list = []
			for r in self.db.get_repo_list(option='starinfo'):
				# created_at = self.db.get_last_star(source=r['source'],repo=r['name'],owner=r['owner'])['created_at']
				# created_at = self.db.get_last_star(source=r[0],repo=r[2],owner=r[1])['created_at']
				# created_at = self.db.get_last_star(source=r[0],repo=r[2],owner=r[1])['created_at']
				source,owner,repo_name,repo_id,created_at,success = r[:6]

				if isinstance(created_at,str):
					created_at = datetime.datetime.strptime(created_at,'%Y-%m-%d %H:%M:%S')


				if (force==True) or (created_at is None) or ((not isinstance(force,bool)) and time.time()-created_at.timestamp()>force) or (retry and not success):
					# repo_list.append('{}/{}'.format(r['name'],r['owner']))
					# repo_list.append('{}/{}'.format(r[2],r[3]))
					repo_list.append(r)

		if workers == 1:
			requester_gen = self.get_github_requester()
			if in_thread:
				db = self.db.copy()
			else:
				db = self.db
			new_repo = True
			while len(repo_list):
				current_repo = repo_list[0]
				# owner,repo_name = current_repo.split('/')
				# source = 'GitHub'
				# repo_id = db.get_repo_id(owner=owner,source=source,name=repo_name)
				source,owner,repo_name,repo_id = current_repo[:4]
				if new_repo:
					new_repo = False
					self.logger.info('Filling stars for repo {}/{}'.format(owner,repo_name))
				requester = next(requester_gen)
				try:
					repo_apiobj = requester.get_repo('{}/{}'.format(owner,repo_name))
				except github.GithubException:
					self.logger.info('No such repository: {}/{}'.format(owner,repo_name))
					db.insert_update(repo_id=repo_id,table='stars',success=False)
					repo_list.pop(0)
					new_repo = True
				else:
					while requester.get_rate_limit().core.remaining > self.querymin_threshold:
						nb_stars = db.count_stars(source=source,repo=repo_name,owner=owner)
						# sg_list = list(repo_apiobj.get_stargazers_with_dates()[nb_stars:nb_stars+per_page])
						sg_list = list(repo_apiobj.get_stargazers_with_dates().get_page(int(nb_stars/self.per_page)))

						if nb_stars < self.per_page*(int(nb_stars/self.per_page))+len(sg_list):
							# if in_thread:
							if db.db_type == 'sqlite' and in_thread:
								time.sleep(1+random.random()) # to avoid database locked issues, and smooth a bit concurrency
							db.insert_stars(stars_list=[{'repo_id':repo_id,'source':source,'repo':repo_name,'owner':owner,'starred_at':sg.starred_at,'login':sg.user.login} for sg in sg_list],commit=False)
						else:
							self.logger.info('Filled stars for repo {}/{}: {}'.format(owner,repo_name,nb_stars))
							db.insert_update(repo_id=repo_id,table='stars',success=True)
							db.connection.commit()
							repo_list.pop(0)
							new_repo = True
							break
			if in_thread:
				db.connection.close()

		else:
			with ThreadPoolExecutor(max_workers=workers) as executor:
				futures = []
				for repo in repo_list:
					futures.append(executor.submit(self.fill_stars,repo_list=[repo],workers=1,in_thread=True))
				# for future in concurrent.futures.as_completed(futures):
				# 	pass
				for future in futures:
					future.result()



class GHLoginsFiller(GithubFiller):
	"""
	Fills in github login information
	"""
	def __init__(self,force=False,info_list=None,**kwargs):
		self.force = force
		self.info_list = info_list
		GithubFiller.__init__(self,**kwargs)



	def apply(self):
		self.fill_gh_logins(info_list=self.info_list,force=self.force)
		self.db.connection.commit()


	def fill_gh_logins(self,info_list=None,force=False,workers=1,in_thread=False):
		'''
		Associating emails to github logins using GitHub API
		force: retry emails that were previously not retrievable
		Otherwise trying all emails which have no login yet and never failed before
		'''

		if force:
			option = 'id_sha_repoinfo_all'
		else:
			option = 'id_sha_repoinfo'

		if info_list is None:
			info_list = self.db.get_user_list(option=option)

		if workers == 1:
			requester_gen = self.get_github_requester()
			if in_thread:
				db = self.db.copy()
			else:
				db = self.db
			for infos in info_list:
				user_id,repo_id,repo_owner,repo_name,commit_sha = infos
				self.logger.info('Filling gh login for user id {}'.format(user_id))
				requester = next(requester_gen)
				try:
					repo_apiobj = requester.get_repo('{}/{}'.format(repo_owner,repo_name))
					try:
						commit_apiobj = repo_apiobj.get_commit(commit_sha)
					except github.GithubException:
						self.logger.info('No such commit: {}/{}/{}'.format(repo_owner,repo_name,commit_sha))
						commit_apiobj = None
				except github.GithubException:
					self.logger.info('No such repository: {}/{}'.format(repo_owner,repo_name))
					# db.insert_update(user_id=user_id,table='stars',success=False)
				else:
					if commit_apiobj is None:
						pass
					else:
						if commit_apiobj.author is None:
							login = None
							self.logger.info('No login available for user id {}'.format(user_id))
						else:
							try:
								login = commit_apiobj.author.login
								self.logger.info('Found login {} for user id {}'.format(login,user_id))
							except github.GithubException:
								self.logger.info('No login available for user id {}, uncompletable object error'.format(user_id))
								login = None
						db.set_gh_login(user_id=user_id,login=login)
			if in_thread:
				db.connection.close()
		else:
			with ThreadPoolExecutor(max_workers=workers) as executor:
				futures = []
				for infos in info_list:
					futures.append(executor.submit(self.fill_gh_logins,info_list=[infos],workers=1,in_thread=True))
				# for future in concurrent.futures.as_completed(futures):
				# 	pass
				for future in futures:
					future.result()


class FollowersFiller(GithubFiller):
	"""
	Fills in follower information
	"""
	def __init__(self,time_delay=24*3600,login_list=None,**kwargs):
		self.login_list = login_list
		self.time_delay = time_delay
		GithubFiller.__init__(self,**kwargs)



	def apply(self):
		self.fill_followers(login_list=self.login_list,time_delay=self.time_delay)
		self.db.connection.commit()

	def fill_followers(self,login_list=None,workers=1,in_thread=False,time_delay=24*3600):
		'''
		Getting followers for github logins. Avoiding by default logins which already have a value from less than time_delay seconds ago.
		'''

		option = 'logins'

		if login_list is None:
			login_list = self.db.get_user_list(option=option,time_delay=time_delay)

		if workers == 1:
			requester_gen = self.get_github_requester()
			if in_thread:
				db = self.db.copy()
			else:
				db = self.db
			for login in login_list:
				self.logger.info('Filling followers for login {}'.format(login))
				requester = next(requester_gen)
				try:
					user_apiobj = requester.get_user('{}'.format(login))
				except github.GithubException:
					self.logger.info('No such user: {}'.format(login))
				else:
					try:
						followers = user_apiobj.followers
						self.logger.info('Login {} has {} followers'.format(login,followers))
					except github.GithubException:
						self.logger.info('No followers info available for login {}, uncompletable object error'.format(login))
						followers = None

					db.fill_followers(followers_info_list=[(login,followers)])

			if in_thread:
				db.connection.close()
				del db
		else:
			with ThreadPoolExecutor(max_workers=workers) as executor:
				futures = []
				for login in login_list:
					futures.append(executor.submit(self.fill_followers,login_list=[login],workers=1,in_thread=True))
				# for future in concurrent.futures.as_completed(futures):
				# 	pass
				for future in futures:
					future.result()
