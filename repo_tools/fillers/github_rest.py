import datetime
import os
import psycopg2
from psycopg2 import extras
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
import sqlite3
import random

import concurrent.futures
from concurrent.futures import ThreadPoolExecutor


from .. import fillers
from ..fillers import generic

from github.GithubException import UnknownObjectException,RateLimitExceededException,IncompletableObject

class GithubFiller(fillers.Filler):
	"""
	class to be inherited from, contains github credentials management
	"""
	def __init__(self,querymin_threshold=50,per_page=100,env_apikey='GITHUB_API_KEY',workers=1,identity_type='github_login',no_unauth=False,api_keys_file='github_api_keys.txt',api_keys=None,fail_on_wait=False,start_offset=None,retry=False,force=False,incremental_update=True,**kwargs):
		fillers.Filler.__init__(self,**kwargs)
		self.querymin_threshold = querymin_threshold
		self.incremental_update = incremental_update
		self.per_page = per_page
		self.workers = workers
		self.force = force
		self.retry = retry
		self.no_unauth = no_unauth
		self.api_keys_file = api_keys_file
		if api_keys is None:
			api_keys = []
		self.api_keys = copy.deepcopy(api_keys)
		self.fail_on_wait = fail_on_wait
		self.start_offset = start_offset
		self.identity_type = identity_type
		self.env_apikey = env_apikey

	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder

		self.set_api_keys()
		self.set_requesters()

		if self.db.db_type == 'postgres':
			self.db.cursor.execute(''' INSERT INTO identity_types(name) VALUES(%s) ON CONFLICT DO NOTHING;''',(self.identity_type,))
		else:
			self.db.cursor.execute(''' INSERT OR IGNORE INTO identity_types(name) VALUES(?);''',(self.identity_type,))
		self.db.connection.commit()

	def set_api_keys(self,reset=False):
		'''
		Setting github requesters
		api keys file syntax, per line: API#notes
		'''
		if self.data_folder is None:
			self.data_folder = self.db.data_folder

		api_keys_file = os.path.join(self.data_folder,self.api_keys_file)
		if os.path.exists(api_keys_file):
			with open(api_keys_file,'r') as f:
				self.api_keys += [l.split('#')[0] for l in f.read().split('\n')]
		elif os.path.exists(self.api_keys_file):
			with open(self.api_keys_file,'r') as f:
				self.api_keys += [l.split('#')[0] for l in f.read().split('\n')]
		elif os.path.exists(os.path.join(os.environ['HOME'],'.repo_tools',self.api_keys_file)):
			with open(os.path.join(os.environ['HOME'],'.repo_tools',self.api_keys_file),'r') as f:
				self.api_keys += [l.split('#')[0] for l in f.read().split('\n')]
		# if os.path.exists(os.path.join(os.environ['HOME'],'.repo_tools','github_api_keys.txt')):
		# 	with open(os.path.join(os.environ['HOME'],'.repo_tools','github_api_keys.txt'),'r') as f:
		# 		self.api_keys += [l.split('#')[0] for l in f.read().split('\n')]
		try:
			self.api_keys.append(os.environ[self.env_apikey])
		except KeyError:
			pass
		try:
			self.api_keys.append(os.environ['REPOTOOLS_'+self.env_apikey])
		except KeyError:
			pass
		self.api_keys = [ak for ak in self.api_keys if ak!=''] # removing empty api keys, can come from e.g. trailing newlines at end of parsed file
		api_keys = []
		for ak in self.api_keys:
			if ak not in api_keys:
				api_keys.append(ak)
		self.api_keys = api_keys
		if not len(self.api_keys):
			self.logger.info('No API keys found for {}, using default anonymous (but limited and only REST, no GQL) authentication.\nLooking for api_keys_file in db.data_folder, then in current folder, then in ~/.repo_tools/.\nAdding content of environment variables {} and REPOTOOLS_{}.'.format(self.env_apikey.split('_')[0],self.env_apikey,self.env_apikey))


	def set_requesters(self):
		'''
		Setting github requesters
		api keys file syntax, per line: API#notes
		'''

		if self.no_unauth:
			self.requesters = []
		else:
			self.requesters = [github.Github(per_page=self.per_page)]
		for ak in set(self.api_keys):
			g = github.Github(ak,per_page=self.per_page)
			try:
				g.get_rate_limit()
			except:
				self.logger.info('API key starting with "{}" and of length {} not valid'.format(ak[:5],len(ak)))
			else:
				self.requesters.append(g)

	def get_rate_limit(self,requester):
		ans = requester.get_rate_limit().core.remaining
		if not isinstance(ans,int):
			raise ValueError('Github answer to rate limit query:{}'.format(ans))
		else:
			return ans

	def get_reset_at(self,requester):
		return calendar.timegm(requester.get_rate_limit().core.reset.timetuple())

	def get_requester(self,random_pick=True,requesters=None):
		'''
		Going through requesters respecting threshold of minimum remaining api queries
		'''
		if requesters is None:
			requesters = self.requesters
		if not hasattr(self,'requesters'):
			self.set_requesters()
		if random_pick:
			while True:
				active_requesters = [rq for rq in requesters if self.get_rate_limit(rq) > self.querymin_threshold]
				if len(active_requesters):
					yield random.choice(active_requesters)
				elif self.fail_on_wait:
					raise IOError('All {} API keys are below the min remaining query threshold'.format(len(requesters)))
				else:
					earliest_reset = min([self.get_reset_at(rq) for rq in requesters])
					time_to_reset =  earliest_reset - calendar.timegm(time.gmtime())
					while time_to_reset <= 0:
						time_to_reset += 3600 # hack to consider shifted reset_at time between REST and GQL APIs
					self.logger.info('Waiting for reset of at least one github requester, sleeping {} seconds'.format(time_to_reset+1))
					time.sleep(time_to_reset+1)
		else:
			while True:
				for i,rq in enumerate(requesters):
					self.logger.debug('Using github requester {}, {} queries remaining'.format(i,self.get_rate_limit(rq)))
					# time.sleep(0.5)
					while self.get_rate_limit(rq) > self.querymin_threshold:
						yield rq
				if any(((self.get_rate_limit(rq) > self.querymin_threshold) for rq in requesters)):
					continue
				elif self.fail_on_wait:
					raise IOError('All {} API keys are below the min remaining query threshold'.format(len(requesters)))
				else:
					earliest_reset = min([self.get_reset_at(rq) for rq in requesters])
					time_to_reset =  earliest_reset - calendar.timegm(time.gmtime())
					self.logger.info('Waiting for reset of at least one github requester, sleeping {} seconds'.format(time_to_reset+1))
					time.sleep(time_to_reset+1)


class StarsFiller(GithubFiller):
	"""
	Fills in star information
	"""
	def __init__(self,repo_list=None,**kwargs):
		self.repo_list = repo_list
		GithubFiller.__init__(self,**kwargs)


	def apply(self):
		self.fill_stars(force=self.force,retry=self.retry,repo_list=self.repo_list,workers=self.workers,incremental_update=self.incremental_update)
		self.db.connection.commit()
		self.db.batch_merge_repos()

	def fill_stars(self,force=False,retry=False,repo_list=None,workers=1,in_thread=False,incremental_update=True,repo_nb=None,total_repos=None):
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
			self.db.connection.commit()

		if self.start_offset is not None:
			repo_list = [r for r in repo_list if r[1]>=self.start_offset]

		if total_repos is None:
			total_repos = len(repo_list)
		if repo_nb is None:
			repo_nb = 0

		if workers == 1:
			source,owner,repo_name = None,None,None # init values for the exception
			try:
				if in_thread:
					db = self.db.copy()
				else:
					db = self.db
				requester_gen = self.get_requester()
				new_repo = True
				while len(repo_list):
					current_repo = repo_list[0]
					# owner,repo_name = current_repo.split('/')
					# source = 'GitHub'
					# repo_id = db.get_repo_id(owner=owner,source=source,name=repo_name)
					source,owner,repo_name,repo_id = current_repo[:4]
					if new_repo:
						if incremental_update:
							nb_stars = db.count_stars(repo_id=repo_id)
							db.connection.commit()
						else:
							nb_stars = 0
						new_repo = False
						self.logger.info('Filling stars for repo {}/{} ({}/{})'.format(owner,repo_name,repo_nb,total_repos))
					requester = next(requester_gen)
					try:
						repo_apiobj = requester.get_repo('{}/{}'.format(owner,repo_name))
					except UnknownObjectException:
						self.logger.info('No such repository: {}/{} ({}/{})'.format(owner,repo_name,repo_nb,total_repos))
						db.insert_update(repo_id=repo_id,table='stars',success=False)
						repo_list.pop(0)
						new_repo = True
						repo_nb += 1
					else:
						checked_repo_owner,checked_repo_name = repo_apiobj.full_name.split('/')
						to_be_merged = False
						if (checked_repo_owner,checked_repo_name) != (owner,repo_name):
							to_be_merged = True
							self.db.plan_repo_merge(
								new_id=None,
								new_source=None,
								new_owner=checked_repo_owner,
								new_name=checked_repo_name,
								obsolete_id=repo_id,
								obsolete_source=source,
								obsolete_owner=owner,
								obsolete_name=repo_name,
								merging_reason_source='Repo redirect detected on github REST API when processing stars'
								)
							# db.merge_repos(obsolete_owner=owner,obsolete_name=repo_name,new_owner=checked_repo_owner,new_name=checked_repo_name,obsolete_source=source)
							# repo_id = db.get_repo_id(owner=checked_repo_owner,name=checked_repo_name,source='GitHub')
							# repo_name = checked_repo_name
							# owner = checked_repo_owner
						if repo_apiobj.watchers_count == 0:
							if to_be_merged:
								self.logger.info('No stars for repo {}/{} ({}/{})'.format(checked_repo_owner,checked_repo_name,owner,repo_name))
							else:
								self.logger.info('No stars for repo {}/{} ({}/{})'.format(owner,repo_name,repo_nb,total_repos))
							db.insert_update(repo_id=repo_id,table='stars',success=True)
							db.connection.commit()
							repo_list.pop(0)
							new_repo = True
							repo_nb += 1
							break

						while self.get_rate_limit(requester) > self.querymin_threshold:
							# nb_stars = db.count_stars(source=source,repo=repo_name,owner=owner)
							# nb_stars = db.count_stars(repo_id=repo_id)
							db.connection.commit()
							# sg_list = list(repo_apiobj.get_stargazers_with_dates()[nb_stars:nb_stars+per_page])
							sg_list = list(repo_apiobj.get_stargazers_with_dates().get_page(int(nb_stars/self.per_page)))

							if nb_stars < self.per_page*(int(nb_stars/self.per_page))+len(sg_list):
								# if in_thread:
								#if db.db_type == 'sqlite' and in_thread:
									#time.sleep(1+random.random()) # to avoid database locked issues, and smooth a bit concurrency
								# db.insert_stars(stars_list=[{'repo_id':repo_id,'source':source,'repo':repo_name,'owner':owner,'starred_at':sg.starred_at,'login':sg.user.login} for sg in sg_list],commit=False)
								self.insert_stars(db=db,stars_list=[{'repo_id':repo_id,'source':source,'repo':repo_name,'owner':owner,'starred_at':sg.starred_at,'login':sg.user.login} for sg in sg_list],commit=False)
								nb_stars += len(sg_list)
								db.connection.commit()
							else:

								if to_be_merged:
									self.logger.info('Filled stars for repo {}/{} ({}/{}): {}'.format(checked_repo_owner,checked_repo_name,owner,repo_name,nb_stars))
								else:
									self.logger.info('Filled stars for repo {}/{}: {} ({}/{})'.format(owner,repo_name,nb_stars,repo_nb,total_repos))
								db.insert_update(repo_id=repo_id,table='stars',success=True)
								db.connection.commit()
								repo_list.pop(0)
								new_repo = True
								repo_nb += 1
								break
			except Exception as e:
				if in_thread:
					self.logger.error('Exception in stars {}/{}/{}: \n {}: {}'.format(source,owner,repo_name,e.__class__.__name__,e))
				db.log_error('Exception in stars {}/{}/{}: \n {}: {}'.format(source,owner,repo_name,e.__class__.__name__,e))
				raise Exception('Exception in stars {}/{}/{}'.format(source,owner,repo_name)) from e
			finally:
				if in_thread and 'db' in locals():
					db.cursor.close()
					db.connection.close()

		else:
			with ThreadPoolExecutor(max_workers=workers) as executor:
				futures = []
				for i,repo in enumerate(repo_list):
					futures.append(executor.submit(self.fill_stars,repo_list=[repo],workers=1,in_thread=True,incremental_update=incremental_update,repo_nb=i+1,total_repos=total_repos))
				# for future in concurrent.futures.as_completed(futures):
				# 	pass
				for future in futures:
					future.result()


	def insert_stars(self,stars_list,commit=True,db=None):
		'''
		Inserts starring events.
		commit defines the behavior at the end, commit of the transaction or not. Committing externally allows to do it only when all stars for a repo have been added
		'''
		if db is None:
			db = self.db
		if db.db_type == 'postgres':
			extras.execute_batch(db.cursor,'''
				INSERT INTO stars(starred_at,login,repo_id,identity_type_id,identity_id)
				VALUES(%s,
						%s,
						%s,
						(SELECT id FROM identity_types WHERE name='github_login'),
						(SELECT id FROM identities WHERE identity=%s AND identity_type_id=(SELECT id FROM identity_types WHERE name='github_login'))
					)
				ON CONFLICT DO NOTHING
				;''',((s['starred_at'],s['login'],s['repo_id'],s['login']) for s in stars_list))
		else:
			db.cursor.executemany('''
					INSERT OR IGNORE INTO stars(starred_at,login,repo_id,identity_type_id,identity_id)
					VALUES(?,
							?,
							?,
							(SELECT id FROM identity_types WHERE name='github_login'),
							(SELECT id FROM identities WHERE identity=? AND identity_type_id=(SELECT id FROM identity_types WHERE name='github_login'))
						);''',((s['starred_at'],s['login'],s['repo_id'],s['login']) for s in stars_list))

		if commit:
			db.connection.commit()


class GHLoginsFiller(GithubFiller):
	"""
	Fills in github login information
	"""
	def __init__(self,info_list=None,**kwargs):
		self.info_list = info_list
		GithubFiller.__init__(self,**kwargs)



	def apply(self):
		self.fill_gh_logins(info_list=self.info_list,workers=self.workers)
		self.db.connection.commit()

	def prepare(self):
		'''
		ASSUMPTION: Only one github login can be associated to an email address.
		'''
		GithubFiller.prepare(self)
		if self.info_list is None:

			if self.force:
				if self.db.db_type == 'postgres':
					# self.db.cursor.execute('''
					# 	SELECT i.id,c.repo_id,r.owner,r.name,c.sha
					# 	FROM identities i
					# 	INNER JOIN identity_types it
					# 	ON i.identity_type_id = it.id AND it.name!='github_login'
					# 	JOIN LATERAL (SELECT cc.sha,cc.repo_id FROM commits cc
					# 		WHERE cc.author_id=i.id ORDER BY cc.created_at DESC LIMIT 1) AS c
					# 	ON (SELECT i2.id FROM identities i2
					# 		INNER JOIN identity_types it2
					# 		ON i2.user_id=i.user_id AND i2.identity_type_id=it2.id AND it2.name='github_login'
					# 		LIMIT 1) IS NULL
					# 	INNER JOIN repositories r
					# 	ON r.id=c.repo_id
					# 	;''')
					self.db.cursor.execute('''
						SELECT i.id,c.repo_id,r.owner,r.name,c.sha
						FROM (	(SELECT i1.id FROM identities i1)
									EXCEPT
								(SELECT i2.id FROM identities i2
									INNER JOIN identities i3
									ON i3.user_id = i2.user_id
									INNER JOIN identity_types it2
									ON it2.id=i3.identity_type_id AND it2.name='github_login')
								) AS i
					 	JOIN LATERAL (SELECT cc.sha,cc.repo_id FROM commits cc
					 		WHERE cc.author_id=i.id ORDER BY cc.created_at DESC LIMIT 1) AS c
					 	ON true
					 	INNER JOIN repositories r
					 	ON c.repo_id=r.id
						ORDER BY i.id
						;''')
				else:
					# self.db.cursor.execute('''
					# 	SELECT i.id,c.repo_id,r.owner,r.name,c.sha
					# 	FROM identities i
					# 	INNER JOIN identity_types it
					# 	ON i.identity_type_id = it.id AND it.name!='github_login'
					# 	JOIN commits c
					# 		ON (SELECT i2.id FROM identities i2
					# 			INNER JOIN identity_types it2
					# 			ON i2.user_id=i.user_id AND i2.identity_type_id=it2.id AND it2.name='github_login'
					# 			LIMIT 1) IS NULL AND
					# 		c.id IN (SELECT cc.id FROM commits cc
					# 			WHERE cc.author_id=i.id ORDER BY cc.created_at DESC LIMIT 1)
					# 	INNER JOIN repositories r
					# 	ON r.id=c.repo_id
						# ;''')
					self.db.cursor.execute('''
						SELECT i.id,c.repo_id,r.owner,r.name,c.sha
						FROM (	SELECT i1.id FROM identities i1
									EXCEPT
								SELECT i2.id FROM identities i2
									INNER JOIN identities i3
									ON i3.user_id = i2.user_id
									INNER JOIN identity_types it2
									ON it2.id=i3.identity_type_id AND it2.name='github_login'
								) AS i
					 	JOIN commits c
					 	ON c.id IN (SELECT cc.id FROM commits cc
					 		WHERE cc.author_id=i.id ORDER BY cc.created_at DESC LIMIT 1)
					 	INNER JOIN repositories r
					 	ON c.repo_id=r.id
						ORDER BY i.id
						;''')


			else:
				if self.db.db_type == 'postgres':
					self.db.cursor.execute('''
						SELECT i.id,c.repo_id,r.owner,r.name,c.sha
						FROM (
							SELECT ii.id FROM
						 		(SELECT iii.id FROM identities iii
								WHERE (SELECT iiii.id FROM identities iiii
									INNER JOIN identity_types iiiit
									ON iiii.user_id=iii.user_id AND iiiit.id=iiii.identity_type_id AND iiiit.name='github_login') IS NULL) AS ii
								LEFT JOIN table_updates tu
								ON tu.identity_id=ii.id AND tu.table_name='login'
								GROUP BY ii.id,tu.identity_id
								HAVING tu.identity_id IS NULL
							) AS i
						JOIN LATERAL (SELECT cc.sha,cc.repo_id FROM commits cc
							WHERE cc.author_id=i.id ORDER BY cc.created_at DESC LIMIT 1) AS c
						ON true
						INNER JOIN repositories r
						ON r.id=c.repo_id
						ORDER BY i.id
						;''')
				else:
					self.db.cursor.execute('''
						SELECT i.id,c.repo_id,r.owner,r.name,c.sha
						FROM (
							SELECT ii.id FROM
						 		(SELECT iii.id FROM identities iii
								WHERE (SELECT iiii.id FROM identities iiii
									INNER JOIN identity_types iiiit
									ON iiii.user_id=iii.user_id AND iiiit.id=iiii.identity_type_id AND iiiit.name='github_login') IS NULL) AS ii
								LEFT JOIN table_updates tu
								ON tu.identity_id=ii.id AND tu.table_name='login'
								GROUP BY ii.id,tu.identity_id
								HAVING tu.identity_id IS NULL
							) AS i
						JOIN commits c
							ON
							c.id IN (SELECT cc.id FROM commits cc
								WHERE cc.author_id=i.id ORDER BY cc.created_at DESC LIMIT 1)
						INNER JOIN repositories r
						ON r.id=c.repo_id
						ORDER BY i.id
						;''')

			self.info_list = list(self.db.cursor.fetchall())
			self.db.connection.commit()


	def fill_gh_logins(self,info_list=None,workers=1,in_thread=False,user_nb=None,total_users=None):
		'''
		Associating emails to github logins using GitHub API
		force: retry emails that were previously not retrievable
		Otherwise trying all emails which have no login yet and never failed before
		'''

		if info_list is None:
			info_list = self.info_list


		if self.start_offset is not None:
			info_list = [r for r in info_list if r[0]>=self.start_offset]

		if total_users is None:
			total_users = len(info_list)
		if user_nb is None:
			user_nb = 1

		if workers == 1:
			identity_id = None # init values for the exception
			try:
				if in_thread:
					db = self.db.copy()
				else:
					db = self.db
				requester_gen = self.get_requester()
				for infos in info_list:
					identity_id,repo_id,repo_owner,repo_name,commit_sha = infos
					self.logger.info('Filling gh login for user id {} ({}/{})'.format(identity_id,user_nb,total_users))
					requester = next(requester_gen)
					try:
						repo_apiobj = requester.get_repo('{}/{}'.format(repo_owner,repo_name))
						try:
							commit_apiobj = repo_apiobj.get_commit(commit_sha)
						except github.GithubException:
							self.logger.info('No such commit: {}/{}/{}'.format(repo_owner,repo_name,commit_sha))
							commit_apiobj = None
					except UnknownObjectException:
						self.logger.info('No such repository: {}/{}'.format(repo_owner,repo_name))
						# db.insert_update(identity_id=identity_id,table='stars',success=False)
					else:
						if commit_apiobj is None:
							pass
						else:
							if commit_apiobj.author is None:
								login = None
								self.logger.info('No login available for user id {} ({}/{})'.format(identity_id,user_nb,total_users))
							else:
								try:
									login = commit_apiobj.author.login
									self.logger.info('Found login {} for user id {} ({}/{})'.format(login,identity_id,user_nb,total_users))
								except (UnknownObjectException,IncompletableObject):
									self.logger.info('No login available for user id {} ({}/{}), uncompletable object error'.format(identity_id,user_nb,total_users))
									login = None
							self.set_gh_login(db=db,identity_id=identity_id,login=login,reason='Email/login match through github API for commit {}'.format(commit_sha))
					user_nb += 1

			except Exception as e:
				if in_thread:
					self.logger.error('Exception in getting login {}: \n {}: {}'.format(identity_id,e.__class__.__name__,e))
				db.log_error('Exception in getting login {}: \n {}: {}'.format(identity_id,e.__class__.__name__,e))
				raise Exception('Exception in getting login {}'.format(identity_id)) from e
			finally:
				if in_thread and 'db' in locals():
					db.cursor.close()
					db.connection.close()
		else:
			with ThreadPoolExecutor(max_workers=workers) as executor:
				futures = []
				for i,infos in enumerate(info_list):
					futures.append(executor.submit(self.fill_gh_logins,info_list=[infos],workers=1,in_thread=True,user_nb=i+1,total_users=total_users))
				# for future in concurrent.futures.as_completed(futures):
				# 	pass
				for future in futures:
					future.result()

	def set_gh_login(self,identity_id,login,autocommit=True,db=None,reason=None):
		'''
		Sets a login for a given user (id refers to a unique email, which can refer to several logins)
		'''
		if db is None:
			db = self.db
		if db.db_type == 'postgres':
			if login is not None:
				db.cursor.execute(''' INSERT INTO users(creation_identity_type_id,creation_identity) VALUES(
											(SELECT id FROM identity_types WHERE name='github_login'),
											%s
											) ON CONFLICT DO NOTHING;''',(login,))

				db.cursor.execute(''' INSERT INTO identities(identity_type_id,user_id,identity)
												VALUES((SELECT id FROM identity_types WHERE name='github_login'),
														(SELECT id FROM users
														WHERE creation_identity_type_id=(SELECT id FROM identity_types WHERE name='github_login')
															AND creation_identity=%s),
														%s)
												ON CONFLICT DO NOTHING;''',(login,login,))

				db.cursor.execute('''SELECT id FROM identities
											WHERE identity_type_id=(SELECT id FROM identity_types WHERE name='github_login')
											AND identity=%s;''',(login,))
				identity2 = db.cursor.fetchone()[0]
				db.merge_identities(identity1=identity2,identity2=identity_id,autocommit=False,reason=reason)
			db.cursor.execute('''INSERT INTO table_updates(identity_id,table_name,success) VALUES(%s,'login',%s);''',(identity_id,(login is not None)))
		else:
			if login is not None:


				db.cursor.execute(''' INSERT OR IGNORE INTO users(creation_identity_type_id,creation_identity) VALUES(
											(SELECT id FROM identity_types WHERE name='github_login'),
											?
											);''',(login,))

				db.cursor.execute(''' INSERT OR IGNORE INTO identities(identity_type_id,user_id,identity)
												VALUES((SELECT id FROM identity_types WHERE name='github_login'),
														(SELECT id FROM users
														WHERE creation_identity_type_id=(SELECT id FROM identity_types WHERE name='github_login')
															AND creation_identity=?),
														?);''',(login,login,))

				db.cursor.execute('''SELECT id FROM identities
											WHERE identity_type_id=(SELECT id FROM identity_types WHERE name='github_login')
											AND identity=?;''',(login,))
				identity2 = db.cursor.fetchone()[0]


				db.merge_identities(identity1=identity2,identity2=identity_id,autocommit=False,reason=reason)

			db.cursor.execute('''INSERT INTO table_updates(identity_id,table_name,success) VALUES(?,'login',?);''',(identity_id,(login is not None)))
		if autocommit:
			db.connection.commit()


class ForksFiller(GithubFiller):
	"""
	Fills in forks info for github repositories
	"""
	def __init__(self,repo_list=None,**kwargs):
		self.repo_list = repo_list
		GithubFiller.__init__(self,**kwargs)



	def apply(self):
		self.fill_forks(repo_list=self.repo_list,force=self.force,retry=self.retry,workers=self.workers,incremental_update=self.incremental_update)
		self.fill_fork_ranks()
		self.db.connection.commit()
		self.db.batch_merge_repos()



	def fill_forks(self,repo_list=None,force=False,workers=1,in_thread=False,retry=False,incremental_update=True,repo_nb=None,total_repos=None):
		'''
		Retrieving fork information from github.
		force: retry repos that were previously not retrievable
		Otherwise trying all emails which have no login yet and never failed before
		'''


		if repo_list is None:
			#build repo list
			repo_list = []
			for r in self.db.get_repo_list(option='forkinfo'):
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

			self.db.connection.commit() # avoids a lock 'idle in transaction' for the previous select query


		if self.start_offset is not None:
			repo_list = [r for r in repo_list if r[1]>=self.start_offset]

		if total_repos is None:
			total_repos = len(repo_list)
		if repo_nb is None:
			repo_nb = 0

		if workers == 1:
			source,owner,repo_name = None,None,None # init values for the exception
			try:
				if in_thread:
					db = self.db.copy()
				else:
					db = self.db

				requester_gen = self.get_requester()
				new_repo = True
				while len(repo_list):
					current_repo = repo_list[0]
					# owner,repo_name = current_repo.split('/')
					# source = 'GitHub'
					# repo_id = db.get_repo_id(owner=owner,source=source,name=repo_name)
					source,owner,repo_name,repo_id = current_repo[:4]
					if new_repo:
						if incremental_update:
							nb_forks = db.count_forks(repo_id=repo_id)
							db.connection.commit()
						else:
							nb_forks = 0
						new_repo = False
						self.logger.info('Filling forks for repo {}/{} ({}/{})'.format(owner,repo_name,repo_nb,total_repos))
					requester = next(requester_gen)
					try:
						repo_apiobj = requester.get_repo('{}/{}'.format(owner,repo_name))
					except UnknownObjectException:
						self.logger.info('No such repository: {}/{} ({}/{})'.format(owner,repo_name,repo_nb,total_repos))
						db.insert_update(repo_id=repo_id,table='forks',success=False)
						repo_list.pop(0)
						new_repo = True
						repo_nb += 1
					else:
						checked_repo_owner,checked_repo_name = repo_apiobj.full_name.split('/')
						to_be_merged = False
						if (checked_repo_owner,checked_repo_name) != (owner,repo_name):
							to_be_merged = True
							db.plan_repo_merge(
								new_id=None,
								new_source=None,
								new_owner=checked_repo_owner,
								new_name=checked_repo_name,
								obsolete_id=repo_id,
								obsolete_source=source,
								obsolete_owner=owner,
								obsolete_name=repo_name,
								merging_reason_source='Repo redirect detected on github REST API when processing forks'
								)
							# db.merge_repos(obsolete_owner=owner,obsolete_name=repo_name,new_owner=checked_repo_owner,new_name=checked_repo_name,obsolete_source=source)
							# repo_id = db.get_repo_id(owner=checked_repo_owner,name=checked_repo_name,source='GitHub')
							# repo_name = checked_repo_name
							# owner = checked_repo_ownerif repo_apiobj.watchers_count == 0:

						if repo_apiobj.forks_count == 0:
							if to_be_merged:
								self.logger.info('No forks for repo {}/{} ({}/{})'.format(checked_repo_owner,checked_repo_name,owner,repo_name))
							else:
								self.logger.info('No forks for repo {}/{} ({}/{})'.format(owner,repo_name,repo_nb,total_repos))
							db.insert_update(repo_id=repo_id,table='forks',success=True)
							db.connection.commit()
							repo_list.pop(0)
							new_repo = True
							repo_nb += 1
							break

						while self.get_rate_limit(requester) > self.querymin_threshold:
							# nb_forks = db.count_forks(source=source,repo=repo_name,owner=owner)

							# sg_list = list(repo_apiobj.get_stargazers_with_dates()[nb_stars:nb_stars+per_page])
							sg_list = list(repo_apiobj.get_forks().get_page(int(nb_forks/self.per_page)))
							forks_list=[{'repo_id':repo_id,'source':source,'repo':repo_name,'owner':owner,'repo_fullname':sg.full_name,'created_at':sg.created_at} for sg in sg_list]

							if nb_forks < self.per_page*(int(nb_forks/self.per_page))+len(sg_list):
								# if in_thread:
								#if db.db_type == 'sqlite' and in_thread:
								#	time.sleep(1+random.random()) # to avoid database locked issues, and smooth a bit concurrency
								if self.db.db_type == 'postgres':
									extras.execute_batch(db.cursor,'''
										INSERT INTO forks(forking_repo_id,forked_repo_id,forking_repo_url,forked_at)
										VALUES((SELECT r.id FROM repositories r
													INNER JOIN sources s
													ON s.name=%s AND s.id=r.source AND CONCAT(r.owner,'/',r.name)=%s)
												,%s,%s,%s)
										ON CONFLICT DO NOTHING
										;''',((s['source'],s['repo_fullname'],s['repo_id'],'github.com/'+s['repo_fullname'],s['created_at']) for s in forks_list))
								else:
									db.cursor.executemany('''
										INSERT OR IGNORE INTO forks(forking_repo_id,forked_repo_id,forking_repo_url,forked_at)
										VALUES((SELECT r.id FROM repositories r
													INNER JOIN sources s
													ON s.name=? AND s.id=r.source AND r.owner || '/' || r.name=?)
												,?,?,?)
										;''',((s['source'],s['repo_fullname'],s['repo_id'],'github.com/'+s['repo_fullname'],s['created_at']) for s in forks_list))
								#db.insert_forks(,commit=False)

								# get returned rows
								# should be equal to len sg list
								nb_forks += len(sg_list)

								db.connection.commit()
							else:
								if to_be_merged:
									self.logger.info('Filled forks for repo {}/{} ({}/{}): {}'.format(checked_repo_owner,checked_repo_name,owner,repo_name,nb_forks))
								else:
									self.logger.info('Filled forks for repo {}/{}: {} ({}/{})'.format(owner,repo_name,nb_forks,repo_nb,total_repos))
								db.insert_update(repo_id=repo_id,table='forks',success=True)
								db.connection.commit()
								repo_list.pop(0)
								new_repo = True
								repo_nb += 1
								break
			except Exception as e:
				if in_thread:
					self.logger.error('Exception in forks {}/{}/{}: \n {}: {}'.format(source,owner,repo_name,e.__class__.__name__,e))
				db.log_error('Exception in forks {}/{}/{}: \n {}: {}'.format(source,owner,repo_name,e.__class__.__name__,e))
				raise Exception('Exception in forks {}/{}/{}'.format(source,owner,repo_name)) from e
			finally:
				if in_thread and 'db' in locals():
					db.cursor.close()
					db.connection.close()
		else:
			with ThreadPoolExecutor(max_workers=workers) as executor:
				futures = []
				for i,repo in enumerate(repo_list):
					futures.append(executor.submit(self.fill_forks,repo_list=[repo],workers=1,in_thread=True,incremental_update=incremental_update,repo_nb=i+1,total_repos=total_repos))
				# for future in concurrent.futures.as_completed(futures):
				# 	pass
				for future in futures:
					future.result()

	def fill_fork_ranks(self,step=1):
		self.logger.info('Filling fork ranks, step {}'.format(step))
		if self.db.db_type == 'postgres':
			self.db.cursor.execute('''
				INSERT INTO forks(forking_repo_id,forking_repo_url,forked_repo_id,forked_at,fork_rank)
					SELECT f1.forking_repo_id,f1.forking_repo_url,f2.forked_repo_id,f1.forked_at,f2.fork_rank+f1.fork_rank
						FROM forks f1
						INNER JOIN forks f2
						ON f1.forked_repo_id=f2.forking_repo_id
				ON CONFLICT DO NOTHING
				;
				''')
			rowcount = self.db.cursor.rowcount
		else:
			self.db.cursor.execute('''
				INSERT OR IGNORE INTO forks(forking_repo_id,forking_repo_url,forked_repo_id,forked_at,fork_rank)
					SELECT f1.forking_repo_id,f1.forking_repo_url,f2.forked_repo_id,f1.forked_at,f2.fork_rank+f1.fork_rank
						FROM forks f1
						INNER JOIN forks f2
						ON f1.forked_repo_id=f2.forking_repo_id

				;
				''')
			rowcount = self.db.cursor.rowcount
		if rowcount > 0:
			self.logger.info('Filled {} missing indirect fork relations'.format(rowcount))
			self.fill_fork_ranks(step=step+1)

		self.db.connection.commit()


class FollowersFiller(GithubFiller):
	"""
	Fills in follower information
	"""

	def __init__(self,login_list=None,**kwargs):
		self.login_list = login_list
		GithubFiller.__init__(self,**kwargs)


	def apply(self):
		self.fill_followers(retry=self.retry,login_list=self.login_list,workers=self.workers,incremental_update=self.incremental_update)
		self.db.connection.commit()


	# def fill_followers(self,login_list=None,workers=1,in_thread=False,time_delay=24*3600):
	# 	'''
	# 	Getting followers for github logins. Avoiding by default logins which already have a value from less than time_delay seconds ago.
	# 	'''

	# 	option = 'logins'

	# 	if login_list is None:
	# 		login_list = self.db.get_user_list(option=option,time_delay=time_delay)

	# 	if workers == 1:
	# 		requester_gen = self.get_requester()
	# 		if in_thread:
	# 			db = self.db.copy()
	# 		else:
	# 			db = self.db
	# 		for login in login_list:
	# 			self.logger.info('Filling followers for login {}'.format(login))
	# 			requester = next(requester_gen)
	# 			try:
	# 				user_apiobj = requester.get_user('{}'.format(login))
	# 			except github.GithubException:
	# 				self.logger.info('No such user: {}'.format(login))
	# 			else:
	# 				try:
	# 					followers = user_apiobj.followers
	# 					self.logger.info('Login {} has {} followers'.format(login,followers))
	# 				except github.GithubException:
	# 					self.logger.info('No followers info available for login {}, uncompletable object error'.format(login))
	# 					followers = None

	# 				db.fill_followers(followers_info_list=[(login,followers)])

	# 		if in_thread:
	# 			db.connection.close()
	# 			del db
	# 	else:
	# 		with ThreadPoolExecutor(max_workers=workers) as executor:
	# 			futures = []
	# 			for login in login_list:
	# 				futures.append(executor.submit(self.fill_followers,login_list=[login],workers=1,in_thread=True))
	# 			# for future in concurrent.futures.as_completed(futures):
	# 			# 	pass
	# 			for future in futures:
	# 				future.result()


	def prepare(self):
		GithubFiller.prepare(self)
		if self.login_list is None:
			#build login list
			if self.force:
				self.db.cursor.execute('''
					SELECT i.id,i.identity,i.identity_type_id FROM identities i
					INNER JOIN identity_types it
					ON it.id=i.identity_type_id AND it.name='github_login'
					ORDER BY i.identity;
					''')
			else:
				self.db.cursor.execute('''
					SELECT i.id,i.identity,i.identity_type_id FROM identities i
					INNER JOIN identity_types it
					ON it.id=i.identity_type_id AND it.name='github_login'
					EXCEPT
					SELECT i.id,i.identity,i.identity_type_id FROM identities i
					INNER JOIN identity_types it
					ON it.id=i.identity_type_id AND it.name='github_login'
					INNER JOIN table_updates tu
					ON tu.success AND tu.identity_id=i.id AND tu.table_name='followers'
					ORDER BY identity;
					''')
			self.login_list = list(self.db.cursor.fetchall())
		self.db.connection.commit()


	def fill_followers(self,retry=False,login_list=None,workers=1,in_thread=False,incremental_update=True,user_nb=None,total_users=None):
		'''
		Filling followers
		Checking if an entry exists in table_updates with login_id and table_name followers

		login syntax: (identity_id,identity(login),identity_type_id)
		'''

		if self.start_offset is not None:
			login_list = [r for r in login_list if r[1]>=self.start_offset]

		if total_users is None:
			total_users = len(login_list)
		if user_nb is None:
			user_nb = 1

		if workers == 1:
			login_id,login,identity_type_id = None,None,None # init values for the exception
			try:
				if in_thread:
					db = self.db.copy()
				else:
					db = self.db
				requester_gen = self.get_requester()
				new_login = True
				while len(login_list):
					login_id,login,identity_type_id = login_list[0]

					if new_login:
						if incremental_update:
							nb_followers = db.count_followers(login_id=login_id)
							db.connection.commit()
						else:
							nb_followers = 0
						new_login = False
						self.logger.info('Filling followers for login {} ({}/{})'.format(login,user_nb,total_users))
					requester = next(requester_gen)
					try:
						login_apiobj = requester.get_user('{}'.format(login))
					except UnknownObjectException:
						self.logger.info('No such login: {} ({}/{})'.format(login,user_nb,total_users))
						db.insert_update(identity_id=login_id,table='followers',success=False)
						login_list.pop(0)
						new_login = True
						user_nb += 1
					else:

						if login_apiobj.followers == 0:
							self.logger.info('No followers for login: {} ({}/{})'.format(login,user_nb,total_users))
							db.insert_update(identity_id=login_id,table='followers',success=True)
							db.connection.commit()
							login_list.pop(0)
							new_login = True
							user_nb += 1
							break

						while self.get_rate_limit(requester) > self.querymin_threshold:
							# nb_followers = db.count_followers(login_id=login_id)
							db.connection.commit()

							sg_list = list(login_apiobj.get_followers().get_page(int(nb_followers/self.per_page)))

							if nb_followers < self.per_page*(int(nb_followers/self.per_page))+len(sg_list):
								# if db.db_type == 'sqlite' and in_thread:
								# 	time.sleep(1+random.random()) # to avoid database locked issues, and smooth a bit concurrency
								self.insert_followers(db=db,followers_list=[{'login_id':login_id,'identity_type_id':identity_type_id,'login':login,'follower_login':sg.login} for sg in sg_list],commit=True)
								nb_followers += len(sg_list)
							else:
								self.logger.info('Filled followers for login {} ({}/{}): {}'.format(login,user_nb,total_users,nb_followers))
								db.insert_update(identity_id=login_id,table='followers',success=True)
								db.connection.commit()
								login_list.pop(0)
								new_login = True
								user_nb += 1
								break
			except Exception as e:
				if in_thread:
					self.logger.error('Exception in followers {}: \n {}: {}'.format(login,e.__class__.__name__,e))
				db.log_error('Exception in followers {}: \n {}: {}'.format(login,e.__class__.__name__,e))
				raise Exception('Exception in followers {}'.format(login)) from e
			finally:
				if in_thread and 'db' in locals():
					db.cursor.close()
					db.connection.close()

		else:
			with ThreadPoolExecutor(max_workers=workers) as executor:
				futures = []
				for i,login in enumerate(login_list):
					futures.append(executor.submit(self.fill_followers,login_list=[login],workers=1,incremental_update=incremental_update,in_thread=True,user_nb=i+1,total_users=total_users))
				# for future in concurrent.futures.as_completed(futures):
				# 	pass
				for future in futures:
					future.result()


	def insert_followers(self,followers_list,commit=True,db=None):
		'''
		Inserts followers. Syntax [{'identity_type_id':<>,'follower_id':<>,'follower_login':<>,'login_id':<>,'login':<>}]
		commit defines the behavior at the end, commit of the transaction or not. Committing externally allows to do it only when all followers for a login have been added
		'''
		if db is None:
			db = self.db
		if db.db_type == 'postgres':
			extras.execute_batch(db.cursor,'''
				INSERT INTO followers(follower_identity_type_id,follower_login,follower_id,followee_id)
				VALUES(%s,
						%s,
						(SELECT id FROM identities WHERE identity=%s AND identity_type_id=%s),
						%s
					)
				ON CONFLICT DO NOTHING
				;''',((f['identity_type_id'],f['follower_login'],f['follower_login'],f['identity_type_id'],f['login_id'],) for f in followers_list))
		else:
			db.cursor.executemany('''
				INSERT OR IGNORE INTO followers(follower_identity_type_id,follower_login,follower_id,followee_id)
				VALUES(?,
						?,
						(SELECT id FROM identities WHERE identity=? AND identity_type_id=?),
						?
					)
				;''',((f['identity_type_id'],f['follower_login'],f['follower_login'],f['identity_type_id'],f['login_id'],) for f in followers_list))
		if commit:
			db.connection.commit()

