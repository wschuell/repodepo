import datetime
import os
import psycopg2
from psycopg2 import extras
import copy
import calendar
import time
import sqlite3
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import asyncio
import time
import random

from .. import fillers
from ..fillers import generic
from ..fillers import github_rest,github_gql

import gql
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.INFO)


class RequesterGitlab(github_gql.Requester):
	'''
	Class implementing the Request
	Caching rate limit information, updating at each query, or requerying after <refresh_time in sec> without update
	'''
	def __init__(self,url_root='gitlab.com',auth_header_prefix='Bearer ',**kwargs):
		url = 'https://{}/api/graphql'.format(url_root)
		github_gql.Requester.__init__(self,url=url,auth_header_prefix=auth_header_prefix,**kwargs)



	def get_rate_limit(self,refresh=False):
		# if refresh or self.refreshed_at is None or self.refreshed_at + datetime.timedelta(seconds=self.refresh_time)<= datetime.datetime.now():
		# 	self.query('''
		# 		query {
		# 			rateLimit {
		# 				cost
		# 				remaining
		# 				resetAt
		# 			}
		# 		}
		# 		''')
		self.remaining = 2000
		return self.remaining

	def query(self,gql_query,params=None,retries=None):
		if retries is None:
			retries = self.retries
		if params is not None:
			gql_query = gql_query.format(**params)
		# RL_query = '''
		# 		rateLimit {
		# 			cost
		# 			remaining
		# 			resetAt
		# 		}
		# '''
		# if 'rateLimit' not in gql_query:
		# 	splitted_string = gql_query.split('}')
		# 	gql_query = '}'.join(splitted_string[:-1])+RL_query+'}'+splitted_string[-1]

		try:
			retries_left = retries
			result_found = False
			while not result_found:
				try:
					result = self.client.execute(gql(gql_query))
					result_found = True
				except asyncio.TimeoutError as e:
					if retries_left>0:
						time.sleep(0.1*(retries-retries_left)*random.random())
						retries_left -= 1
					else:
						raise e.__class__('''TimeoutError happened more times than the set retries: {}. Rerun, maybe with higher value.
Original error message: {}'''.format(retries,e))
		except Exception as e:
			if hasattr(e,'data'):
				result = e.data
				if result is None:
					raise
				else:
					self.logger.info('Exception catched, {} :{}, result: {}'.format(e.__class__,e,result))
			else:
				raise
		self.remaining = 2000
		# self.remaining = result['rateLimit']['remaining']
		# self.reset_at = datetime.datetime.strptime(result['rateLimit']['resetAt'], '%Y-%m-%dT%H:%M:%SZ')
		# self.reset_at = time.mktime(self.reset_at.timetuple()) # converting to seconds to epoch; to have same format as REST API
		# self.refreshed_at = datetime.datetime.now()

		return result





class GitlabGQLFiller(github_gql.GHGQLFiller):
	"""
	class to be inherited from, contains credentials management
	"""

	def __init__(self,env_apikey='GITLAB_API_KEY',source_name='Gitlab',target_identity_type='gitlab_login',api_keys_file='gitlab_api_keys.txt',**kwargs):
		github_gql.GHGQLFiller.__init__(self,requester_class=RequesterGitlab,source_name=source_name,env_apikey=env_apikey,target_identity_type=target_identity_type,api_keys_file=api_keys_file,**kwargs)




class LoginsFiller(GitlabGQLFiller):
	'''
	Querying logins through the GraphQL API using commits
	'''
	def __init__(self,**kwargs):
		self.items_name = 'login'
		self.queried_obj = 'email'
		self.pageinfo_path = None
		GitlabGQLFiller.__init__(self,**kwargs)


	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return ''' query {{
						repository:project(fullPath:"{repo_owner}/{repo_name}" ) {{
							fullPath
    						repository{{
      							tree(ref:"{commit_sha}"){{
      								lastCommit{{
        								id
        								author{{
        									username
        									}}
    									}}
    								}}
    							}}
    						}}
    					}}
		'''
	def parse_query_result(self,query_result,identity_id,**kwargs):
		'''
		In subclasses this has to be implemented
		output: [{'commit_sha':sha,'login':lo,'created_at':cr_at,'repo_owner':rpo,'repo_name':rpn,'email_id':eid}]
		'''
		ans = dict()
		ans['email_id'] = identity_id
		if query_result['repository'] is None:
			ans['login'] = None
			ans['repo_owner'] = None
			ans['repo_name'] = None
			ans['created_at'] = None
			ans['commit_sha'] = None
		elif query_result['repository']['repository']['tree'] is None:
			ans['login'] = None
			ans['repo_owner'] = query_result['repository']['fullPath'].split('/')[1]
			ans['repo_name'] = query_result['repository']['fullPath'].split('/')[0]
			ans['created_at'] = None
			ans['commit_sha'] = None
		else:
			ans['repo_owner'] = query_result['repository']['fullPath'].split('/')[1]
			ans['repo_name'] = query_result['repository']['fullPath'].split('/')[0]
			if query_result['repository']['repository']['tree']['lastCommit']['author'] is None:
				ans['login'] = None
				ans['created_at'] = None
			elif query_result['repository']['repository']['tree']['lastCommit']['author']['username'] is None:
				ans['login'] = None
				ans['created_at'] = None
			else:
				ans['login'] = query_result['repository']['repository']['tree']['lastCommit']['author']['username']
				ans['created_at'] = None
			ans['commit_sha'] = query_result['repository']['repository']['tree']['lastCommit']['id']


		return [ans]


	def insert_items(self,items_list,commit=True,db=None):
		github_gql.LoginsGQLFiller.insert_items(self,items_list=items_list,commit=commit,db=db)

	def get_nb_items(self,query_result):
		return 1

	def set_element_list(self):
		github_gql.LoginsGQLFiller.set_element_list(self)


class RepoCreatedAtFiller(GitlabGQLFiller):
	'''
	Querying logins through the GraphQL API using commits
	'''
	def __init__(self,**kwargs):
		self.items_name = 'repo_createdat'
		self.queried_obj = 'repo'
		self.pageinfo_path = None
		GitlabGQLFiller.__init__(self,**kwargs)


	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return ''' query {{
						repository:project(fullPath:"{repo_owner}/{repo_name}" ) {{
							nameWithOwner:fullPath
    						createdAt
    						}}
    					}}
		'''

	def parse_query_result(self,query_result,repo_id,identity_id,repo_owner=None,repo_name=None,**kwargs):
		'''
		In subclasses this has to be implemented
		output: [ {'repo_id':r_id,'repo_owner':r_ow,'repo_name':r_na,'created_at':c_at} , ...]
		'''
		ans = []
		if repo_owner is None:
			repo_owner = query_result['repository']['nameWithOwner'].split('/')[0]
			repo_name = '/'.join(query_result['repository']['nameWithOwner'].split('/')[1:])
		d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name}
		try:
			d['created_at'] = query_result['repository']['createdAt']
		except (KeyError,TypeError) as err:
			self.logger.info('Error when parsing creation date for {}/{}: {}'.format(repo_owner,repo_name,err))
		else:
			ans.append(d)
		return ans

	def insert_items(self,items_list,commit=True,db=None):
		github_gql.RepoCreatedAtGQLFiller.insert_items(self,items_list=items_list,commit=commit,db=db)

	def get_nb_items(self,query_result):
		return 1

	def set_element_list(self):
		github_gql.RepoCreatedAtGQLFiller.set_element_list(self)



class StarsGQLFiller(GitlabGQLFiller):
	'''
	Querying stars through the GraphQL API
	'''
	def __init__(self,**kwargs):
		self.items_name = 'stars'
		self.queried_obj = 'repo'

		raise NotImplementedError #For the moment the Gitlab GraphQL API doesnt offer starrers of projects, only starred projects of users. REST API does though
		self.pageinfo_path = ['repository','stargazers','pageInfo']
		GitlabGQLFiller.__init__(self,**kwargs)

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		raise NotImplementedError
		return '''query {{
					repository(owner:"{repo_owner}", name:"{repo_name}") {{
						nameWithOwner
						stargazers (first:100, orderBy: {{ field: STARRED_AT, direction: ASC }} {after_end_cursor} ){{
						 totalCount
						 pageInfo {{
							endCursor
							hasNextPage
						 }}
						 edges {{
						 	starredAt
							node {{
								login
							}}
						 }}
						}}
					}}
				}}'''

	def parse_query_result(self,query_result,repo_id,identity_id,repo_owner=None,repo_name=None,**kwargs):
		'''
		In subclasses this has to be implemented
		output: [ {'repo_id':r_id,'repo_owner':r_ow,'repo_name':r_na,'starrer_login':s_lo,'starred_at':st_at} , ...]
		'''
		raise NotImplementedError
		ans = []
		if repo_owner is None:
			repo_owner,repo_name = query_result['repository']['nameWithOwner'].split('/')
		for e in query_result['repository']['stargazers']['edges']:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name}
			try:
				d['starred_at'] = e['starredAt']
				d['starrer_login'] = e['node']['login']
			except (KeyError,TypeError) as err:
				self.logger.info('Result triggering error: {} \nError when parsing stars for {}/{}: {}'.format(e,repo_owner,repo_name,err))
				continue
			else:
				ans.append(d)
		return ans


	def insert_items(self,items_list,commit=True,db=None):
		github_gql.StarsGQLFiller.insert_items(self,items_list=items_list,commit=commit,db=db)

	def set_element_list(self):
		github_gql.StarsGQLFiller.set_element_list(self)

	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['repository']['stargazers']['totalCount']


class RandomCommitLoginsFiller(LoginsFiller):

	def set_element_list(self,**kwargs):
		github_gql.RandomCommitLoginsGQLFiller.set_element_list(self,**kwargs)
