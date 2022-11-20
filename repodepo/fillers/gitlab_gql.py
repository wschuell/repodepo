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
from gql import Client
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



	def check_scope(self,scope):
		return True

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

	def format_query(self,gql_query,params):
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
		return gql_query

	def query(self,gql_query,params=None,retries=None,cp_params=True):
		if cp_params:
			params = copy.deepcopy(params)
		if retries is None:
			retries = self.retries
		# if params is not None:
		# 	gql_query = gql_query.format(**params)
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
					result = self.client.execute(gql.gql(self.format_query(gql_query=gql_query,params=params)))
					result_found = True
				except asyncio.TimeoutError as e:
					if retries_left>0:
						time.sleep(0.1*(retries-retries_left)*random.random())
						retries_left -= 1
						for k in ['page_size','max_page_size']:
							if params is not None and k in params.keys():
								params[k] = max(1,int(0.9*params[k]))
								self.logger.info(f'Setting {k} to {params[k]}')
						self.logger.info(f'Retries left: {retries_left}')
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

	scopes = tuple()

	def __init__(self,env_apikey='GITLAB_API_KEY',source_name='Gitlab',target_identity_type='gitlab_login',api_keys_file='gitlab_api_keys.txt',**kwargs):
		github_gql.GHGQLFiller.__init__(self,requester_class=RequesterGitlab,source_name=source_name,env_apikey=env_apikey,target_identity_type=target_identity_type,api_keys_file=api_keys_file,**kwargs)




class LoginsFiller(GitlabGQLFiller):
	'''
	Querying logins through the GraphQL API using commits
	'''
	def __init__(self,actor='author',**kwargs):
		self.actor = actor
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




class RandomCommitLoginsFiller(LoginsFiller):

	def set_element_list(self,**kwargs):
		github_gql.RandomCommitLoginsGQLFiller.set_element_list(self,**kwargs)


class CompleteIssuesGQLFiller(github_gql.CompleteIssuesGQLFiller):
	'''
	Querying issues through the GraphQL API
	Adding comments, labels and reactions (first 100 per issue), and first 40 reactions to comments.
	'''
	scopes = tuple()

	def __init__(self,**kwargs):
		new_kwargs = dict(requester_class=RequesterGitlab,env_apikey='GITLAB_API_KEY',source_name='Gitlab',target_identity_type='gitlab_login',api_keys_file='gitlab_api_keys.txt')
		new_kwargs.update(kwargs)
		github_gql.CompleteIssuesGQLFiller.__init__(self,**new_kwargs)
		#GitlabGQLFiller.__init__(self)

	def after_insert(self):
		if self.complete_info:
			# self.db.add_filler(IssueReactionsGQLFiller(**self.get_generic_kwargs()))
			self.db.add_filler(IssueLabelsGQLFiller(**self.get_generic_kwargs()))
			self.db.add_filler(IssueCommentsGQLFiller(**self.get_generic_kwargs()))
			# self.db.add_filler(IssueCommentReactionsGQLFiller(**self.get_generic_kwargs()))

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return ''' query {{
						repository:project(fullPath:"{repo_owner}/{repo_name}" ) {{
							nameWithOwner:fullPath
      							issues(first:{page_size} {after_end_cursor}) {{
    							totalCount:count
    							pageInfo {{
									endCursor
									hasNextPage
						 			}}
      							nodes {{
									title
									id
									iid
									relativePosition
									author {{ username }}
									description
									createdAt
									closedAt
									notes(first:{secondary_page_size}) {{
										# totalCount:count
										pageInfo {{
											endCursor
											hasNextPage
											}}
										nodes {{
											id
											createdAt
											author {{ username }}
											body
											
											}}
										}}

									labels(first:{secondary_page_size}) {{
										totalCount:count
										pageInfo {{
											endCursor
											hasNextPage
											}}
										nodes {{
											title
											description
											createdAt
											}}
										}}

									
      								}}
    						
    							}}
    						}}
    					}}
    				'''


	def parse_query_result(self,query_result,repo_id,identity_id,repo_owner=None,repo_name=None,**kwargs):
		'''
		In subclasses this has to be implemented
		output: [ {'repo_id':r_id,'repo_owner':r_ow,'repo_name':r_na,'issue_number':issue_na,'issue_title':title_na,'created_at':created_date,'closed_at':closed_date} , ...]
		'''
		ans = []
		if repo_owner is None:
			repo_owner = query_result['repository']['nameWithOwner'].split('/')[0]
			repo_name = '/'.join(query_result['repository']['nameWithOwner'].split('/')[1:])
		for e in query_result['repository']['issues']['nodes']:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'issue'}
			try:
				d['created_at'] = e['createdAt']
				d['closed_at'] = e['closedAt']
				# d['issue_number'] = e['relativePosition']
				d['issue_number'] = e['iid']
				d['issue_title'] = e['title']
				d['issue_gql_id'] = e['id']
				d['issue_text'] = e['description']

				# d['reactions_pageinfo'] = e['reactions']['pageInfo']
				d['labels_pageinfo'] = e['labels']['pageInfo']
				d['comments_pageinfo'] = e['notes']['pageInfo']

				try:
					d['author_login'] = e['author']['username']
				except (KeyError,TypeError) as err:
					d['author_login'] = None
			except (KeyError,TypeError) as err:
				self.logger.info('Result triggering error: {} \nError when parsing issues for {}/{}: {}'.format(e,repo_owner,repo_name,err))
				continue
			else:
				ans.append(d)

				if e['labels']['totalCount']>0:
					for ee in e['labels']['nodes']:
						r = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'issue_label'}
						r['issue_number'] = d['issue_number']
						r['issue_gql_id'] = d['issue_gql_id']
						try:
							# r['created_at'] = ee['createdAt']
							r['issue_label'] = ee['title']
							# try:
							# 	r['author_login'] = ee['user']['login']
							# except:
							# 	r['author_login'] = None
						except (KeyError,TypeError) as err:
							self.logger.info('Result triggering error: {} \nError when parsing issue_labels for {}/{}: {}'.format(e,repo_owner,repo_name,err))
							continue
						else:
							ans.append(r)

				if len(e['notes']['nodes'])>0:
					for ee in e['notes']['nodes']:
						r = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'issue_comment'}
						r['issue_number'] = d['issue_number']
						r['issue_gql_id'] = d['issue_gql_id']
						try:
							r['created_at'] = ee['createdAt']
							r['issue_comment_text'] = ee['body']
							r['issue_comment_id'] = int(ee['id'].split('/')[-1])
							r['issue_comment_gql_id'] = ee['id']
							# r['comment_reactions_pageinfo'] = ee['reactions']['pageInfo']
							try:
								r['author_login'] = ee['author']['username']
							except:
								r['author_login'] = None


						except (KeyError,TypeError) as err:
							self.logger.info('Result triggering error: {} \nError when parsing issue_comments for {}/{}: {}'.format(e,repo_owner,repo_name,err))
							continue
						else:
							ans.append(r)

		return ans


	def insert_reaction_updates(self,**kwargs):
		pass

	def insert_reactions(self,**kwargs):
		pass

	def insert_comment_reaction_updates(self,**kwargs):
		pass

	def insert_comment_reactions(self,**kwargs):
		pass


class IssueLabelsGQLFiller(github_gql.IssueLabelsGQLFiller):

	scopes = tuple()

	def __init__(self,**kwargs):
		new_kwargs = dict(requester_class=RequesterGitlab,env_apikey='GITLAB_API_KEY',source_name='Gitlab',target_identity_type='gitlab_login',api_keys_file='gitlab_api_keys.txt')
		new_kwargs.update(kwargs)
		github_gql.IssueLabelsGQLFiller.__init__(self,**new_kwargs)


	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
					repository:project(fullPath:"{repo_owner}/{repo_name}" ) {{
							nameWithOwner:fullPath }}
  					node:issue(id:"{issue_gql_id}") {{
  									title
									id
									iid
									relativePosition
									author {{ username }}
									description
									createdAt
									closedAt

									labels(first:{page_size} {after_end_cursor}) {{
										totalCount:count
										pageInfo {{
											endCursor
											hasNextPage
											}}
										nodes {{
											title
											description
											createdAt
											}}
										}}

									
  							}}
						}}'''


	def parse_query_result(self,query_result,repo_id,identity_id,repo_owner=None,repo_name=None,**kwargs):
		'''
		In subclasses this has to be implemented
		output: [ {'repo_id':r_id,'repo_owner':r_ow,'repo_name':r_na,'issue_number':issue_na,'issue_title':title_na,'created_at':created_date,'closed_at':closed_date} , ...]
		'''
		ans = []
		if repo_owner is None:
			repo_owner = query_result['repository']['nameWithOwner'].split('/')[0]
			repo_name = '/'.join(query_result['repository']['nameWithOwner'].split('/')[1:])
		for e in [query_result['node']]:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'issue'}
			try:
				d['issue_gql_id'] = e['id']
				d['issue_number'] = e['iid']
				#d['issue_number'] = e['relativePosition']
				d['issue_id'] = e['iid']
				
			except (KeyError,TypeError) as err:
				self.logger.info('Result triggering error: {} \nError when parsing issues for {}/{}: {}'.format(e,repo_owner,repo_name,err))
				continue
			else:
				ans.append(d)
				if e['labels']['totalCount']>0:
					for ee in e['labels']['nodes']:
						r = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'issue_label'}
						r['issue_number'] = d['issue_number']
						r['issue_gql_id'] = d['issue_gql_id']
						try:
							# r['created_at'] = ee['createdAt']
							r['issue_label'] = ee['title']
							# try:
							# 	r['author_login'] = ee['user']['login']
							# except:
							# 	r['author_login'] = None
						except (KeyError,TypeError) as err:
							self.logger.info('Result triggering error: {} \nError when parsing issue_labels for {}/{}: {}'.format(e,repo_owner,repo_name,err))
							continue
						else:
							ans.append(r)
		return ans


class IssueCommentsGQLFiller(github_gql.IssueCommentsGQLFiller):

	scopes = tuple()

	def __init__(self,**kwargs):
		new_kwargs = dict(requester_class=RequesterGitlab,env_apikey='GITLAB_API_KEY',source_name='Gitlab',target_identity_type='gitlab_login',api_keys_file='gitlab_api_keys.txt')
		new_kwargs.update(kwargs)
		github_gql.IssueCommentsGQLFiller.__init__(self,**new_kwargs)

		self.pageinfo_path = ['node','notes','pageInfo']


	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
					repository:project(fullPath:"{repo_owner}/{repo_name}" ) {{
							nameWithOwner:fullPath }}
  					node:issue(id:"{issue_gql_id}") {{
  									title
									id
									iid
									relativePosition
									author {{ username }}
									description
									createdAt
									closedAt

									notes(first:{page_size} {after_end_cursor}) {{
										# totalCount:count
										pageInfo {{
											endCursor
											hasNextPage
											}}
										nodes {{
											id
											createdAt
											author {{ username }}
											body
											
											}}
										}}

									
  							}}
						}}'''

	def parse_query_result(self,query_result,repo_id,identity_id,repo_owner=None,repo_name=None,**kwargs):
		'''
		In subclasses this has to be implemented
		output: [ {'repo_id':r_id,'repo_owner':r_ow,'repo_name':r_na,'issue_number':issue_na,'issue_title':title_na,'created_at':created_date,'closed_at':closed_date} , ...]
		'''
		ans = []
		if repo_owner is None:
			repo_owner = query_result['repository']['nameWithOwner'].split('/')[0]
			repo_name = '/'.join(query_result['repository']['nameWithOwner'].split('/')[1:])
		for e in [query_result['node']]:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'issue'}
			try:
				d['issue_gql_id'] = e['id']
				d['issue_number'] = e['iid']
				#d['issue_number'] = e['relativePosition']
				d['issue_id'] = e['iid']
				
			except (KeyError,TypeError) as err:
				self.logger.info('Result triggering error: {} \nError when parsing issues for {}/{}: {}'.format(e,repo_owner,repo_name,err))
				continue
			else:
				ans.append(d)
				if  len(e['notes']['nodes'])>0:
					for ee in e['notes']['nodes']:
						r = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'issue_comment'}
						r['issue_number'] = d['issue_number']
						r['issue_gql_id'] = d['issue_gql_id']
						try:
							r['created_at'] = ee['createdAt']
							r['issue_comment_text'] = ee['body']
							r['issue_comment_id'] = int(ee['id'].split('/')[-1])
							r['issue_comment_gql_id'] = ee['id']
							#r['comment_reactions_pageinfo'] = ee['reactions']['pageInfo']
							try:
								r['author_login'] = ee['author']['username']
							except:
								r['author_login'] = None


						except (KeyError,TypeError) as err:
							self.logger.info('Result triggering error: {} \nError when parsing issue_comments for {}/{}: {}'.format(e,repo_owner,repo_name,err))
							continue
						else:
							ans.append(r)

		return ans

	def insert_items(self,db=None,**kwargs):

		if db is None:
			db = self.db

		CompleteIssuesGQLFiller.insert_comments(self,db=db,**kwargs)
		#CompleteIssuesGQLFiller.insert_comment_reactions(self,db=db,**kwargs)
		#CompleteIssuesGQLFiller.insert_comment_reaction_updates(self,db=db,**kwargs)

		if len(kwargs['items_list']):
			db.cursor.execute('''INSERT INTO full_updates(update_type) SELECT 'issue_comments' ;''')


	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		# return query_result['node']['notes']['totalCount']
		return 'NA'



class CompletePullRequestsGQLFiller(github_gql.CompletePullRequestsGQLFiller):
	'''
	Querying PullRequests through the GraphQL API
	Adding comments, labels and reactions (first 100 per PullRequest), and first 40 reactions to comments.
	'''

	scopes = tuple()

	def __init__(self,**kwargs):
		new_kwargs = dict(requester_class=RequesterGitlab,env_apikey='GITLAB_API_KEY',source_name='Gitlab',target_identity_type='gitlab_login',api_keys_file='gitlab_api_keys.txt')
		new_kwargs.update(kwargs)
		github_gql.CompletePullRequestsGQLFiller.__init__(self,**new_kwargs)
		#GitlabGQLFiller.__init__(self)

	def after_insert(self):
		if self.complete_info:
			# self.db.add_filler(PullRequestReactionsGQLFiller(**self.get_generic_kwargs()))
			self.db.add_filler(PullRequestLabelsGQLFiller(**self.get_generic_kwargs()))
			self.db.add_filler(PullRequestCommentsGQLFiller(**self.get_generic_kwargs()))
			# self.db.add_filler(PullRequestCommentReactionsGQLFiller(**self.get_generic_kwargs()))

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return ''' query {{
						repository:project(fullPath:"{repo_owner}/{repo_name}" ) {{
							nameWithOwner:fullPath
      							pullRequests:mergeRequests(first:{page_size} {after_end_cursor}) {{
    							totalCount:count
    							pageInfo {{
									endCursor
									hasNextPage
						 			}}
      							nodes {{
									title
									id
									iid
									author {{ username }}
									description
									createdAt
									mergedAt
									updatedAt
									state
									mergeUser {{ username }}
									notes(first:{secondary_page_size}) {{
										# totalCount:count
										pageInfo {{
											endCursor
											hasNextPage
											}}
										nodes {{
											id
											createdAt
											author {{ username }}
											body
											
											}}
										}}

									labels(first:{secondary_page_size}) {{
										totalCount:count
										pageInfo {{
											endCursor
											hasNextPage
											}}
										nodes {{
											title
											description
											createdAt
											}}
										}}

									
      								}}
    						
    							}}
    						}}
    					}}
    				'''


	def parse_query_result(self,query_result,repo_id,identity_id,repo_owner=None,repo_name=None,**kwargs):
		'''
		In subclasses this has to be implemented
		output: [ {'repo_id':r_id,'repo_owner':r_ow,'repo_name':r_na,'PullRequest_number':PullRequest_na,'PullRequest_title':title_na,'created_at':created_date,'closed_at':closed_date} , ...]
		'''
		ans = []
		if repo_owner is None:
			repo_owner = query_result['repository']['nameWithOwner'].split('/')[0]
			repo_name = '/'.join(query_result['repository']['nameWithOwner'].split('/')[1:])
		for e in query_result['repository']['pullRequests']['nodes']:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'pullrequest'}
			try:
				d['created_at'] = e['createdAt']
				d['merged_at'] = e['mergedAt']
				if e['state'] == 'closed':
					d['closed_at'] = e['updatedAt']
				elif e['state'] == 'merged':
					d['closed_at'] = e['mergedAt']
				elif e['state'] == 'opened':
					d['closed_at'] = None
				else:
					raise ValueError(f'''Unknown state for gitlab mergeRequest: {e['state']} , should be "opened", "merged" or "closed"''')
				d['pullrequest_number'] = e['iid']
				d['pullrequest_title'] = e['title']
				d['pullrequest_gql_id'] = e['id']
				d['pullrequest_text'] = e['description']

				# d['reactions_pageinfo'] = e['reactions']['pageInfo']
				d['labels_pageinfo'] = e['labels']['pageInfo']
				d['comments_pageinfo'] = e['notes']['pageInfo']

				try:
					d['author_login'] = e['author']['username']
				except (KeyError,TypeError) as err:
					d['author_login'] = None
				try:
					d['merger_login'] = e['mergeUser']['username']
				except (KeyError,TypeError) as err:
					d['merger_login'] = None
			except (KeyError,TypeError) as err:
				self.logger.info('Result triggering error: {} \nError when parsing pullrequests for {}/{}: {}'.format(e,repo_owner,repo_name,err))
				continue
			else:
				ans.append(d)

				if e['labels']['totalCount']>0:
					for ee in e['labels']['nodes']:
						r = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'pullrequest_label'}
						r['pullrequest_number'] = d['pullrequest_number']
						r['pullrequest_gql_id'] = d['pullrequest_gql_id']
						try:
							# r['created_at'] = ee['createdAt']
							r['pullrequest_label'] = ee['title']
							# try:
							# 	r['author_login'] = ee['user']['login']
							# except:
							# 	r['author_login'] = None
						except (KeyError,TypeError) as err:
							self.logger.info('Result triggering error: {} \nError when parsing pullrequest_labels for {}/{}: {}'.format(e,repo_owner,repo_name,err))
							continue
						else:
							ans.append(r)

				if len(e['notes']['nodes'])>0:
					for ee in e['notes']['nodes']:
						r = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'pullrequest_comment'}
						r['pullrequest_number'] = d['pullrequest_number']
						r['pullrequest_gql_id'] = d['pullrequest_gql_id']
						try:
							r['created_at'] = ee['createdAt']
							r['pullrequest_comment_text'] = ee['body']
							r['pullrequest_comment_id'] = int(ee['id'].split('/')[-1])
							r['pullrequest_comment_gql_id'] = ee['id']
							# r['comment_reactions_pageinfo'] = ee['reactions']['pageInfo']
							try:
								r['author_login'] = ee['author']['username']
							except:
								r['author_login'] = None


						except (KeyError,TypeError) as err:
							self.logger.info('Result triggering error: {} \nError when parsing pullrequest_comments for {}/{}: {}'.format(e,repo_owner,repo_name,err))
							continue
						else:
							ans.append(r)

		return ans


	def insert_reaction_updates(self,**kwargs):
		pass

	def insert_reactions(self,**kwargs):
		pass

	def insert_comment_reaction_updates(self,**kwargs):
		pass

	def insert_comment_reactions(self,**kwargs):
		pass


class PullRequestLabelsGQLFiller(github_gql.PRLabelsGQLFiller):

	scopes = tuple()
	def __init__(self,**kwargs):
		new_kwargs = dict(requester_class=RequesterGitlab,env_apikey='GITLAB_API_KEY',source_name='Gitlab',target_identity_type='gitlab_login',api_keys_file='gitlab_api_keys.txt')
		new_kwargs.update(kwargs)
		github_gql.PRLabelsGQLFiller.__init__(self,**new_kwargs)


	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
					repository:project(fullPath:"{repo_owner}/{repo_name}" ) {{
							nameWithOwner:fullPath }}
  					node:mergeRequest(id:"{pullrequest_gql_id}") {{
  									title
									id
									iid
									author {{ username }}
									description
									createdAt
									mergedAt

									labels(first:{page_size} {after_end_cursor}) {{
										totalCount:count
										pageInfo {{
											endCursor
											hasNextPage
											}}
										nodes {{
											title
											description
											createdAt
											}}
										}}

									
  							}}
						}}'''


	def parse_query_result(self,query_result,repo_id,identity_id,repo_owner=None,repo_name=None,**kwargs):
		'''
		In subclasses this has to be implemented
		output: [ {'repo_id':r_id,'repo_owner':r_ow,'repo_name':r_na,'pullrequest_number':pullrequest_na,'pullrequest_title':title_na,'created_at':created_date,'closed_at':closed_date} , ...]
		'''
		ans = []
		if repo_owner is None:
			repo_owner = query_result['repository']['nameWithOwner'].split('/')[0]
			repo_name = '/'.join(query_result['repository']['nameWithOwner'].split('/')[1:])
		for e in [query_result['node']]:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'pullrequest'}
			try:
				d['pullrequest_gql_id'] = e['id']
				d['pullrequest_number'] = e['iid']
				d['pullrequest_id'] = e['iid']
				
			except (KeyError,TypeError) as err:
				self.logger.info('Result triggering error: {} \nError when parsing pullrequests for {}/{}: {}'.format(e,repo_owner,repo_name,err))
				continue
			else:
				ans.append(d)
				if e['labels']['totalCount']>0:
					for ee in e['labels']['nodes']:
						r = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'pullrequest_label'}
						r['pullrequest_number'] = d['pullrequest_number']
						r['pullrequest_gql_id'] = d['pullrequest_gql_id']
						try:
							# r['created_at'] = ee['createdAt']
							r['pullrequest_label'] = ee['title']
							# try:
							# 	r['author_login'] = ee['user']['login']
							# except:
							# 	r['author_login'] = None
						except (KeyError,TypeError) as err:
							self.logger.info('Result triggering error: {} \nError when parsing pullrequest_labels for {}/{}: {}'.format(e,repo_owner,repo_name,err))
							continue
						else:
							ans.append(r)
		return ans


class PullRequestCommentsGQLFiller(github_gql.PRCommentsGQLFiller):

	scopes = tuple()
	def __init__(self,**kwargs):
		new_kwargs = dict(requester_class=RequesterGitlab,env_apikey='GITLAB_API_KEY',source_name='Gitlab',target_identity_type='gitlab_login',api_keys_file='gitlab_api_keys.txt')
		new_kwargs.update(kwargs)
		github_gql.PRCommentsGQLFiller.__init__(self,**new_kwargs)

		self.pageinfo_path = ['node','notes','pageInfo']


	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
					repository:project(fullPath:"{repo_owner}/{repo_name}" ) {{
							nameWithOwner:fullPath }}
  					node:mergeRequest(id:"{pullrequest_gql_id}") {{
  									title
									id
									iid
									author {{ username }}
									description
									createdAt
									mergedAt

									notes(first:{page_size} {after_end_cursor}) {{
										# totalCount:count
										pageInfo {{
											endCursor
											hasNextPage
											}}
										nodes {{
											id
											createdAt
											author {{ username }}
											body
											
											}}
										}}

									
  							}}
						}}'''

	def parse_query_result(self,query_result,repo_id,identity_id,repo_owner=None,repo_name=None,**kwargs):
		'''
		In subclasses this has to be implemented
		output: [ {'repo_id':r_id,'repo_owner':r_ow,'repo_name':r_na,'pullrequest_number':pullrequest_na,'pullrequest_title':title_na,'created_at':created_date,'closed_at':closed_date} , ...]
		'''
		ans = []
		if repo_owner is None:
			repo_owner = query_result['repository']['nameWithOwner'].split('/')[0]
			repo_name = '/'.join(query_result['repository']['nameWithOwner'].split('/')[1:])
		for e in [query_result['node']]:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'pullrequest'}
			try:
				d['pullrequest_gql_id'] = e['id']
				d['pullrequest_number'] = e['iid']
				d['pullrequest_id'] = e['iid']
				
			except (KeyError,TypeError) as err:
				self.logger.info('Result triggering error: {} \nError when parsing pullrequests for {}/{}: {}'.format(e,repo_owner,repo_name,err))
				continue
			else:
				ans.append(d)
				if  len(e['notes']['nodes'])>0:
					for ee in e['notes']['nodes']:
						r = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'pullrequest_comment'}
						r['pullrequest_number'] = d['pullrequest_number']
						r['pullrequest_gql_id'] = d['pullrequest_gql_id']
						try:
							r['created_at'] = ee['createdAt']
							r['pullrequest_comment_text'] = ee['body']
							r['pullrequest_comment_id'] = int(ee['id'].split('/')[-1])
							r['pullrequest_comment_gql_id'] = ee['id']
							#r['comment_reactions_pageinfo'] = ee['reactions']['pageInfo']
							try:
								r['author_login'] = ee['author']['username']
							except:
								r['author_login'] = None


						except (KeyError,TypeError) as err:
							self.logger.info('Result triggering error: {} \nError when parsing pullrequest_comments for {}/{}: {}'.format(e,repo_owner,repo_name,err))
							continue
						else:
							ans.append(r)

		return ans

	def insert_items(self,db=None,**kwargs):

		if db is None:
			db = self.db

		CompletePullRequestsGQLFiller.insert_comments(self,db=db,**kwargs)
		#CompletePullRequestsGQLFiller.insert_comment_reactions(self,db=db,**kwargs)
		#CompletePullRequestsGQLFiller.insert_comment_reaction_updates(self,db=db,**kwargs)

		if len(kwargs['items_list']):
			db.cursor.execute('''INSERT INTO full_updates(update_type) SELECT 'pullrequest_comments' ;''')


	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		# return query_result['node']['notes']['totalCount']
		return 'NA'
