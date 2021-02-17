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

from repo_tools import fillers
from repo_tools.fillers import generic
from repo_tools.fillers import github_rest
import repo_tools as rp

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


class Requester(object):
	'''
	Class implementing the Request
	Caching rate limit information, updating at each query, or requerying after <refresh_time in sec> without update
	'''
	def __init__(self,api_key,refresh_time=120,schema=None,fetch_schema=False):
		self.logger = logger
		self.api_key = api_key
		self.remaining = 0
		self.reset_at = datetime.datetime.now() # Like on the API, reset time is last reset time, not future reset time
		self.refresh_time = refresh_time
		self.refreshed_at = None
		self.transport = AIOHTTPTransport(url="https://api.github.com/graphql",headers={'Accept-Encoding':'gzip','Authorization':'token {}'.format(self.api_key)})
		if schema is not None:
			self.client = Client(transport=self.transport, schema=schema)
			self.schema = schema
		elif fetch_schema:
			self.client = Client(transport=self.transport, fetch_schema_from_transport=True)
			self.schema = self.client.schema
		else:
			self.client = Client(transport=self.transport, fetch_schema_from_transport=False)
			self.schema = schema

	def clone(self):
		out_obj = self.__class__(api_key=self.api_key,refresh_time=self.refresh_time,fetch_schema=False,schema=self.schema)
		out_obj.refreshed_at = self.refreshed_at
		out_obj.reset_at = self.reset_at
		out_obj.remaining = self.remaining
		return out_obj


	def get_rate_limit(self,refresh=False):
		if refresh or self.refreshed_at is None or self.refreshed_at + datetime.timedelta(seconds=self.refresh_time)<= datetime.datetime.now():
			self.query('''
				query {
					rateLimit {
						cost
						remaining
						resetAt
					}
				}
				''')
		return self.remaining

	def query(self,gql_query,params=None):
		if params is not None:
			gql_query = gql_query.format(**params)
		RL_query = '''
				rateLimit {
					cost
					remaining
					resetAt
				}
		'''
		if 'rateLimit' not in gql_query:
			splitted_string = gql_query.split('}')
			gql_query = '}'.join(splitted_string[:-1])+RL_query+'}'+splitted_string[-1]

		try:
			result = self.client.execute(gql(gql_query))
		except Exception as e:
			if hasattr(e,'data'):
				result = e.data
				if result is None:
					raise
				else:
					self.logger.info('Exception catched, {} :{}, result: {}'.format(e.__class__,e,result))
			else:
				raise
		self.remaining = result['rateLimit']['remaining']
		self.reset_at = datetime.datetime.strptime(result['rateLimit']['resetAt'], '%Y-%m-%dT%H:%M:%SZ')
		self.reset_at = time.mktime(self.reset_at.timetuple()) # converting to seconds to epoch; to have same format as REST API
		self.refreshed_at = datetime.datetime.now()

		return result

	def paginated_query(self,gql_query,params=None,pageinfo_path=[]):
		pageinfo_path = copy.deepcopy(pageinfo_path)
		EC_var = 'after_end_cursor'
		if params is None:
			params = {}
		else:
			params = copy.deepcopy(params)
		if EC_var not in params.keys() or params[EC_var] is None:
			params[EC_var] = ''
		elif not params[EC_var].startswith(', after:'):
			params[EC_var] = ', after:"{}"'.format(params[EC_var])
		has_next_page = True
		while has_next_page:
			result = self.query(gql_query=gql_query,params=params)
			page_info = result
			try:
				for elt in pageinfo_path:
					page_info = page_info[elt]
			except (KeyError,TypeError):
				has_next_page = False
				end_cursor = None
			else:
				has_next_page = page_info['hasNextPage']
				end_cursor = page_info['endCursor']
			if end_cursor is None:
				params[EC_var] = ''
			else:
				params[EC_var] = ', after:"{}"'.format(end_cursor)
			yield copy.deepcopy(result),copy.deepcopy(page_info) # copying so that any usage of results fields in the generator cannot be corrupted between 2 yields






class GHGQLFiller(github_rest.GithubFiller):
	"""
	class to be inherited from, contains github credentials management
	"""

	def apply(self):
		self.fill_items(elt_list=self.elt_list,workers=self.workers,incremental_update=self.incremental_update)
		self.db.connection.commit()
		self.db.batch_merge_repos()

	def prepare(self):
		github_rest.GithubFiller.prepare(self)
		self.set_element_list()

	def get_rate_limit(self,requester):
		return requester.get_rate_limit()

	def get_reset_at(self,requester):
		return requester.reset_at

	def set_github_requesters(self,fetch_schema=False):
		'''
		Setting github requesters
		api keys file syntax, per line: API#notes
		'''
		github_requesters = []
		schema = None
		for ak in self.api_keys:
			g = Requester(api_key=ak,schema=schema,fetch_schema=fetch_schema)
			try:
				g.get_rate_limit()
			except:
				self.logger.info('API key starting with "{}" and of length {} not valid'.format(ak[:5],len(ak)))
			else:
				github_requesters.append(g)
			schema = g.client.schema
		if len(github_requesters) == 0:
			raise ValueError('No valid API key provided')
		self.github_requesters = github_requesters

	def get_github_requester(self,random_pick=True,in_thread=False,requesters=None):
		'''
		Going through requesters respecting threshold of minimum remaining api queries
		'''
		if requesters is None:
			requesters = self.github_requesters
		if in_thread:
			requesters = [rq.clone() for rq in requesters]
		return github_rest.GithubFiller.get_github_requester(self,random_pick=random_pick,requesters=requesters)

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		raise NotImplementedError

	def parse_query_result(self,query_result,**kwargs):
		'''
		In subclasses this has to be implemented
		output: a curated result, usable in insert_query_result
		'''
		raise NotImplementedError

	def insert_items(self,**kwargs):
		'''
		In subclasses this has to be implemented
		inserts results in the DB
		output: input data for db.insert_update
		'''
		raise NotImplementedError

	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		raise NotImplementedError

	def set_element_list(self):
		'''
		In subclasses this has to be implemented
		sets self.elt_list to be used in self.fill_items
		'''
		raise NotImplementedError


	def fill_items(self,elt_list=None,workers=1,in_thread=False,incremental_update=True,elt_nb=None,total_elt=None):
		'''
		Main loop to batch query on list of repos or users
		'''

		if elt_list is None:
			elt_list = self.elt_list

		if total_elt is None:
			total_elt = len(elt_list)
		if elt_nb is None:
			elt_nb = 0

		elt_list = copy.deepcopy(elt_list)

		if workers == 1:
			elt_name,owner,repo_name,end_cursor,login = None,None,None,None,None # init values for the exception

			try:
				if in_thread:
					db = self.db.copy()
				else:
					db = self.db
				requester_gen = self.get_github_requester(in_thread=in_thread)
				new_elt = True
				while len(elt_list):
					current_elt = elt_list[0]
					if self.queried_obj == 'repo':
						source,owner,repo_name,repo_id,end_cursor_orig = current_elt
						identity_id = None
						identity_type_id = None
						elt_name = '{}/{}'.format(owner,repo_name)
					else:
						identity_type_id,login,identity_id,end_cursor_orig = current_elt # source is here identity_type_id
						repo_id = None
						elt_name = login
					if new_elt:
						if incremental_update:
							end_cursor = end_cursor_orig
						else:
							end_cursor = None
						new_elt = False
						self.logger.info('Filling {} for {} {} ({}/{})'.format(self.items_name,self.queried_obj,elt_name,elt_nb,total_elt))
					else:
						end_cursor = pageinfo['endCursor']
					requester = next(requester_gen)

					params = {'repo_owner':owner,'repo_name':repo_name,'user_login':login,'after_end_cursor':end_cursor}

					# first request (with endcursor)
					paginated_query = requester.paginated_query(gql_query=self.query_string(),params=params,pageinfo_path=self.pageinfo_path)
					result,pageinfo = next(paginated_query)

					# catch non existent
					if (self.queried_obj=='repo' and result['repository'] is None) or (self.queried_obj=='user' and result['user'] is None):
						self.logger.info('No such {}: {} ({}/{})'.format(self.queried_obj,elt_name,elt_nb,total_elt))
						db.insert_update(identity_id=identity_id,repo_id=repo_id,table=self.items_name,success=False)
						elt_list.pop(0)
						new_elt = True
						elt_nb += 1
						continue

					# check repo fullname change and plan merge
					if self.queried_obj == 'repo':
						checked_repo_owner,checked_repo_name = result['repository']['nameWithOwner'].split('/')
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
								merging_reason_source='Repo redirect detected on github GraphQL API when processing {}'.format(self.items_name)
								)
							elt_name = '{}/{} ({}/{})'.format(checked_repo_owner,checked_repo_name,owner,repo_name)
						else:
							to_be_merged = False

					# detect 0 elts
					parsed_result = self.parse_query_result(result,repo_id=repo_id,identity_id=identity_id,identity_type_id=identity_type_id)
					if len(parsed_result) == 0:
						self.logger.info('No new {} for {} {} ({}/{})'.format(self.items_name,self.queried_obj,elt_name,elt_nb,total_elt))
						if end_cursor is None:
							end_cursor_json = None
						else:
							end_cursor_json = json.dumps({'end_cursor':end_cursor})
						db.insert_update(identity_id=identity_id,repo_id=repo_id,table=self.items_name,success=True,info=end_cursor_json)
						elt_list.pop(0)
						new_elt = True
						elt_nb += 1
						continue

					# loop
					while requester.get_rate_limit()>self.querymin_threshold:
						# insert results
						self.insert_items(items_list=parsed_result,commit=True,db=db)
						end_cursor = pageinfo['endCursor']
						if end_cursor is None:
							end_cursor_json = None
						else:
							end_cursor_json = json.dumps({'end_cursor':end_cursor})
						# detect loop end
						if not pageinfo['hasNextPage']:
							# insert update success True (+ end cursor)
							db.insert_update(identity_id=identity_id,repo_id=repo_id,table=self.items_name,success=True,info=end_cursor_json,autocommit=True)
							# clean partial updates with NULL success (?)
							# db.clean_null_updates(identity_id=identity_id,repo_id=repo_id,table=self.items_name,autocommit=True)
							# message with total count from result
							nb_items = self.get_nb_items(result)
							if nb_items is not None:
								self.logger.info('Filled {} for {} {} ({}/{}): {}'.format(self.items_name,self.queried_obj,elt_name,elt_nb,total_elt,nb_items))
							else:
								self.logger.info('Filled {} for {} {} ({}/{})'.format(self.items_name,self.queried_obj,elt_name,elt_nb,total_elt))

							elt_list.pop(0)
							new_elt = True
							elt_nb += 1
							break
						else:
							# insert partial update with endcursor value and success NULL (?)
							db.insert_update(identity_id=identity_id,repo_id=repo_id,table=self.items_name,success=None,info=end_cursor_json)
							db.connection.commit()
							# continue query
							result,pageinfo = next(paginated_query)
							parsed_result = self.parse_query_result(result,repo_id=repo_id,identity_id=identity_id,identity_type_id=identity_type_id)

			except Exception as e:
				if in_thread:
					self.logger.error('Exception in {} {}: \n {}: {}'.format(self.items_name,elt_name,e.__class__.__name__,e))
				db.log_error('Exception in {} {}: \n {}: {}'.format(self.items_name,elt_name,e.__class__.__name__,e))
				raise Exception('Exception in {} {}'.format(self.items_name,elt_name)) from e
			finally:
				if in_thread and 'db' in locals():
					db.cursor.close()
					db.connection.close()

		else:
			with ThreadPoolExecutor(max_workers=workers) as executor:
				futures = []
				for i,elt in enumerate(elt_list):
					futures.append(executor.submit(self.fill_items,elt_list=[elt],workers=1,in_thread=True,incremental_update=incremental_update,elt_nb=i+1,total_elt=total_elt))
				for future in futures:
					future.result()


class StarsGQLFiller(GHGQLFiller):
	'''
	Querying stars through the GraphQL API
	'''
	def __init__(self,**kwargs):
		self.items_name = 'stars'
		self.queried_obj = 'repo'
		self.pageinfo_path = ['repository','stargazers','pageInfo']
		GHGQLFiller.__init__(self,**kwargs)

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
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
		'''
		In subclasses this has to be implemented
		inserts results in the DB
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
				;''',((s['starred_at'],s['starrer_login'],s['repo_id'],s['starrer_login']) for s in items_list))
		else:
			db.cursor.executemany('''
					INSERT OR IGNORE INTO stars(starred_at,login,repo_id,identity_type_id,identity_id)
					VALUES(?,
							?,
							?,
							(SELECT id FROM identity_types WHERE name='github_login'),
							(SELECT id FROM identities WHERE identity=? AND identity_type_id=(SELECT id FROM identity_types WHERE name='github_login'))
						);''',((s['starred_at'],s['starrer_login'],s['repo_id'],s['starrer_login']) for s in items_list))

		if commit:
			db.connection.commit()

	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['repository']['stargazers']['totalCount']

	def set_element_list(self):
		'''
		In subclasses this has to be implemented
		sets self.elt_list to be used in self.fill_items
		'''

		#if force: all elts
		#if retry: all without success false or null on last update
		#else: all with success is null on lats update

		if self.db.db_type == 'postgres':
			if self.force:
				self.db.cursor.execute('''
						SELECT t1.sid,t1.rowner,t1.rname,t1.rid,t1.end_cursor FROM
							(SELECT s.id AS sid,r.owner AS rowner,r.name AS rname,r.id AS rid,tu.updated_at AS updated,tu.success AS succ, tu.info ->> 'end_cursor' as end_cursor
								FROM repositories r
								INNER JOIN sources s
								ON s.id=r.source AND s.name='GitHub'
								LEFT OUTER JOIN table_updates tu
								ON tu.repo_id=r.id AND tu.table_name='stars'
								ORDER BY r.owner,r.name,tu.updated_at ) as t1
							INNER JOIN
								(SELECT s.id AS sid,r.owner AS rowner,r.name AS rname,r.id AS rid,MAX(tu.updated_at) AS updated
								FROM repositories r
								INNER JOIN sources s
								ON s.id=r.source AND s.name='GitHub'
								LEFT OUTER JOIN table_updates tu
								ON tu.repo_id=r.id AND tu.table_name='stars'
								GROUP BY r.owner,r.name,r.id,s.id ) AS t2
						ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.rid=t2.rid
						ORDER BY t1.sid,t1.rowner,t1.rname
				;''')
			elif self.retry:
				self.db.cursor.execute('''
						SELECT t1.sid,t1.rowner,t1.rname,t1.rid,t1.end_cursor FROM
							(SELECT s.id AS sid,r.owner AS rowner,r.name AS rname,r.id AS rid,tu.updated_at AS updated,tu.success AS succ, tu.info ->> 'end_cursor' as end_cursor
								FROM repositories r
								INNER JOIN sources s
								ON s.id=r.source AND s.name='GitHub'
								LEFT OUTER JOIN table_updates tu
								ON tu.repo_id=r.id AND tu.table_name='stars'
								ORDER BY r.owner,r.name,tu.updated_at ) as t1
							INNER JOIN
								(SELECT s.id AS sid,r.owner AS rowner,r.name AS rname,r.id AS rid,MAX(tu.updated_at) AS updated
								FROM repositories r
								INNER JOIN sources s
								ON s.id=r.source AND s.name='GitHub'
								LEFT OUTER JOIN table_updates tu
								ON tu.repo_id=r.id AND tu.table_name='stars'
								GROUP BY r.owner,r.name,r.id,s.id ) AS t2
						ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.rid=t2.rid
						AND ((NOT t1.success) OR t1.succ IS NULL)
						ORDER BY t1.sid,t1.rowner,t1.rname
				;''')
			else:
				self.db.cursor.execute('''
						SELECT t1.sid,t1.rowner,t1.rname,t1.rid,t1.end_cursor FROM
							(SELECT s.id AS sid,r.owner AS rowner,r.name AS rname,r.id AS rid,tu.updated_at AS updated,tu.success AS succ, tu.info ->> 'end_cursor' as end_cursor
								FROM repositories r
								INNER JOIN sources s
								ON s.id=r.source AND s.name='GitHub'
								LEFT OUTER JOIN table_updates tu
								ON tu.repo_id=r.id AND tu.table_name='stars'
								ORDER BY r.owner,r.name,tu.updated_at ) as t1
							INNER JOIN
								(SELECT s.id AS sid,r.owner AS rowner,r.name AS rname,r.id AS rid,MAX(tu.updated_at) AS updated
								FROM repositories r
								INNER JOIN sources s
								ON s.id=r.source AND s.name='GitHub'
								LEFT OUTER JOIN table_updates tu
								ON tu.repo_id=r.id AND tu.table_name='stars'
								GROUP BY r.owner,r.name,r.id,s.id ) AS t2
						ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.rid=t2.rid
						AND t1.succ IS NULL
						ORDER BY t1.sid,t1.rowner,t1.rname
				;''')

			self.elt_list = list(self.db.cursor.fetchall())

		else:
			if self.force:
				self.db.cursor.execute('''
						SELECT t1.sid,t1.rowner,t1.rname,t1.rid,t1.end_cursor FROM
							(SELECT s.id AS sid,r.owner AS rowner,r.name AS rname,r.id AS rid,tu.updated_at AS updated,tu.success AS succ, tu.info as end_cursor
								FROM repositories r
								INNER JOIN sources s
								ON s.id=r.source AND s.name='GitHub'
								LEFT OUTER JOIN table_updates tu
								ON tu.repo_id=r.id AND tu.table_name='stars'
								ORDER BY r.owner,r.name,tu.updated_at ) as t1
							INNER JOIN
								(SELECT s.id AS sid,r.owner AS rowner,r.name AS rname,r.id AS rid,MAX(tu.updated_at) AS updated
								FROM repositories r
								INNER JOIN sources s
								ON s.id=r.source AND s.name='GitHub'
								LEFT OUTER JOIN table_updates tu
								ON tu.repo_id=r.id AND tu.table_name='stars'
								GROUP BY r.owner,r.name,r.id,s.id ) AS t2
						ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.rid=t2.rid
						ORDER BY t1.sid,t1.rowner,t1.rname
				;''')
			elif self.retry:
				self.db.cursor.execute('''
						SELECT t1.sid,t1.rowner,t1.rname,t1.rid,t1.end_cursor FROM
							(SELECT s.id AS sid,r.owner AS rowner,r.name AS rname,r.id AS rid,tu.updated_at AS updated,tu.success AS succ, tu.info as end_cursor
								FROM repositories r
								INNER JOIN sources s
								ON s.id=r.source AND s.name='GitHub'
								LEFT OUTER JOIN table_updates tu
								ON tu.repo_id=r.id AND tu.table_name='stars'
								ORDER BY r.owner,r.name,tu.updated_at ) as t1
							INNER JOIN
								(SELECT s.id AS sid,r.owner AS rowner,r.name AS rname,r.id AS rid,MAX(tu.updated_at) AS updated
								FROM repositories r
								INNER JOIN sources s
								ON s.id=r.source AND s.name='GitHub'
								LEFT OUTER JOIN table_updates tu
								ON tu.repo_id=r.id AND tu.table_name='stars'
								GROUP BY r.owner,r.name,r.id,s.id ) AS t2
						ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.rid=t2.rid
						AND ((NOT t1.success) OR t1.succ IS NULL)
						ORDER BY t1.sid,t1.rowner,t1.rname
				;''')
			else:
				self.db.cursor.execute('''
						SELECT t1.sid,t1.rowner,t1.rname,t1.rid,t1.end_cursor FROM
							(SELECT s.id AS sid,r.owner AS rowner,r.name AS rname,r.id AS rid,tu.updated_at AS updated,tu.success AS succ, tu.info as end_cursor
								FROM repositories r
								INNER JOIN sources s
								ON s.id=r.source AND s.name='GitHub'
								LEFT OUTER JOIN table_updates tu
								ON tu.repo_id=r.id AND tu.table_name='stars'
								ORDER BY r.owner,r.name,tu.updated_at ) as t1
							INNER JOIN
								(SELECT s.id AS sid,r.owner AS rowner,r.name AS rname,r.id AS rid,MAX(tu.updated_at) AS updated
								FROM repositories r
								INNER JOIN sources s
								ON s.id=r.source AND s.name='GitHub'
								LEFT OUTER JOIN table_updates tu
								ON tu.repo_id=r.id AND tu.table_name='stars'
								GROUP BY r.owner,r.name,r.id,s.id ) AS t2
						ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.rid=t2.rid
						AND t1.succ IS NULL
						ORDER BY t1.sid,t1.rowner,t1.rname
				;''')


			elt_list = list(self.db.cursor.fetchall())

			# specific to sqlite because no internal json parsing implemented in query
			self.elt_list = []
			for (source,owner,name,repo_id,end_cursor_info) in elt_list:
				try:
					self.elt_list.append((source,owner,name,repo_id,json.loads(end_cursor_info)['end_cursor']))
				except:
					self.elt_list.append((source,owner,name,repo_id,None))

		if self.start_offset is not None:
			self.elt_list = [r for r in self.elt_list if r[1]>=self.start_offset]


class SponsorsUserFiller(GHGQLFiller):
	'''
	Querying sponsors of users through the GraphQL API
	'''
	def __init__(self,**kwargs):
		self.items_name = 'sponsors_user'
		self.queried_obj = 'user'
		self.pageinfo_path = ['user','sponsorshipsAsMaintainer','pageInfo']
		GHGQLFiller.__init__(self,**kwargs)

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
					user(login:"{user_login}") {{
						login
						sponsorshipsAsMaintainer (includePrivate:true, first:100{after_end_cursor} ){{
							totalCount
							pageInfo {{
								endCursor
								hasNextPage
						 		}}
						 	nodes {{
								createdAt
								privacyLevel
								id
								tier {{
									updatedAt
									name
									description
									monthlyPriceInCents
									monthlyPriceInDollars
									}}
								sponsor {{
									login
								}}
							}}
						}}
					}}
				}}'''

	def parse_query_result(self,query_result,identity_id,identity_type_id,**kwargs):
		'''
		In subclasses this has to be implemented
		output: [ {'repo_id':r_id,'repo_owner':r_ow,'repo_name':r_na,'starrer_login':s_lo,'starred_at':st_at} , ...]
		'''
		ans = []
		if query_result['user'] is not None:
			user_login = query_result['user']['login']
			for e in query_result['user']['sponsorshipsAsMaintainer']['nodes']:
				d = {'sponsored_id':identity_id,'sponsored_login':user_login,'identity_type_id':identity_type_id}
				try:
					d['created_at'] = e['createdAt']
					d['external_id'] = e['id']
					if e['privacyLevel'] == 'PRIVATE' or e['sponsor'] is None:
						d['sponsor_login'] = None
					else:
						d['sponsor_login'] = e['sponsor']['login']
					if e['tier'] is None:
						d['tier'] = None
					else:
						d['tier'] = json.dumps(e['tier'])
				except KeyError as e:
					self.logger.info('KeyError when parsing sponsors_user for {}: {}'.format(user_login,e))
					continue
				else:
					ans.append(d)
		return ans


	def insert_items(self,items_list,commit=True,db=None):
		'''
		In subclasses this has to be implemented
		inserts results in the DB
		'''
		if db is None:
			db = self.db
		if db.db_type == 'postgres':
			extras.execute_batch(db.cursor,'''
				INSERT INTO sponsors_user(sponsored_id,sponsor_identity_type_id,sponsor_id,sponsor_login,created_at,external_id,tier)
				VALUES(%s,
						%s,
						(SELECT id FROM identities WHERE identity=%s AND identity_type_id=%s),
						%s,
						%s,
						%s,
						%s
					)
				ON CONFLICT DO NOTHING
				;''',((f['sponsored_id'],f['identity_type_id'],f['sponsor_login'],f['identity_type_id'],f['sponsor_login'],f['created_at'],f['external_id'],f['tier']) for f in items_list))
		else:
			db.cursor.executemany('''
				INSERT OR IGNORE INTO sponsors_user(sponsored_id,sponsor_identity_type_id,sponsor_id,sponsor_login,created_at,external_id,tier)
				VALUES(?,
						?,
						(SELECT id FROM identities WHERE identity=? AND identity_type_id=?),
						?,
						?,
						?,
						?
					)
				;''',((f['sponsored_id'],f['identity_type_id'],f['sponsor_login'],f['identity_type_id'],f['sponsor_login'],f['created_at'],f['external_id'],f['tier']) for f in items_list))
		if commit:
			db.connection.commit()


	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['user']['sponsorshipsAsMaintainer']['totalCount']

	def set_element_list(self):
		'''
		In subclasses this has to be implemented
		sets self.elt_list to be used in self.fill_items
		'''

		#if force: all elts
		#if retry: all without success false or null on last update
		#else: all with success is null on lats update

		if self.db.db_type == 'postgres':
			if self.force:
				self.db.cursor.execute('''
						SELECT t1.itid,t1.identity,t1.iid,t1.end_cursor FROM
							(SELECT it.id AS itid,i.identity as identity,i.id AS iid,tu.updated_at AS updated,tu.success AS succ, tu.info ->> 'end_cursor' as end_cursor
								FROM identities i
								INNER JOIN identity_types it
								ON it.id=i.identity_type_id AND it.name='github_login'
								LEFT OUTER JOIN table_updates tu
								ON tu.identity_id=i.id AND tu.table_name='sponsors_user'
								ORDER BY i.identity,tu.updated_at ) as t1
							INNER JOIN
								(SELECT it.id AS itid,i.identity as identity,i.id AS iid,MAX(tu.updated_at) AS updated
								FROM identities i
								INNER JOIN identity_types it
								ON it.id=i.identity_type_id AND it.name='github_login'
								LEFT OUTER JOIN table_updates tu
								ON tu.identity_id=i.id AND tu.table_name='sponsors_user'
								GROUP BY i.identity,i.id,it.id ) AS t2
						ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.iid=t2.iid
						ORDER BY t1.itid,t1.identity
				;''')
			elif self.retry:
				self.db.cursor.execute('''
						SELECT t1.itid,t1.identity,t1.iid,t1.end_cursor FROM
							(SELECT it.id AS itid,i.identity as identity,i.id AS iid,tu.updated_at AS updated,tu.success AS succ, tu.info ->> 'end_cursor' as end_cursor
								FROM identities i
								INNER JOIN identity_types it
								ON it.id=i.identity_type_id AND it.name='github_login'
								LEFT OUTER JOIN table_updates tu
								ON tu.identity_id=i.id AND tu.table_name='sponsors_user'
								ORDER BY i.identity,tu.updated_at ) as t1
							INNER JOIN
								(SELECT it.id AS itid,i.identity as identity,i.id AS iid,MAX(tu.updated_at) AS updated
								FROM identities i
								INNER JOIN identity_types it
								ON it.id=i.identity_type_id AND it.name='github_login'
								LEFT OUTER JOIN table_updates tu
								ON tu.identity_id=i.id AND tu.table_name='sponsors_user'
								GROUP BY i.identity,i.id,it.id ) AS t2
						ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.iid=t2.iid
						AND ((NOT t1.success) OR t1.succ IS NULL)
						ORDER BY t1.itid,t1.identity
				;''')
			else:
				self.db.cursor.execute('''
						SELECT t1.itid,t1.identity,t1.iid,t1.end_cursor FROM
							(SELECT it.id AS itid,i.identity as identity,i.id AS iid,tu.updated_at AS updated,tu.success AS succ, tu.info ->> 'end_cursor' as end_cursor
								FROM identities i
								INNER JOIN identity_types it
								ON it.id=i.identity_type_id AND it.name='github_login'
								LEFT OUTER JOIN table_updates tu
								ON tu.identity_id=i.id AND tu.table_name='sponsors_user'
								ORDER BY i.identity,tu.updated_at ) as t1
							INNER JOIN
								(SELECT it.id AS itid,i.identity as identity,i.id AS iid,MAX(tu.updated_at) AS updated
								FROM identities i
								INNER JOIN identity_types it
								ON it.id=i.identity_type_id AND it.name='github_login'
								LEFT OUTER JOIN table_updates tu
								ON tu.identity_id=i.id AND tu.table_name='sponsors_user'
								GROUP BY i.identity,i.id,it.id ) AS t2
						ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.iid=t2.iid
						AND t1.succ IS NULL
						ORDER BY t1.itid,t1.identity
				;''')

			self.elt_list = list(self.db.cursor.fetchall())

		else:
			if self.force:
				self.db.cursor.execute('''
						SELECT t1.itid,t1.identity,t1.iid,t1.end_cursor FROM
							(SELECT it.id AS itid,i.identity as identity,i.id AS iid,tu.updated_at AS updated,tu.success AS succ, tu.info as end_cursor
								FROM identities i
								INNER JOIN identity_types it
								ON it.id=i.identity_type_id AND it.name='github_login'
								LEFT OUTER JOIN table_updates tu
								ON tu.identity_id=i.id AND tu.table_name='sponsors_user'
								ORDER BY i.identity,tu.updated_at ) as t1
							INNER JOIN
								(SELECT it.id AS itid,i.identity as identity,i.id AS iid,MAX(tu.updated_at) AS updated
								FROM identities i
								INNER JOIN identity_types it
								ON it.id=i.identity_type_id AND it.name='github_login'
								LEFT OUTER JOIN table_updates tu
								ON tu.identity_id=i.id AND tu.table_name='sponsors_user'
								GROUP BY i.identity,i.id,it.id ) AS t2
						ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.iid=t2.iid
						ORDER BY t1.itid,t1.identity
				;''')
			elif self.retry:
				self.db.cursor.execute('''
						SELECT t1.itid,t1.identity,t1.iid,t1.end_cursor FROM
							(SELECT it.id AS itid,i.identity as identity,i.id AS iid,tu.updated_at AS updated,tu.success AS succ, tu.info as end_cursor
								FROM identities i
								INNER JOIN identity_types it
								ON it.id=i.identity_type_id AND it.name='github_login'
								LEFT OUTER JOIN table_updates tu
								ON tu.identity_id=i.id AND tu.table_name='sponsors_user'
								ORDER BY i.identity,tu.updated_at ) as t1
							INNER JOIN
								(SELECT it.id AS itid,i.identity as identity,i.id AS iid,MAX(tu.updated_at) AS updated
								FROM identities i
								INNER JOIN identity_types it
								ON it.id=i.identity_type_id AND it.name='github_login'
								LEFT OUTER JOIN table_updates tu
								ON tu.identity_id=i.id AND tu.table_name='sponsors_user'
								GROUP BY i.identity,i.id,it.id ) AS t2
						ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.iid=t2.iid
						AND ((NOT t1.success) OR t1.succ IS NULL)
						ORDER BY t1.itid,t1.identity
				;''')
			else:
				self.db.cursor.execute('''
						SELECT t1.itid,t1.identity,t1.iid,t1.end_cursor FROM
							(SELECT it.id AS itid,i.identity as identity,i.id AS iid,tu.updated_at AS updated,tu.success AS succ, tu.info as end_cursor
								FROM identities i
								INNER JOIN identity_types it
								ON it.id=i.identity_type_id AND it.name='github_login'
								LEFT OUTER JOIN table_updates tu
								ON tu.identity_id=i.id AND tu.table_name='sponsors_user'
								ORDER BY i.identity,tu.updated_at ) as t1
							INNER JOIN
								(SELECT it.id AS itid,i.identity as identity,i.id AS iid,MAX(tu.updated_at) AS updated
								FROM identities i
								INNER JOIN identity_types it
								ON it.id=i.identity_type_id AND it.name='github_login'
								LEFT OUTER JOIN table_updates tu
								ON tu.identity_id=i.id AND tu.table_name='sponsors_user'
								GROUP BY i.identity,i.id,it.id ) AS t2
						ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.iid=t2.iid
						AND t1.succ IS NULL
						ORDER BY t1.itid,t1.identity
				;''')


			elt_list = list(self.db.cursor.fetchall())

			# specific to sqlite because no internal json parsing implemented in query
			self.elt_list = []
			for (identity_type_id,login,identity_id,end_cursor_info) in elt_list:
				try:
					self.elt_list.append((identity_type_id,login,identity_id,json.loads(end_cursor_info)['end_cursor']))
				except:
					self.elt_list.append((identity_type_id,login,identity_id,None))

		if self.start_offset is not None:
			self.elt_list = [r for r in self.elt_list if r[1]>=self.start_offset]

class FollowersGQLFiller(GHGQLFiller):
	'''
	Querying followers through the GraphQL API
	'''
	def __init__(self,**kwargs):
		self.items_name = 'followers'
		self.queried_obj = 'user'
		self.pageinfo_path = ['user','followers','pageInfo']
		GHGQLFiller.__init__(self,**kwargs)

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
					user(login:"{user_login}") {{
						login
						followers (first:100 {after_end_cursor} ){{
						 totalCount
						 pageInfo {{
							endCursor
							hasNextPage
						 }}
						 edges {{
							node {{
								login
							}}
						 }}
						}}
					}}
				}}'''

	def parse_query_result(self,query_result,identity_id,identity_type_id,**kwargs):
		'''
		In subclasses this has to be implemented
		output: [ {'repo_id':r_id,'repo_owner':r_ow,'repo_name':r_na,'starrer_login':s_lo,'starred_at':st_at} , ...]
		'''
		ans = []
		user_login = query_result['user']['login']
		for e in query_result['user']['followers']['edges']:
			d = {'identity_id':identity_id,'user_login':user_login,'identity_type_id':identity_type_id}
			try:
				d['follower_login'] = e['node']['login']
			except KeyError as e:
				self.logger.info('KeyError when parsing followers for {}: {}'.format(user_login,e))
				continue
			else:
				ans.append(d)
		return ans


	def insert_items(self,items_list,commit=True,db=None):
		'''
		In subclasses this has to be implemented
		inserts results in the DB
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
				;''',((f['identity_type_id'],f['follower_login'],f['follower_login'],f['identity_type_id'],f['identity_id'],) for f in items_list))
		else:
			db.cursor.executemany('''
				INSERT OR IGNORE INTO followers(follower_identity_type_id,follower_login,follower_id,followee_id)
				VALUES(?,
						?,
						(SELECT id FROM identities WHERE identity=? AND identity_type_id=?),
						?
					)
				;''',((f['identity_type_id'],f['follower_login'],f['follower_login'],f['identity_type_id'],f['identity_id'],) for f in items_list))
		if commit:
			db.connection.commit()


	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['user']['followers']['totalCount']

	def set_element_list(self):
		'''
		In subclasses this has to be implemented
		sets self.elt_list to be used in self.fill_items
		'''

		#if force: all elts
		#if retry: all without success false or null on last update
		#else: all with success is null on lats update

		if self.db.db_type == 'postgres':
			if self.force:
				self.db.cursor.execute('''
						SELECT t1.itid,t1.identity,t1.iid,t1.end_cursor FROM
							(SELECT it.id AS itid,i.identity as identity,i.id AS iid,tu.updated_at AS updated,tu.success AS succ, tu.info ->> 'end_cursor' as end_cursor
								FROM identities i
								INNER JOIN identity_types it
								ON it.id=i.identity_type_id AND it.name='github_login'
								LEFT OUTER JOIN table_updates tu
								ON tu.identity_id=i.id AND tu.table_name='followers'
								ORDER BY i.identity,tu.updated_at ) as t1
							INNER JOIN
								(SELECT it.id AS itid,i.identity as identity,i.id AS iid,MAX(tu.updated_at) AS updated
								FROM identities i
								INNER JOIN identity_types it
								ON it.id=i.identity_type_id AND it.name='github_login'
								LEFT OUTER JOIN table_updates tu
								ON tu.identity_id=i.id AND tu.table_name='followers'
								GROUP BY i.identity,i.id,it.id ) AS t2
						ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.iid=t2.iid
						ORDER BY t1.itid,t1.identity
				;''')
			elif self.retry:
				self.db.cursor.execute('''
						SELECT t1.itid,t1.identity,t1.iid,t1.end_cursor FROM
							(SELECT it.id AS itid,i.identity as identity,i.id AS iid,tu.updated_at AS updated,tu.success AS succ, tu.info ->> 'end_cursor' as end_cursor
								FROM identities i
								INNER JOIN identity_types it
								ON it.id=i.identity_type_id AND it.name='github_login'
								LEFT OUTER JOIN table_updates tu
								ON tu.identity_id=i.id AND tu.table_name='followers'
								ORDER BY i.identity,tu.updated_at ) as t1
							INNER JOIN
								(SELECT it.id AS itid,i.identity as identity,i.id AS iid,MAX(tu.updated_at) AS updated
								FROM identities i
								INNER JOIN identity_types it
								ON it.id=i.identity_type_id AND it.name='github_login'
								LEFT OUTER JOIN table_updates tu
								ON tu.identity_id=i.id AND tu.table_name='followers'
								GROUP BY i.identity,i.id,it.id ) AS t2
						ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.iid=t2.iid
						AND ((NOT t1.success) OR t1.succ IS NULL)
						ORDER BY t1.itid,t1.identity
				;''')
			else:
				self.db.cursor.execute('''
						SELECT t1.itid,t1.identity,t1.iid,t1.end_cursor FROM
							(SELECT it.id AS itid,i.identity as identity,i.id AS iid,tu.updated_at AS updated,tu.success AS succ, tu.info ->> 'end_cursor' as end_cursor
								FROM identities i
								INNER JOIN identity_types it
								ON it.id=i.identity_type_id AND it.name='github_login'
								LEFT OUTER JOIN table_updates tu
								ON tu.identity_id=i.id AND tu.table_name='followers'
								ORDER BY i.identity,tu.updated_at ) as t1
							INNER JOIN
								(SELECT it.id AS itid,i.identity as identity,i.id AS iid,MAX(tu.updated_at) AS updated
								FROM identities i
								INNER JOIN identity_types it
								ON it.id=i.identity_type_id AND it.name='github_login'
								LEFT OUTER JOIN table_updates tu
								ON tu.identity_id=i.id AND tu.table_name='followers'
								GROUP BY i.identity,i.id,it.id ) AS t2
						ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.iid=t2.iid
						AND t1.succ IS NULL
						ORDER BY t1.itid,t1.identity
				;''')

			self.elt_list = list(self.db.cursor.fetchall())

		else:
			if self.force:
				self.db.cursor.execute('''
						SELECT t1.itid,t1.identity,t1.iid,t1.end_cursor FROM
							(SELECT it.id AS itid,i.identity as identity,i.id AS iid,tu.updated_at AS updated,tu.success AS succ, tu.info as end_cursor
								FROM identities i
								INNER JOIN identity_types it
								ON it.id=i.identity_type_id AND it.name='github_login'
								LEFT OUTER JOIN table_updates tu
								ON tu.identity_id=i.id AND tu.table_name='followers'
								ORDER BY i.identity,tu.updated_at ) as t1
							INNER JOIN
								(SELECT it.id AS itid,i.identity as identity,i.id AS iid,MAX(tu.updated_at) AS updated
								FROM identities i
								INNER JOIN identity_types it
								ON it.id=i.identity_type_id AND it.name='github_login'
								LEFT OUTER JOIN table_updates tu
								ON tu.identity_id=i.id AND tu.table_name='followers'
								GROUP BY i.identity,i.id,it.id ) AS t2
						ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.iid=t2.iid
						ORDER BY t1.itid,t1.identity
				;''')
			elif self.retry:
				self.db.cursor.execute('''
						SELECT t1.itid,t1.identity,t1.iid,t1.end_cursor FROM
							(SELECT it.id AS itid,i.identity as identity,i.id AS iid,tu.updated_at AS updated,tu.success AS succ, tu.info as end_cursor
								FROM identities i
								INNER JOIN identity_types it
								ON it.id=i.identity_type_id AND it.name='github_login'
								LEFT OUTER JOIN table_updates tu
								ON tu.identity_id=i.id AND tu.table_name='followers'
								ORDER BY i.identity,tu.updated_at ) as t1
							INNER JOIN
								(SELECT it.id AS itid,i.identity as identity,i.id AS iid,MAX(tu.updated_at) AS updated
								FROM identities i
								INNER JOIN identity_types it
								ON it.id=i.identity_type_id AND it.name='github_login'
								LEFT OUTER JOIN table_updates tu
								ON tu.identity_id=i.id AND tu.table_name='followers'
								GROUP BY i.identity,i.id,it.id ) AS t2
						ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.iid=t2.iid
						AND ((NOT t1.success) OR t1.succ IS NULL)
						ORDER BY t1.itid,t1.identity
				;''')
			else:
				self.db.cursor.execute('''
						SELECT t1.itid,t1.identity,t1.iid,t1.end_cursor FROM
							(SELECT it.id AS itid,i.identity as identity,i.id AS iid,tu.updated_at AS updated,tu.success AS succ, tu.info as end_cursor
								FROM identities i
								INNER JOIN identity_types it
								ON it.id=i.identity_type_id AND it.name='github_login'
								LEFT OUTER JOIN table_updates tu
								ON tu.identity_id=i.id AND tu.table_name='followers'
								ORDER BY i.identity,tu.updated_at ) as t1
							INNER JOIN
								(SELECT it.id AS itid,i.identity as identity,i.id AS iid,MAX(tu.updated_at) AS updated
								FROM identities i
								INNER JOIN identity_types it
								ON it.id=i.identity_type_id AND it.name='github_login'
								LEFT OUTER JOIN table_updates tu
								ON tu.identity_id=i.id AND tu.table_name='followers'
								GROUP BY i.identity,i.id,it.id ) AS t2
						ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.iid=t2.iid
						AND t1.succ IS NULL
						ORDER BY t1.itid,t1.identity
				;''')


			elt_list = list(self.db.cursor.fetchall())

			# specific to sqlite because no internal json parsing implemented in query
			self.elt_list = []
			for (identity_type_id,login,identity_id,end_cursor_info) in elt_list:
				try:
					self.elt_list.append((identity_type_id,login,identity_id,json.loads(end_cursor_info)['end_cursor']))
				except:
					self.elt_list.append((identity_type_id,login,identity_id,None))

		if self.start_offset is not None:
			self.elt_list = [r for r in self.elt_list if r[1]>=self.start_offset]

'''
stars:

query {{
	repository(owner:"{OWNER}", name:"{NAME}") {{
		stargazers (first:100{} ){{
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
}}

sponsors:

query {{
	user(login:"{LOGIN}") {{
		sponsorshipsAsMaintainer (includePrivate:true, first:100{} ){{
		 totalCount
		 pageInfo {{
			endCursor
			hasNextPage
		 }}
		 nodes {{
		 	createdAt
		 	privacyLevel
			sponsor {{
				login
			}}
		 }}
		}}
	}}
}}
'''
