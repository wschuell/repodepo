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
import dateutil
from dateutil.relativedelta import relativedelta
import asyncio
import time
import random

from .. import fillers
from ..fillers import generic
from ..fillers import github_rest

import gql
from gql import Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.exceptions import TransportProtocolError

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
	def __init__(self,api_key,refresh_time=120,schema=None,fetch_schema=False,url="https://api.github.com/graphql",auth_header_prefix='token ',retries=50,secondary_limit_wait=300):
		self.logger = logger
		self.retries = retries
		self.api_key = api_key
		self.remaining = 0
		self.reset_at = datetime.datetime.now() # Like on the API, reset time is last reset time, not future reset time
		self.refresh_time = refresh_time
		self.secondary_limit_wait = secondary_limit_wait
		self.refreshed_at = None
		self.url = url
		self.auth_header_prefix = auth_header_prefix
		self.transport = AIOHTTPTransport(url=self.url,headers={'Accept-Encoding':'gzip','Authorization':'{}{}'.format(self.auth_header_prefix,self.api_key)})

		if schema is not None:
			self.client = Client(transport=self.transport, schema=schema)
			self.schema = schema
		elif fetch_schema:
			self.client = Client(transport=self.transport, fetch_schema_from_transport=True)
			self.schema = self.client.schema
		else:
			self.client = Client(transport=self.transport, fetch_schema_from_transport=False)
			self.schema = schema

	def check_scope(self,scope):
		if 'X-OAuth-Scopes' not in self.client.transport.response_headers.keys():
			self.get_rate_limit() 
		return scope in self.client.transport.response_headers['X-OAuth-Scopes'].split(', ')

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

	def query(self,gql_query,params=None,retries=None):
		if retries is None:
			retries = self.retries
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
			retries_left = retries
			result_found = False
			sec_limit_detected = False
			while not result_found:
				try:
					result = self.client.execute(gql.gql(gql_query))
					result_found = True
				except Exception as e:
					if e.__class__ == asyncio.TimeoutError:
						if retries_left>0:
							time.sleep(0.1*(retries-retries_left)*random.random())
							retries_left -= 1
						else:
							raise e.__class__('''TimeoutError happened more times than the set retries: {}. Rerun, maybe with higher value.
Original error message: {}'''.format(retries,e))
					# if hasattr(e,'errors') and e.errors[0]['message'] ==  "You have exceeded a secondary rate limit. Please wait a few minutes before you try again.":
					if e.__class__ == TransportProtocolError and "You have exceeded a secondary rate limit. Please wait a few minutes before you try again." in str(e):
						if retries_left > 0:
							self.logger.info(f'Secondary rate limit exceeded, waiting {self.secondary_limit_wait}s')
							time.sleep(self.secondary_limit_wait)
							if sec_limit_detected:
								self.secondary_limit_wait += 60
							sec_limit_detected = True
							retries_left -= 1
						else:
							raise e.__class__('''Secondary rate limit error happened more times than the set retries: {}. Rerun, maybe with higher value.
Original error message: {}'''.format(retries,e))
					elif hasattr(e,'errors') and len(e.errors) and 'type' in e.errors[0].keys() and e.errors[0]['type'] == 'RATE_LIMITED':
						self.get_rate_limit()
						reset_time = int(self.reset_at - time.time())
						if reset_time <= 0:
							reset_time += 3600
						self.logger.info(f'RATE_LIMITED error detected, sleeping until reset: {reset_time+1}s')
						time.sleep(reset_time+1)
					else:
						raise

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

	def paginated_query(self,gql_query,params=None,pageinfo_path=[],retries=None):
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
			result = self.query(gql_query=gql_query,params=params,retries=retries)
			if pageinfo_path is None:
				page_info = {'hasNextPage':False,'endCursor':None}
				has_next_page = False
				end_cursor = None
			else:
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
	scopes = ('read:user',)

	def __init__(self,
			requester_class=None,
			source_name='GitHub',
			target_identity_type='github_login',
			retry_fails_permanent=False,
			init_page_size=10,
			max_page_size=100,
			secondary_page_size=None,
			other_update_names = None,
			**kwargs):
		if requester_class is None:
			self.Requester = Requester
		else:
			self.Requester = requester_class
		self.source_name = source_name
		self.retry_fails_permanent = retry_fails_permanent
		self.target_identity_type = target_identity_type
		self.init_page_size = init_page_size
		self.max_page_size = max_page_size
		self.page_counts = 0
		if secondary_page_size is None:
			self.secondary_page_size = self.init_page_size
		else:
			self.secondary_page_size = secondary_page_size
		if other_update_names is None:
			self.other_update_names = []
		else:
			self.other_update_names = copy.deepcopy(other_update_names)
		if not hasattr(self,'sub_queried_obj'):
			self.sub_queried_obj = self.queried_obj
		github_rest.GithubFiller.__init__(self,identity_type=target_identity_type,**kwargs)

	def get_generic_kwargs(self):
		return dict(
				requester_class=self.Requester,
				source_name=self.source_name,
				target_identity_type=self.target_identity_type,
				retry_fails_permanent=self.retry_fails_permanent,
				init_page_size=self.init_page_size,
				max_page_size=self.max_page_size,
				secondary_page_size=self.secondary_page_size,
				# other_update_names = self.other_update_names,
				data_folder=self.data_folder,
				querymin_threshold=self.querymin_threshold,
				per_page=self.per_page,
				env_apikey=self.env_apikey,
				workers=self.workers,
				# identity_type=self.identity_type,
				no_unauth=self.no_unauth,
				api_keys_file=self.api_keys_file,
				api_keys=self.api_keys,
				fail_on_wait=self.fail_on_wait,
				start_offset=self.start_offset,
				retry=self.retry,
				force=self.force,
				incremental_update=self.incremental_update,
				)

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

	def get_page_size(self):
		self.page_counts += 1
		if self.page_counts == 1:
			return self.init_page_size
		else:
			return self.max_page_size

	def insert_update(self,db=None,**kwargs):
		if db is None:
			db = self.db
		db.insert_update(table=self.items_name,**kwargs)
		for t in self.other_update_names:
			db.insert_update(table=t,**kwargs)

	def set_requesters(self,fetch_schema=False):
		'''
		Setting requesters
		api keys file syntax, per line: API#notes
		'''
		requesters = []
		schema = None
		for ak in self.api_keys:
			g = self.Requester(api_key=ak,schema=schema,fetch_schema=fetch_schema)
			try:
				g.get_rate_limit()
			except Exception as e:
				self.logger.info('API key starting with "{}" and of length {} not valid: {}:{}'.format(ak[:5],len(ak),e.__class__,e))
			else:
				missing_scopes = []
				for s in self.scopes:
					if not g.check_scope(scope=s):
						missing_scopes.append(s)
				if len(missing_scopes) > 0:
					self.logger.info('API key starting with "{}" missing scopes: {}'.format(ak[:5],missing_scopes))
				else:
					requesters.append(g)

			schema = g.client.schema
		if len(requesters) == 0:
			raise ValueError('No valid API key provided')
		self.requesters = requesters

	def get_requester(self,random_pick=True,in_thread=False,requesters=None):
		'''
		Going through requesters respecting threshold of minimum remaining api queries
		'''
		if requesters is None:
			requesters = self.requesters
		if in_thread:
			requesters = [rq.clone() for rq in requesters]
		return github_rest.GithubFiller.get_requester(self,random_pick=random_pick,requesters=requesters)

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		raise NotImplementedError

	def additional_query_attributes(self):
		'''
		Possibility to add specific parameters for the query formatting
		'''
		return {}

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
		output: input data for self.insert_update
		'''
		raise NotImplementedError

	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
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
			pageinfo = {'endCursor':end_cursor}
			try:
				if in_thread:
					db = self.db.copy()
				else:
					db = self.db
				requester_gen = self.get_requester(in_thread=in_thread)
				new_elt = True
				while len(elt_list):
					current_elt = elt_list[0]
					local_additional_query_attributes = {}
					update_info = {}
					if self.queried_obj == 'repo':
						if self.sub_queried_obj != 'repo':
							source,owner,repo_name,repo_id,sq_id,sq_gql_id,end_cursor_orig = current_elt
							local_additional_query_attributes[self.sub_queried_obj+'_id'] = sq_id
							local_additional_query_attributes[self.sub_queried_obj+'_gql_id'] = sq_gql_id
							update_info[self.sub_queried_obj+'_id'] = sq_id
							update_info[self.sub_queried_obj+'_gql_id'] = sq_gql_id
						else:
							source,owner,repo_name,repo_id,end_cursor_orig = current_elt
						identity_id = None
						identity_type_id = None
						email = None
						commit_sha = None
						elt_name = '{}/{}'.format(owner,repo_name)
					elif self.queried_obj == 'email':
						source,owner,repo_name,repo_id,commit_sha,email,identity_id,identity_type_id = current_elt
						elt_name = email
						end_cursor_orig = None
					else:
						identity_type_id,login,identity_id,end_cursor_orig = current_elt # source is here identity_type_id
						repo_id = None
						email = None
						commit_sha = None
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

					params = {'repo_owner':owner,'repo_name':repo_name,'user_login':login,'commit_sha':commit_sha,'after_end_cursor':end_cursor,'page_size':self.get_page_size(),'secondary_page_size':self.secondary_page_size}
					params.update(self.additional_query_attributes())
					params.update(local_additional_query_attributes)
					# first request (with endcursor)
					paginated_query = requester.paginated_query(gql_query=self.query_string(),params=params,pageinfo_path=self.pageinfo_path)
					try:
						result,pageinfo = next(paginated_query)
					except asyncio.TimeoutError as e:
						if self.retry_fails_permanent:
							err_text = 'Timeout threshold reached {}, marking query as to be discarded {}: {}'.format(requester.retries,e.__class__,e)
							self.logger.error(err_text)
							db.log_error(err_text)
							self.insert_update(db=db,identity_id=identity_id,repo_id=repo_id,success=False,info=update_info)
							elt_list.pop(0)
							new_elt = True
							elt_nb += 1
							continue
						else:
							raise

					# catch non existent
					if (self.queried_obj=='repo' and result['repository'] is None) or (self.queried_obj=='user' and result['user'] is None):
						self.logger.info('No such {}: {} ({}/{})'.format(self.queried_obj,elt_name,elt_nb,total_elt))
						self.insert_update(db=db,identity_id=identity_id,repo_id=repo_id,success=False,info=update_info)
						elt_list.pop(0)
						new_elt = True
						elt_nb += 1
						continue

					# check repo fullname change and plan merge
					if self.queried_obj == 'repo':
						checked_repo_owner,checked_repo_name = result['repository']['nameWithOwner'].split('/')
						if (checked_repo_owner,checked_repo_name) != (owner,repo_name):
							to_be_merged = True
							db.plan_repo_merge(
								new_id=None,
								new_source=None,
								new_owner=checked_repo_owner,
								new_name=checked_repo_name,
								obsolete_id=repo_id,
								obsolete_source=self.source_name,
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
						# if end_cursor is None:
						# 	end_cursor_json = None
						# else:
						# 	end_cursor_json = json.dumps({'end_cursor':end_cursor})
						if end_cursor is not None:
							update_info.update({'end_cursor':end_cursor})
						self.insert_update(db=db,identity_id=identity_id,repo_id=repo_id,success=True,info=update_info)
						elt_list.pop(0)
						new_elt = True
						elt_nb += 1
						continue

					if (self.queried_obj=='email' and parsed_result[0]['repo_owner'] is None):
						self.logger.info('No such repo: {}/{} for email {} ({}/{})'.format(owner,repo_name,elt_name,elt_nb,total_elt))
						self.insert_update(db=db,identity_id=identity_id,repo_id=repo_id,success=False,info=update_info)
						elt_list.pop(0)
						new_elt = True
						elt_nb += 1
						continue
					elif (self.queried_obj=='email' and parsed_result[0]['commit_sha'] is None):
						self.logger.info('No such commit: {} for email {} ({}/{})'.format(commit_sha,elt_name,elt_nb,total_elt))
						self.insert_update(db=db,identity_id=identity_id,repo_id=repo_id,success=False,info=update_info)
						elt_list.pop(0)
						new_elt = True
						elt_nb += 1
						continue

					# loop
					while requester.get_rate_limit()>self.querymin_threshold:
						# insert results
						self.insert_items(items_list=parsed_result,commit=True,db=db)
						end_cursor = pageinfo['endCursor']
						if end_cursor is not None:
							update_info.update({'end_cursor':end_cursor})
						# detect loop end
						if not pageinfo['hasNextPage']:
							# insert update success True (+ end cursor)
							self.insert_update(db=db,identity_id=identity_id,repo_id=repo_id,success=True,info=update_info,autocommit=True)
							db.connection.commit()
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
							self.insert_update(db=db,identity_id=identity_id,repo_id=repo_id,success=None,info=update_info)
							db.connection.commit()
							# continue query
							try:
								result,pageinfo = next(paginated_query)
							except asyncio.TimeoutError as e:
								if self.retry_fails_permanent:
									err_text = 'Timeout threshold reached {}, marking query as to be discarded {}: {}'.format(requester.retries,e.__class__,e)
									self.logger.error(err_text)
									db.log_error(err_text)
									self.insert_update(db=db,identity_id=identity_id,repo_id=repo_id,success=False,info=update_info)
									elt_list.pop(0)
									new_elt = True
									elt_nb += 1
									break
								else:
									raise
							parsed_result = self.parse_query_result(result,repo_id=repo_id,identity_id=identity_id,identity_type_id=identity_type_id)
			except KeyboardInterrupt:
				raise
			except Exception as e:
				err_text = 'Exception in {} {}: \n {}: {}'.format(self.items_name,elt_name,e.__class__.__name__,e)
				if e.__class__ == RuntimeError and 'cannot schedule new futures after shutdown' in str(e):
					raise
				elif e.__class__ == asyncio.TimeoutError and self.retry_fails_permanent:
					self.insert_update(db=db,identity_id=identity_id,repo_id=repo_id,success=False,info=update_info)
					self.logger.error(err_text+' retry_fails_permanent is set to True')
				if in_thread:
					self.logger.error(err_text)
				db.log_error(err_text)
				raise Exception(err_text) from e
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
					try:
						future.result()
					except KeyboardInterrupt:
						executor.shutdown(wait=False)
						break


	def set_element_list(self,sub_queried_obj=None,queried_obj=None,items_name=None):

		if sub_queried_obj is None:
			sub_queried_obj = self.sub_queried_obj
		if queried_obj is None:
			queried_obj = self.queried_obj
		if items_name is None:
			items_name = self.items_name

		if sub_queried_obj == 'repo':
			if self.db.db_type == 'postgres':
				if self.force:
					self.db.cursor.execute('''
							SELECT t1.sid,t1.rowner,t1.rname,t1.rid,t1.end_cursor FROM
								(SELECT s.id AS sid,r.owner AS rowner,r.name AS rname,r.id AS rid,tu.updated_at AS updated,tu.success AS succ, tu.info ->> 'end_cursor' as end_cursor
									FROM repositories r
									INNER JOIN sources s
									ON s.id=r.source AND s.name=%(source_name)s
									LEFT OUTER JOIN table_updates tu
									ON tu.repo_id=r.id AND tu.table_name=%(table_name)s
									ORDER BY r.owner,r.name,tu.updated_at ) as t1
								INNER JOIN
									(SELECT s.id AS sid,r.owner AS rowner,r.name AS rname,r.id AS rid,MAX(tu.updated_at) AS updated
									FROM repositories r
									INNER JOIN sources s
									ON s.id=r.source AND s.name=%(source_name)s
									LEFT OUTER JOIN table_updates tu
									ON tu.repo_id=r.id AND tu.table_name=%(table_name)s
									GROUP BY r.owner,r.name,r.id,s.id ) AS t2
							ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.rid=t2.rid
							ORDER BY t1.sid,t1.rowner,t1.rname
					;''',{'source_name':self.source_name,'table_name':items_name})
				elif self.retry:
					self.db.cursor.execute('''
							SELECT t1.sid,t1.rowner,t1.rname,t1.rid,t1.end_cursor FROM
								(SELECT s.id AS sid,r.owner AS rowner,r.name AS rname,r.id AS rid,tu.updated_at AS updated,tu.success AS succ, tu.info ->> 'end_cursor' as end_cursor
									FROM repositories r
									INNER JOIN sources s
									ON s.id=r.source AND s.name=%(source_name)s
									LEFT OUTER JOIN table_updates tu
									ON tu.repo_id=r.id AND tu.table_name=%(table_name)s
									ORDER BY r.owner,r.name,tu.updated_at ) as t1
								INNER JOIN
									(SELECT s.id AS sid,r.owner AS rowner,r.name AS rname,r.id AS rid,MAX(tu.updated_at) AS updated
									FROM repositories r
									INNER JOIN sources s
									ON s.id=r.source AND s.name=%(source_name)s
									LEFT OUTER JOIN table_updates tu
									ON tu.repo_id=r.id AND tu.table_name=%(table_name)s
									GROUP BY r.owner,r.name,r.id,s.id ) AS t2
							ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.rid=t2.rid
							AND ((NOT t1.success) OR t1.succ IS NULL)
							ORDER BY t1.sid,t1.rowner,t1.rname
					;''',{'source_name':self.source_name,'table_name':items_name})
				else:
					self.db.cursor.execute('''
							SELECT t1.sid,t1.rowner,t1.rname,t1.rid,t1.end_cursor FROM
								(SELECT s.id AS sid,r.owner AS rowner,r.name AS rname,r.id AS rid,tu.updated_at AS updated,tu.success AS succ, tu.info ->> 'end_cursor' as end_cursor
									FROM repositories r
									INNER JOIN sources s
									ON s.id=r.source AND s.name=%(source_name)s
									LEFT OUTER JOIN table_updates tu
									ON tu.repo_id=r.id AND tu.table_name=%(table_name)s
									ORDER BY r.owner,r.name,tu.updated_at ) as t1
								INNER JOIN
									(SELECT s.id AS sid,r.owner AS rowner,r.name AS rname,r.id AS rid,MAX(tu.updated_at) AS updated
									FROM repositories r
									INNER JOIN sources s
									ON s.id=r.source AND s.name=%(source_name)s
									LEFT OUTER JOIN table_updates tu
									ON tu.repo_id=r.id AND tu.table_name=%(table_name)s
									GROUP BY r.owner,r.name,r.id,s.id ) AS t2
							ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.rid=t2.rid
							AND t1.succ IS NULL
							ORDER BY t1.sid,t1.rowner,t1.rname
					;''',{'source_name':self.source_name,'table_name':items_name})

				self.elt_list = list(self.db.cursor.fetchall())

			else:
				if self.force:
					self.db.cursor.execute('''
							SELECT t1.sid,t1.rowner,t1.rname,t1.rid,t1.end_cursor FROM
								(SELECT s.id AS sid,r.owner AS rowner,r.name AS rname,r.id AS rid,tu.updated_at AS updated,tu.success AS succ, json_extract(tu.info,'$.end_cursor') as end_cursor
									FROM repositories r
									INNER JOIN sources s
									ON s.id=r.source AND s.name=:source_name
									LEFT OUTER JOIN table_updates tu
									ON tu.repo_id=r.id AND tu.table_name=:table_name
									ORDER BY r.owner,r.name,tu.updated_at ) as t1
								INNER JOIN
									(SELECT s.id AS sid,r.owner AS rowner,r.name AS rname,r.id AS rid,MAX(tu.updated_at) AS updated
									FROM repositories r
									INNER JOIN sources s
									ON s.id=r.source AND s.name=:source_name
									LEFT OUTER JOIN table_updates tu
									ON tu.repo_id=r.id AND tu.table_name=:table_name
									GROUP BY r.owner,r.name,r.id,s.id ) AS t2
							ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.rid=t2.rid
							ORDER BY t1.sid,t1.rowner,t1.rname
					;''',{'source_name':self.source_name,'table_name':items_name})
				elif self.retry:
					self.db.cursor.execute('''
							SELECT t1.sid,t1.rowner,t1.rname,t1.rid,t1.end_cursor FROM
								(SELECT s.id AS sid,r.owner AS rowner,r.name AS rname,r.id AS rid,tu.updated_at AS updated,tu.success AS succ, json_extract(tu.info,'$.end_cursor') as end_cursor
									FROM repositories r
									INNER JOIN sources s
									ON s.id=r.source AND s.name=:source_name
									LEFT OUTER JOIN table_updates tu
									ON tu.repo_id=r.id AND tu.table_name=:table_name
									ORDER BY r.owner,r.name,tu.updated_at ) as t1
								INNER JOIN
									(SELECT s.id AS sid,r.owner AS rowner,r.name AS rname,r.id AS rid,MAX(tu.updated_at) AS updated
									FROM repositories r
									INNER JOIN sources s
									ON s.id=r.source AND s.name=:source_name
									LEFT OUTER JOIN table_updates tu
									ON tu.repo_id=r.id AND tu.table_name=:table_name
									GROUP BY r.owner,r.name,r.id,s.id ) AS t2
							ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.rid=t2.rid
							AND ((NOT t1.success) OR t1.succ IS NULL)
							ORDER BY t1.sid,t1.rowner,t1.rname
					;''',{'source_name':self.source_name,'table_name':items_name})
				else:
					self.db.cursor.execute('''
							SELECT t1.sid,t1.rowner,t1.rname,t1.rid,t1.end_cursor FROM
								(SELECT s.id AS sid,r.owner AS rowner,r.name AS rname,r.id AS rid,tu.updated_at AS updated,tu.success AS succ, json_extract(tu.info,'$.end_cursor') as end_cursor
									FROM repositories r
									INNER JOIN sources s
									ON s.id=r.source AND s.name=:source_name
									LEFT OUTER JOIN table_updates tu
									ON tu.repo_id=r.id AND tu.table_name=:table_name
									ORDER BY r.owner,r.name,tu.updated_at ) as t1
								INNER JOIN
									(SELECT s.id AS sid,r.owner AS rowner,r.name AS rname,r.id AS rid,MAX(tu.updated_at) AS updated
									FROM repositories r
									INNER JOIN sources s
									ON s.id=r.source AND s.name=:source_name
									LEFT OUTER JOIN table_updates tu
									ON tu.repo_id=r.id AND tu.table_name=:table_name
									GROUP BY r.owner,r.name,r.id,s.id ) AS t2
							ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.rid=t2.rid
							AND t1.succ IS NULL
							ORDER BY t1.sid,t1.rowner,t1.rname
					;''',{'source_name':self.source_name,'table_name':items_name})


				self.elt_list = list(self.db.cursor.fetchall())

				# # specific to sqlite because no internal json parsing implemented in query
				# self.elt_list = []
				# for (source,owner,name,repo_id,end_cursor_info) in elt_list:
				# 	try:
				# 		self.elt_list.append((source,owner,name,repo_id,json.loads(end_cursor_info)['end_cursor']))
				# 	except:
				# 		self.elt_list.append((source,owner,name,repo_id,None))

			if self.start_offset is not None:
				self.elt_list = [r for r in self.elt_list if r[1]>=self.start_offset]



	################################################################
		elif queried_obj == 'repo' and sub_queried_obj != 'repo':
			#source,owner,repo_name,repo_id,comment_id,comment_gql_id,end_cursor_orig
			if self.db.db_type == 'postgres':
				if self.retry:
					
					self.db.cursor.execute('''
						SELECT t1.sid,t1.rowner,t1.rname,t1.rid,t1.sq_id,t1.sq_gql_id,t2.info ->> 'end_cursor' AS end_cursor FROM
							(SELECT s.id AS sid,
									r.owner AS rowner,
									r.name AS rname,
									r.id AS rid,
									tu.info ->> (%(sub_queried_obj)s||'_id') as sq_id,
									tu.info ->> (%(sub_queried_obj)s||'_gql_id') as sq_gql_id,
									--tu.info ->> 'end_cursor' as end_cursor
									MAX(tu.updated_at) AS max_updated_at
								FROM table_updates tu
								INNER JOIN sources s
								ON table_name=%(table_name)s
								AND sq_id IS NOT NULL
								AND sq_gql_id IS NOT NULL
								AND s.name=%(source_name)s
								INNER JOIN repositories r
								ON s.id=r.source AND tu.repo_id=r.id
								GROUP BY sid,rowner,rname,rid,sq_id,sq_gql_id
								HAVING SUM(CASE WHEN (tu.success IS NULL OR NOT tu.success) THEN 0 ELSE 1 END)=0
							) AS t1
						INNER JOIN table_updates t2
							ON t2.repo_id=t1.rid
							AND t2.table_name=%(table_name)s
							--AND tu.info ->>( %(sub_queried_obj)s||'_id') = t1.sq_id
							AND t2.info ->> (%(sub_queried_obj)s||'_gql_id') = t1.sq_gql_id
							AND t2.updated_at=t1.max_updated_at
					;''',{'source_name':self.source_name,'table_name':items_name,'sub_queried_obj':sub_queried_obj})
				else:
					self.db.cursor.execute('''
						SELECT t1.sid,t1.rowner,t1.rname,t1.rid,t1.sq_id,t1.sq_gql_id,t2.info ->> 'end_cursor' AS end_cursor FROM
							(SELECT s.id AS sid,
									r.owner AS rowner,
									r.name AS rname,
									r.id AS rid,
									tu.info ->> (%(sub_queried_obj)s||'_id') as sq_id,
									tu.info ->> (%(sub_queried_obj)s||'_gql_id') as sq_gql_id,
									--tu.info ->> 'end_cursor' as end_cursor
									MAX(tu.updated_at) AS max_updated_at
								FROM table_updates tu
								INNER JOIN sources s
								ON table_name=%(table_name)s
								AND tu.info ->> (%(sub_queried_obj)s||'_id') IS NOT NULL
								AND tu.info ->> (%(sub_queried_obj)s||'_gql_id') IS NOT NULL
								AND s.name=%(source_name)s
								INNER JOIN repositories r
								ON s.id=r.source AND tu.repo_id=r.id
								GROUP BY sid,rowner,rname,rid,sq_id,sq_gql_id
								HAVING SUM(CASE WHEN tu.success IS NULL THEN 0 ELSE 1 END)=0
							) AS t1
						INNER JOIN table_updates t2
							ON t2.repo_id=t1.rid
							AND t2.table_name=%(table_name)s
							--AND tu.info ->> (%(sub_queried_obj)s||'_id') = t1.sq_id
							AND t2.info ->> (%(sub_queried_obj)s||'_gql_id') = t1.sq_gql_id
							AND t2.updated_at=t1.max_updated_at
					;''',{'source_name':self.source_name,'table_name':items_name,'sub_queried_obj':sub_queried_obj})

				self.elt_list = list(self.db.cursor.fetchall())

			else:
				if self.retry:
					self.db.cursor.execute('''
						SELECT t1.sid,t1.rowner,t1.rname,t1.rid,t1.sq_id,t1.sq_gql_id,json_extract(t2.info, '$.end_cursor') AS end_cursor FROM
							(SELECT s.id AS sid,
									r.owner AS rowner,
									r.name AS rname,
									r.id AS rid,
									json_extract(tu.info, '$.' || :sub_queried_obj || '_id') as sq_id,
									json_extract(tu.info, '$.' || :sub_queried_obj || '_gql_id') as sq_gql_id,
									--tu.info ->> 'end_cursor' as end_cursor
									MAX(tu.updated_at) AS max_updated_at
								FROM table_updates tu
								INNER JOIN sources s
								ON table_name=:table_name
								AND json_extract(tu.info, '$.' || :sub_queried_obj || '_id') IS NOT NULL
								AND json_extract(tu.info, '$.' || :sub_queried_obj || '_gql_id') IS NOT NULL
								AND s.name=:source_name
								INNER JOIN repositories r
								ON s.id=r.source AND tu.repo_id=r.id
								GROUP BY sid,rowner,rname,rid,sq_id,sq_gql_id
								HAVING SUM(CASE WHEN (tu.success IS NULL OR NOT tu.success) THEN 0 ELSE 1 END)=0
							) AS t1
						INNER JOIN table_updates t2
							ON t2.repo_id=t1.rid
							AND t2.table_name=:table_name
							--AND tu.info ->> :sub_queried_obj||'_id' = t1.sq_id
							AND json_extract(t2.info, '$.' || :sub_queried_obj || '_gql_id') = t1.sq_gql_id
							AND t2.updated_at=t1.max_updated_at
					;''',{'source_name':self.source_name,'table_name':items_name,'sub_queried_obj':sub_queried_obj})
				else:
					self.db.cursor.execute('''
						SELECT t1.sid,t1.rowner,t1.rname,t1.rid,t1.sq_id,t1.sq_gql_id,json_extract(t2.info, '$.end_cursor') AS end_cursor FROM
							(SELECT s.id AS sid,
									r.owner AS rowner,
									r.name AS rname,
									r.id AS rid,
									json_extract(tu.info, '$.' || :sub_queried_obj || '_id') as sq_id,
									json_extract(tu.info, '$.' || :sub_queried_obj || '_gql_id') as sq_gql_id,
									--tu.info ->> 'end_cursor' as end_cursor
									MAX(tu.updated_at) AS max_updated_at
								FROM table_updates tu
								INNER JOIN sources s
								ON table_name=:table_name
								AND json_extract(tu.info, '$.' || :sub_queried_obj || '_id') IS NOT NULL
								AND json_extract(tu.info, '$.' || :sub_queried_obj || '_gql_id') IS NOT NULL
								AND s.name=:source_name
								INNER JOIN repositories r
								ON s.id=r.source AND tu.repo_id=r.id
								GROUP BY sid,rowner,rname,rid,sq_id,sq_gql_id
								HAVING SUM(CASE WHEN tu.success IS NULL THEN 0 ELSE 1 END)=0
							) AS t1
						INNER JOIN table_updates t2
							ON t2.repo_id=t1.rid
							AND t2.table_name=:table_name
							--AND tu.info ->> :sub_queried_obj||'_id' = t1.sq_id
							AND json_extract(t2.info, '$.' || :sub_queried_obj || '_gql_id') = t1.sq_gql_id
							AND t2.updated_at=t1.max_updated_at
					;''',{'source_name':self.source_name,'table_name':items_name,'sub_queried_obj':sub_queried_obj})


				self.elt_list = list(self.db.cursor.fetchall())

				# # specific to sqlite because no internal json parsing implemented in query
				# self.elt_list = []
				# for (source,owner,name,repo_id,end_cursor_info) in elt_list:
				# 	try:
				# 		self.elt_list.append((source,owner,name,repo_id,json.loads(end_cursor_info)['end_cursor']))
				# 	except:
				# 		self.elt_list.append((source,owner,name,repo_id,None))

			if self.start_offset is not None:
				self.elt_list = [r for r in self.elt_list if r[1]>=self.start_offset]



	################################################################
		elif sub_queried_obj == 'user':

			if self.db.db_type == 'postgres':
				if self.force:
					self.db.cursor.execute('''
							SELECT t1.itid,t1.identity,t1.iid,t1.end_cursor FROM
								(SELECT it.id AS itid,i.identity as identity,i.id AS iid,tu.updated_at AS updated,tu.success AS succ, tu.info ->> 'end_cursor' as end_cursor
									FROM identities i
									INNER JOIN identity_types it
									ON it.id=i.identity_type_id AND it.name=%(target_identity_type)s
									LEFT OUTER JOIN table_updates tu
									ON tu.identity_id=i.id AND tu.table_name=%(table_name)s
									ORDER BY i.identity,tu.updated_at ) as t1
								INNER JOIN
									(SELECT it.id AS itid,i.identity as identity,i.id AS iid,MAX(tu.updated_at) AS updated
									FROM identities i
									INNER JOIN identity_types it
									ON it.id=i.identity_type_id AND it.name=%(target_identity_type)s
									LEFT OUTER JOIN table_updates tu
									ON tu.identity_id=i.id AND tu.table_name=%(table_name)s
									GROUP BY i.identity,i.id,it.id ) AS t2
							ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.iid=t2.iid
							ORDER BY t1.itid,t1.identity
					;''',{'target_identity_type':self.target_identity_type,'table_name':items_name})
				elif self.retry:
					self.db.cursor.execute('''
							SELECT t1.itid,t1.identity,t1.iid,t1.end_cursor FROM
								(SELECT it.id AS itid,i.identity as identity,i.id AS iid,tu.updated_at AS updated,tu.success AS succ, tu.info ->> 'end_cursor' as end_cursor
									FROM identities i
									INNER JOIN identity_types it
									ON it.id=i.identity_type_id AND it.name=%(target_identity_type)s
									LEFT OUTER JOIN table_updates tu
									ON tu.identity_id=i.id AND tu.table_name=%(table_name)s
									ORDER BY i.identity,tu.updated_at ) as t1
								INNER JOIN
									(SELECT it.id AS itid,i.identity as identity,i.id AS iid,MAX(tu.updated_at) AS updated
									FROM identities i
									INNER JOIN identity_types it
									ON it.id=i.identity_type_id AND it.name=%(target_identity_type)s
									LEFT OUTER JOIN table_updates tu
									ON tu.identity_id=i.id AND tu.table_name=%(table_name)s
									GROUP BY i.identity,i.id,it.id ) AS t2
							ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.iid=t2.iid
							AND ((NOT t1.success) OR t1.succ IS NULL)
							ORDER BY t1.itid,t1.identity
					;''',{'target_identity_type':self.target_identity_type,'table_name':items_name})
				else:
					self.db.cursor.execute('''
							SELECT t1.itid,t1.identity,t1.iid,t1.end_cursor FROM
								(SELECT it.id AS itid,i.identity as identity,i.id AS iid,tu.updated_at AS updated,tu.success AS succ, tu.info ->> 'end_cursor' as end_cursor
									FROM identities i
									INNER JOIN identity_types it
									ON it.id=i.identity_type_id AND it.name=%(target_identity_type)s
									LEFT OUTER JOIN table_updates tu
									ON tu.identity_id=i.id AND tu.table_name=%(table_name)s
									ORDER BY i.identity,tu.updated_at ) as t1
								INNER JOIN
									(SELECT it.id AS itid,i.identity as identity,i.id AS iid,MAX(tu.updated_at) AS updated
									FROM identities i
									INNER JOIN identity_types it
									ON it.id=i.identity_type_id AND it.name=%(target_identity_type)s
									LEFT OUTER JOIN table_updates tu
									ON tu.identity_id=i.id AND tu.table_name=%(table_name)s
									GROUP BY i.identity,i.id,it.id ) AS t2
							ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.iid=t2.iid
							AND t1.succ IS NULL
							ORDER BY t1.itid,t1.identity
					;''',{'target_identity_type':self.target_identity_type,'table_name':items_name})

				self.elt_list = list(self.db.cursor.fetchall())

			else:
				if self.force:
					self.db.cursor.execute('''
							SELECT t1.itid,t1.identity,t1.iid,t1.end_cursor FROM
								(SELECT it.id AS itid,i.identity as identity,i.id AS iid,tu.updated_at AS updated,tu.success AS succ, json_extract(tu.info,'$.end_cursor') as end_cursor
									FROM identities i
									INNER JOIN identity_types it
									ON it.id=i.identity_type_id AND it.name=:target_identity_type
									LEFT OUTER JOIN table_updates tu
									ON tu.identity_id=i.id AND tu.table_name=:table_name
									ORDER BY i.identity,tu.updated_at ) as t1
								INNER JOIN
									(SELECT it.id AS itid,i.identity as identity,i.id AS iid,MAX(tu.updated_at) AS updated
									FROM identities i
									INNER JOIN identity_types it
									ON it.id=i.identity_type_id AND it.name=:target_identity_type
									LEFT OUTER JOIN table_updates tu
									ON tu.identity_id=i.id AND tu.table_name=:table_name
									GROUP BY i.identity,i.id,it.id ) AS t2
							ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.iid=t2.iid
							ORDER BY t1.itid,t1.identity
					;''',{'target_identity_type':self.target_identity_type,'table_name':items_name})
				elif self.retry:
					self.db.cursor.execute('''
							SELECT t1.itid,t1.identity,t1.iid,t1.end_cursor FROM
								(SELECT it.id AS itid,i.identity as identity,i.id AS iid,tu.updated_at AS updated,tu.success AS succ, json_extract(tu.info,'$.end_cursor') as end_cursor
									FROM identities i
									INNER JOIN identity_types it
									ON it.id=i.identity_type_id AND it.name=:target_identity_type
									LEFT OUTER JOIN table_updates tu
									ON tu.identity_id=i.id AND tu.table_name=:table_name
									ORDER BY i.identity,tu.updated_at ) as t1
								INNER JOIN
									(SELECT it.id AS itid,i.identity as identity,i.id AS iid,MAX(tu.updated_at) AS updated
									FROM identities i
									INNER JOIN identity_types it
									ON it.id=i.identity_type_id AND it.name=:target_identity_type
									LEFT OUTER JOIN table_updates tu
									ON tu.identity_id=i.id AND tu.table_name=:table_name
									GROUP BY i.identity,i.id,it.id ) AS t2
							ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.iid=t2.iid
							AND ((NOT t1.success) OR t1.succ IS NULL)
							ORDER BY t1.itid,t1.identity
					;''',{'target_identity_type':self.target_identity_type,'table_name':items_name})
				else:
					self.db.cursor.execute('''
							SELECT t1.itid,t1.identity,t1.iid,t1.end_cursor FROM
								(SELECT it.id AS itid,i.identity as identity,i.id AS iid,tu.updated_at AS updated,tu.success AS succ, json_extract(tu.info,'$.end_cursor') as end_cursor
									FROM identities i
									INNER JOIN identity_types it
									ON it.id=i.identity_type_id AND it.name=:target_identity_type
									LEFT OUTER JOIN table_updates tu
									ON tu.identity_id=i.id AND tu.table_name=:table_name
									ORDER BY i.identity,tu.updated_at ) as t1
								INNER JOIN
									(SELECT it.id AS itid,i.identity as identity,i.id AS iid,MAX(tu.updated_at) AS updated
									FROM identities i
									INNER JOIN identity_types it
									ON it.id=i.identity_type_id AND it.name=:target_identity_type
									LEFT OUTER JOIN table_updates tu
									ON tu.identity_id=i.id AND tu.table_name=:table_name
									GROUP BY i.identity,i.id,it.id ) AS t2
							ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.iid=t2.iid
							AND t1.succ IS NULL
							ORDER BY t1.itid,t1.identity
					;''',{'target_identity_type':self.target_identity_type,'table_name':items_name})


				self.elt_list = list(self.db.cursor.fetchall())

				# # specific to sqlite because no internal json parsing implemented in query
				# self.elt_list = []
				# for (identity_type_id,login,identity_id,end_cursor_info) in elt_list:
				# 	try:
				# 		self.elt_list.append((identity_type_id,login,identity_id,json.loads(end_cursor_info)['end_cursor']))
				# 	except:
				# 		self.elt_list.append((identity_type_id,login,identity_id,None))

			if self.start_offset is not None:
				self.elt_list = [r for r in self.elt_list if r[1]>=self.start_offset]


		else:
			raise NotImplementedError(f'set_element_list not implemented for queried_obj {queried_obj}')


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
						stargazers (first:{page_size}, orderBy: {{ field: STARRED_AT, direction: ASC }} {after_end_cursor} ){{
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
						(SELECT id FROM identity_types WHERE name=%s),
						(SELECT id FROM identities WHERE identity=%s AND identity_type_id=(SELECT id FROM identity_types WHERE name=%s))
					)
				ON CONFLICT DO NOTHING
				;''',((s['starred_at'],s['starrer_login'],s['repo_id'],self.target_identity_type,s['starrer_login'],self.target_identity_type) for s in items_list))
		else:
			db.cursor.executemany('''
					INSERT OR IGNORE INTO stars(starred_at,login,repo_id,identity_type_id,identity_id)
					VALUES(?,
							?,
							?,
							(SELECT id FROM identity_types WHERE name=?),
							(SELECT id FROM identities WHERE identity=? AND identity_type_id=(SELECT id FROM identity_types WHERE name=?))
						);''',((s['starred_at'],s['starrer_login'],s['repo_id'],self.target_identity_type,s['starrer_login'],self.target_identity_type) for s in items_list))

		if commit:
			db.connection.commit()

	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['repository']['stargazers']['totalCount']

class WatchersGQLFiller(GHGQLFiller):
	'''
	Querying watchers through the GraphQL API
	'''
	def __init__(self,**kwargs):
		self.items_name = 'watchers'
		self.queried_obj = 'repo'
		self.pageinfo_path = ['repository','watchers','pageInfo']
		GHGQLFiller.__init__(self,**kwargs)

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
					repository(owner:"{repo_owner}", name:"{repo_name}") {{
						nameWithOwner
						watchers (first:{page_size} {after_end_cursor} ){{
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

	def parse_query_result(self,query_result,repo_id,identity_id,repo_owner=None,repo_name=None,**kwargs):
		'''
		In subclasses this has to be implemented
		output: [ {'repo_id':r_id,'repo_owner':r_ow,'repo_name':r_na,'starrer_login':s_lo,'starred_at':st_at} , ...]
		'''
		ans = []
		if repo_owner is None:
			repo_owner,repo_name = query_result['repository']['nameWithOwner'].split('/')
		for e in query_result['repository']['watchers']['edges']:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'identity_type':self.target_identity_type}
			try:
				d['watcher_login'] = e['node']['login']
			except (KeyError,TypeError) as err:
				self.logger.info('Result triggering error: {} \nError when parsing watchers for {}/{}: {}'.format(e,repo_owner,repo_name,err))
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
				INSERT INTO watchers(login,repo_id,identity_type_id,identity_id)
				VALUES(%(watcher_login)s,
						%(repo_id)s,
						(SELECT id FROM identity_types WHERE name=%(identity_type)s),
						(SELECT id FROM identities WHERE identity=%(watcher_login)s AND identity_type_id=(SELECT id FROM identity_types WHERE name=%(identity_type)s))
					)
				ON CONFLICT DO NOTHING
				;''',items_list)
		else:
			db.cursor.executemany('''
					INSERT OR IGNORE INTO stars(login,repo_id,identity_type_id,identity_id)
					VALUES(:watcher_login,
							:repo_id,
							(SELECT id FROM identity_types WHERE name=:identity_type),
							(SELECT id FROM identities WHERE identity=:watcher_login AND identity_type_id=(SELECT id FROM identity_types WHERE name=:identity_type))
						);''',items_list)
		if commit:
			db.connection.commit()

	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['repository']['watchers']['totalCount']

class ForksGQLFiller(GHGQLFiller):
	'''
	Querying forks through the GraphQL API
	'''
	def __init__(self,**kwargs):
		self.items_name = 'forks'
		self.queried_obj = 'repo'
		self.pageinfo_path = ['repository','forks','pageInfo']
		GHGQLFiller.__init__(self,**kwargs)

	def apply(self):
		GHGQLFiller.apply(self)
		self.fill_fork_ranks()
		self.db.connection.commit()
		self.db.batch_merge_repos()

	def fill_fork_ranks(self,step=1):
		github_rest.ForksFiller.fill_fork_ranks(self,step=step)

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
					repository(owner:"{repo_owner}", name:"{repo_name}") {{
						nameWithOwner
						forks (first:{page_size}, orderBy: {{ field: CREATED_AT, direction: ASC }} {after_end_cursor} ){{
						 totalCount
						 pageInfo {{
							endCursor
							hasNextPage
						 }}
						 nodes {{
						 		createdAt
								nameWithOwner
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
		for e in query_result['repository']['forks']['nodes']:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'source':self.source_name}
			try:
				d['forked_at'] = e['createdAt']
				d['fork_fullname'] = e['nameWithOwner']
			except (KeyError,TypeError) as err:
				self.logger.info('Result triggering error: {} \nError when parsing forks for {}/{}: {}'.format(e,repo_owner,repo_name,err))
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
										INSERT INTO forks(forking_repo_id,forked_repo_id,forking_repo_url,forked_at)
										VALUES((SELECT r.id FROM repositories r
													INNER JOIN sources s
													ON s.name=%(source)s AND s.id=r.source AND CONCAT(r.owner,'/',r.name)=%(fork_fullname)s)
												,%(repo_id)s
												,(SELECT CONCAT(s.url_root,'/',%(fork_fullname)s) FROM sources s
													WHERE s.name=%(source)s)
												,%(forked_at)s)
										ON CONFLICT DO NOTHING
										;''',({'source':s['source'],'fork_fullname':s['fork_fullname'],'repo_id':s['repo_id'],'forked_at':s['forked_at']} for s in items_list))
		else:
			db.cursor.executemany('''
										INSERT OR IGNORE INTO forks(forking_repo_id,forked_repo_id,forking_repo_url,forked_at)
										VALUES((SELECT r.id FROM repositories r
													INNER JOIN sources s
													ON s.name=:source AND s.id=r.source AND r.owner || '/' || r.name=:fork_fullname)
												,:repo_id
												,(SELECT s.url_root || '/' || :fork_fullname FROM sources s
													WHERE s.name=:source)
												,:forked_at)
										;''',({'source':s['source'],'fork_fullname':s['fork_fullname'],'repo_id':s['repo_id'],'forked_at':s['forked_at']} for s in items_list))

		if commit:
			db.connection.commit()



	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['repository']['forks']['totalCount']


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
						sponsorsListing {{
      						createdAt
    							}}
						sponsorshipsAsMaintainer (includePrivate:true, first:{page_size}{after_end_cursor} ){{
							totalCount
							pageInfo {{
								endCursor
								hasNextPage
						 		}}
						 	nodes {{
								createdAt
								privacyLevel
								isOneTimePayment
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
			if query_result['user']['sponsorsListing'] is None:
				self.logger.info('{} is not sponsorable'.format(user_login))
			else:
				sl_ca = query_result['user']['sponsorsListing']['createdAt']
				for e in query_result['user']['sponsorshipsAsMaintainer']['nodes']:
					d = {'sponsored_id':identity_id,
						'sponsored_login':user_login,
						'sponsorsListing_createdat':sl_ca,
						'identity_type_id':identity_type_id}
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
						d['is_otp'] = bool(e['isOneTimePayment'])

					except KeyError as err:
						self.logger.info('KeyError when parsing sponsors_user for {}: {}'.format(user_login,err))
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
				INSERT INTO sponsors_user(sponsored_id,sponsor_identity_type_id,sponsor_id,sponsor_login,created_at,external_id,tier,is_onetime_payment)
				VALUES(%s,
						%s,
						(SELECT id FROM identities WHERE identity=%s AND identity_type_id=%s),
						%s,
						%s,
						%s,
						%s,
						%s
					)
				ON CONFLICT DO NOTHING
				;''',((f['sponsored_id'],f['identity_type_id'],f['sponsor_login'],f['identity_type_id'],f['sponsor_login'],f['created_at'],f['external_id'],f['tier'],f['is_otp']) for f in items_list))

			extras.execute_batch(db.cursor,''' INSERT INTO sponsors_listings(identity_type_id,login,created_at)
													VALUES(%(identity_type_id)s,
															%(sponsored_login)s,
															%(sponsorsListing_createdat)s)
													ON CONFLICT DO NOTHING;''',items_list)
		else:
			db.cursor.executemany('''
				INSERT OR IGNORE INTO sponsors_user(sponsored_id,sponsor_identity_type_id,sponsor_id,sponsor_login,created_at,external_id,tier,is_onetime_payment)
				VALUES(?,
						?,
						(SELECT id FROM identities WHERE identity=? AND identity_type_id=?),
						?,
						?,
						?,
						?,
						?
					)
				;''',((f['sponsored_id'],f['identity_type_id'],f['sponsor_login'],f['identity_type_id'],f['sponsor_login'],f['created_at'],f['external_id'],f['tier'],f['is_otp']) for f in items_list))

			db.cursor.executemany(''' INSERT OR IGNORE INTO sponsors_listings(identity_type_id,login,created_at)
													VALUES(:identity_type_id,
															:sponsored_login,
															:sponsorsListing_createdat)
													;''',items_list)

		if commit:
			db.connection.commit()

	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['user']['sponsorshipsAsMaintainer']['totalCount']

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
						followers (first:{page_size} {after_end_cursor} ){{
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
			except KeyError as err:
				self.logger.info('KeyError when parsing followers for {}: {}'.format(user_login,err))
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


class BackwardsSponsorsUserFiller(SponsorsUserFiller):
	'''
	Querying sponsored users by users in the DB through the GraphQL API
	!!! This fills in the users and identities table, not the sponsors_user table. The SponsorsUserFiller has to be called afterwards
	'''
	def __init__(self,**kwargs):
		self.items_name = 'sponsors_user_backwards'
		self.queried_obj = 'user'
		self.pageinfo_path = ['user','sponsorshipsAsSponsor','pageInfo']
		GHGQLFiller.__init__(self,**kwargs)

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
					user(login:"{user_login}") {{
						login
						sponsorshipsAsSponsor (first:{page_size}{after_end_cursor} ){{
							totalCount
							pageInfo {{
								endCursor
								hasNextPage
						 		}}
						 	nodes {{
								sponsorable {{
									sponsorsListing {{
									name
									}}
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
			for e in query_result['user']['sponsorshipsAsSponsor']['nodes']:
				d = {'sponsor_id':identity_id,'sponsor_login':user_login,'identity_type_id':identity_type_id}
				try:
					sponsor_listing_name = e['sponsorable']['sponsorsListing']['name']
					assert sponsor_listing_name.startswith('sponsors-')
					d['sponsored_login'] = sponsor_listing_name[9:]
				except KeyError as err:
					self.logger.info('KeyError when parsing sponsors_user for {}: {}'.format(user_login,err))
					continue
				else:
					ans.append(d)
		return ans


	def insert_items(self,items_list,commit=True,db=None):
		'''
		In subclasses this has to be implemented
		inserts results in the DB
		Using commits at each statement to avoid batch rollback when race condition on just one login
		'''
		if db is None:
			db = self.db
		if db.db_type == 'postgres':
			extras.execute_batch(db.cursor,'''
				INSERT INTO users(
						creation_identity,
						creation_identity_type_id)
					SELECT %s,%s
					WHERE NOT EXISTS (SELECT 1 FROM identities
										WHERE identity_type_id=%s
										AND identity=%s)
				ON CONFLICT DO NOTHING
				;
				COMMIT;
				''',((f['sponsored_login'],f['identity_type_id'],f['identity_type_id'],f['sponsored_login'],) for f in items_list))
			extras.execute_batch(db.cursor,'''
				INSERT INTO identities(
						identity_type_id,
						identity,
						user_id)
					VALUES(%s,
							%s,
							(SELECT id FROM users u WHERE u.creation_identity=%s AND u.creation_identity_type_id=%s))
					ON CONFLICT DO NOTHING
				;
				COMMIT;
				''',((f['identity_type_id'],f['sponsored_login'],f['sponsored_login'],f['identity_type_id'],) for f in items_list))
		else:
			for f in items_list:

				db.cursor.execute('''
					INSERT INTO users(
							creation_identity,
							creation_identity_type_id)
						SELECT ?,?
						WHERE NOT EXISTS (SELECT 1 FROM identities
											WHERE identity_type_id=?
											AND identity=?)
					ON CONFLICT DO NOTHING
					;
					''',(f['sponsored_login'],f['identity_type_id'],f['identity_type_id'],f['sponsored_login'],))
				db.connection.commit()
				db.cursor.execute('''
					INSERT INTO identities(
							identity_type_id,
							identity,
							user_id)
						VALUES(?,
								?,
								(SELECT id FROM users u WHERE u.creation_identity=? AND u.creation_identity_type_id=?))
						ON CONFLICT DO NOTHING
					;
					''',(f['identity_type_id'],f['sponsored_login'],f['sponsored_login'],f['identity_type_id'],))
				db.connection.commit()
		if commit:
			db.connection.commit()


	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['user']['sponsorshipsAsSponsor']['totalCount']



class ReleasesGQLFiller(GHGQLFiller):
	'''
	Querying releases through the GraphQL API
	'''
	def __init__(self,**kwargs):
		self.items_name = 'releases'
		self.queried_obj = 'repo'
		self.pageinfo_path = ['repository','releases','pageInfo']
		GHGQLFiller.__init__(self,**kwargs)

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
  					repository(owner: "{repo_owner}", name: "{repo_name}") {{
  							nameWithOwner
    						id
    						releases(first:{page_size} {after_end_cursor}) {{
    							totalCount
    							pageInfo {{
									endCursor
									hasNextPage
						 			}}
      							nodes {{
        							createdAt
        							tagName
        							name
      								}}
    							}}
  							}}
						}}'''

	def parse_query_result(self,query_result,repo_id,identity_id,repo_owner=None,repo_name=None,**kwargs):
		'''
		In subclasses this has to be implemented
		output: [ {'repo_id':r_id,'repo_owner':r_ow,'repo_name':r_na,'release_name':rel_na,'tag_name':tag_na,'released_at':rel_date} , ...]
		'''
		ans = []
		if repo_owner is None:
			repo_owner,repo_name = query_result['repository']['nameWithOwner'].split('/')
		for e in query_result['repository']['releases']['nodes']:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name}
			try:
				d['released_at'] = e['createdAt']
				d['tag_name'] = e['tagName']
				d['release_name'] = e['name']
			except (KeyError,TypeError) as err:
				self.logger.info('Result triggering error: {} \nError when parsing releases for {}/{}: {}'.format(e,repo_owner,repo_name,err))
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
				INSERT INTO releases(created_at,name,repo_id,tag_name)
				VALUES(%s,
						%s,
						%s,
						%s)

				ON CONFLICT DO NOTHING
				;''',((s['released_at'],s['release_name'],s['repo_id'],s['tag_name']) for s in items_list))
		else:
			db.cursor.executemany('''
					INSERT OR IGNORE INTO releases(created_at,name,repo_id,tag_name)
					VALUES(?,
							?,
							?,
							?)
				;''',((s['released_at'],s['release_name'],s['repo_id'],s['tag_name']) for s in items_list))


		if commit:
			db.connection.commit()

	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['repository']['releases']['totalCount']

class LoginsGQLFiller(GHGQLFiller):
	'''
	Querying logins through the GraphQL API using commits
	'''
	def __init__(self,target_identity_type='github_login',source_name='GitHub',**kwargs):
		self.items_name = 'login'
		self.queried_obj = 'email'
		self.pageinfo_path = None
		self.target_identity_type = target_identity_type
		GHGQLFiller.__init__(self,source_name=source_name,**kwargs)

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
  					repository(owner: "{repo_owner}", name: "{repo_name}") {{
  							nameWithOwner
    						id
    						object(oid:"{commit_sha}"){{
    							... on Commit{{

    								oid
    								author {{
    									user {{
    									login
    									createdAt
    										}}
    								}}
    							}}
    							}}
  							}}
						}}'''

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
		elif query_result['repository']['object'] is None:
			ans['login'] = None
			ans['repo_owner'] = query_result['repository']['nameWithOwner'].split('/')[1]
			ans['repo_name'] = query_result['repository']['nameWithOwner'].split('/')[0]
			ans['created_at'] = None
			ans['commit_sha'] = None
		else:
			ans['repo_owner'] = query_result['repository']['nameWithOwner'].split('/')[1]
			ans['repo_name'] = query_result['repository']['nameWithOwner'].split('/')[0]
			if query_result['repository']['object']['author'] is None:
				ans['login'] = None
				ans['created_at'] = None
			elif query_result['repository']['object']['author']['user'] is None:
				ans['login'] = None
				ans['created_at'] = None
			else:
				ans['login'] = query_result['repository']['object']['author']['user']['login']
				ans['created_at'] = query_result['repository']['object']['author']['user']['createdAt']
			ans['commit_sha'] = query_result['repository']['object']['oid']

		return [ans]


	def insert_items(self,items_list,commit=True,db=None):
		'''
		In subclasses this has to be implemented
		inserts results in the DB
		'''
		if db is None:
			db = self.db
		for item in items_list:
			login = item['login']
			reason = 'Email/login match through {} for commit {}'.format(self.__class__.__name__,item['commit_sha'])
			identity_id = item['email_id']
			if db.db_type == 'postgres':
				if login is not None:
					db.cursor.execute(''' INSERT INTO users(creation_identity_type_id,creation_identity) VALUES(
												(SELECT id FROM identity_types WHERE name=%s),
												%s
												) ON CONFLICT DO NOTHING;''',(self.target_identity_type,login,))

					db.cursor.execute(''' INSERT INTO identities(identity_type_id,user_id,identity)
													VALUES((SELECT id FROM identity_types WHERE name=%s),
															(SELECT id FROM users
															WHERE creation_identity_type_id=(SELECT id FROM identity_types WHERE name=%s)
																AND creation_identity=%s),
															%s)
													ON CONFLICT DO NOTHING;''',(self.target_identity_type,self.target_identity_type,login,login,))

					db.cursor.execute('''SELECT id FROM identities
												WHERE identity_type_id=(SELECT id FROM identity_types WHERE name=%s)
												AND identity=%s;''',(self.target_identity_type,login,))
					identity2 = db.cursor.fetchone()[0]
					db.merge_identities(identity1=identity2,identity2=identity_id,autocommit=False,reason=reason)
				db.cursor.execute('''INSERT INTO table_updates(identity_id,table_name,success) VALUES(%s,'login',%s);''',(identity_id,(login is not None)))
			else:
				if login is not None:


					db.cursor.execute(''' INSERT OR IGNORE INTO users(creation_identity_type_id,creation_identity) VALUES(
												(SELECT id FROM identity_types WHERE name=?),
												?
												);''',(self.target_identity_type,login,))

					db.cursor.execute(''' INSERT OR IGNORE INTO identities(identity_type_id,user_id,identity)
													VALUES((SELECT id FROM identity_types WHERE name=?),
															(SELECT id FROM users
															WHERE creation_identity_type_id=(SELECT id FROM identity_types WHERE name=?)
																AND creation_identity=?),
															?);''',(self.target_identity_type,self.target_identity_type,login,login,))

					db.cursor.execute('''SELECT id FROM identities
												WHERE identity_type_id=(SELECT id FROM identity_types WHERE name=?)
												AND identity=?;''',(self.target_identity_type,login,))
					identity2 = db.cursor.fetchone()[0]


					db.merge_identities(identity1=identity2,identity2=identity_id,autocommit=False,reason=reason)

				db.cursor.execute('''INSERT INTO table_updates(identity_id,table_name,success) VALUES(?,'login',?);''',(identity_id,(login is not None)))
		if commit:
			db.connection.commit()


	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return 1

	def set_element_list(self):
		'''
		In subclasses this has to be implemented
		sets self.elt_list to be used in self.fill_items
		source,owner,repo_name,repo_id,commit_sha,email,identity_id,identity_type_id
		'''

		#if force: all elts
		#if retry: all without success false or null on last update
		#else: all with success is null on lats update


		if self.force:
			if self.db.db_type == 'postgres':
				self.db.cursor.execute('''
					SELECT s.id,r.owner,r.name,c.repo_id,c.sha,i.identity,i.id,i.identity_type_id
					FROM (	(SELECT i1.id,i1.identity,i1.identity_type_id FROM identities i1)
								EXCEPT
							(SELECT i2.id,i2.identity,i2.identity_type_id FROM identities i2
								INNER JOIN identities i3
								ON i3.user_id = i2.user_id
								INNER JOIN identity_types it2
								ON it2.id=i3.identity_type_id AND it2.name=%s)
							) AS i
				 	JOIN LATERAL (SELECT cc.sha,cc.repo_id FROM commits cc
				 		WHERE cc.author_id=i.id ORDER BY cc.created_at DESC LIMIT 1) AS c
				 	ON true
				 	INNER JOIN repositories r
				 	ON c.repo_id=r.id
				 	INNER JOIN sources s
					ON s.id=r.source AND s.name=%s
					ORDER BY i.identity
					;''',(self.target_identity_type,self.source_name))
			else:
				self.db.cursor.execute('''
					SELECT s.id,r.owner,r.name,c.repo_id,c.sha,i.identity,i.id,i.identity_type_id
					FROM (	SELECT i1.id,i1.identity,i1.identity_type_id FROM identities i1
								EXCEPT
							SELECT i2.id,i2.identity,i2.identity_type_id FROM identities i2
								INNER JOIN identities i3
								ON i3.user_id = i2.user_id
								INNER JOIN identity_types it2
								ON it2.id=i3.identity_type_id AND it2.name=?
							) AS i
				 	JOIN commits c
				 	ON c.id IN (SELECT cc.id FROM commits cc
				 		WHERE cc.author_id=i.id ORDER BY cc.created_at DESC LIMIT 1)
				 	INNER JOIN repositories r
				 	ON c.repo_id=r.id
				 	INNER JOIN sources s
					ON s.id=r.source AND s.name=?
					ORDER BY i.identity
					;''',(self.target_identity_type,self.source_name))
		else:
			if self.db.db_type == 'postgres':
				self.db.cursor.execute('''
					SELECT s.id,r.owner,r.name,c.repo_id,c.sha,i.identity,i.id,i.identity_type_id
					FROM (
						SELECT ii.id,ii.identity,ii.identity_type_id FROM
					 		(SELECT iii.id,iii.identity,iii.identity_type_id FROM identities iii
							WHERE (SELECT iiii.id FROM identities iiii
								INNER JOIN identity_types iiiit
								ON iiii.user_id=iii.user_id AND iiiit.id=iiii.identity_type_id AND iiiit.name=%s) IS NULL) AS ii
							LEFT JOIN table_updates tu
							ON tu.identity_id=ii.id AND tu.table_name='login'
							GROUP BY ii.id,ii.identity,ii.identity_type_id,tu.identity_id
							HAVING tu.identity_id IS NULL
						) AS i
					JOIN LATERAL (SELECT cc.sha,cc.repo_id FROM commits cc
						WHERE cc.author_id=i.id ORDER BY cc.created_at DESC LIMIT 1) AS c
					ON true
					INNER JOIN repositories r
					ON r.id=c.repo_id
				 	INNER JOIN sources s
					ON s.id=r.source AND s.name=%s
					ORDER BY i.id
					;''',(self.target_identity_type,self.source_name))
			else:
				self.db.cursor.execute('''
					SELECT s.id,r.owner,r.name,c.repo_id,c.sha,i.identity,i.id,i.identity_type_id
					FROM (
						SELECT ii.id,ii.identity,ii.identity_type_id FROM
					 		(SELECT iii.id,iii.identity,iii.identity_type_id FROM identities iii
							WHERE (SELECT iiii.id FROM identities iiii
								INNER JOIN identity_types iiiit
								ON iiii.user_id=iii.user_id AND iiiit.id=iiii.identity_type_id AND iiiit.name=?) IS NULL) AS ii
							LEFT JOIN table_updates tu
							ON tu.identity_id=ii.id AND tu.table_name='login'
							GROUP BY ii.id,ii.identity,ii.identity_type_id,tu.identity_id
							HAVING tu.identity_id IS NULL
						) AS i
					JOIN commits c
						ON
						c.id IN (SELECT cc.id FROM commits cc
							WHERE cc.author_id=i.id ORDER BY cc.created_at DESC LIMIT 1)
					INNER JOIN repositories r
					ON r.id=c.repo_id
					INNER JOIN sources s
					ON s.id=r.source AND s.name=?
					ORDER BY i.id
					;''',(self.target_identity_type,self.source_name))

		self.elt_list = list(self.db.cursor.fetchall())
		self.logger.info(self.force)

class RandomCommitLoginsGQLFiller(LoginsGQLFiller):

	def set_element_list(self):
		'''
		In subclasses this has to be implemented
		sets self.elt_list to be used in self.fill_items
		source,owner,repo_name,repo_id,commit_sha,email,identity_id,identity_type_id
		'''

		#if force: all elts
		#if retry: all without success false or null on last update
		#else: all with success is null on lats update


		if self.force:
			if self.db.db_type == 'postgres':
				self.db.cursor.execute('''
					SELECT s.id,r.owner,r.name,c.repo_id,c.sha,i.identity,i.id,i.identity_type_id
					FROM (	(SELECT i1.id,i1.identity,i1.identity_type_id FROM identities i1)
								EXCEPT
							(SELECT i2.id,i2.identity,i2.identity_type_id FROM identities i2
								INNER JOIN identities i3
								ON i3.user_id = i2.user_id
								INNER JOIN identity_types it2
								ON it2.id=i3.identity_type_id AND it2.name=%(id_type)s)
							) AS i
				 	JOIN LATERAL (SELECT cc.sha,cc.repo_id FROM commits cc
									INNER JOIN repositories r2
									ON cc.author_id=i.id
									AND r2.id=cc.repo_id
									INNER JOIN sources s2
									ON s2.id=r2.source AND s2.name=%(s_name)s
							ORDER BY RANDOM() LIMIT 1) AS c
				 	ON true
				 	INNER JOIN repositories r
				 	ON c.repo_id=r.id
				 	INNER JOIN sources s
					ON s.id=r.source AND s.name=%(s_name)s
					ORDER BY i.identity
					;''',{'id_type':self.target_identity_type,'s_name':self.source_name})
			else:
				self.db.cursor.execute('''
					SELECT s.id,r.owner,r.name,c.repo_id,c.sha,i.identity,i.id,i.identity_type_id
					FROM (	SELECT i1.id,i1.identity,i1.identity_type_id FROM identities i1
								EXCEPT
							SELECT i2.id,i2.identity,i2.identity_type_id FROM identities i2
								INNER JOIN identities i3
								ON i3.user_id = i2.user_id
								INNER JOIN identity_types it2
								ON it2.id=i3.identity_type_id AND it2.name=:id_type
							) AS i
				 	JOIN commits c
				 	ON c.id IN (SELECT cc.id FROM commits cc
									INNER JOIN repositories r2
									ON cc.author_id=i.id
									AND r2.id=cc.repo_id
									INNER JOIN sources s2
									ON s2.id=r2.source AND s2.name=:s_name
							ORDER BY RANDOM() LIMIT 1)
				 	INNER JOIN repositories r
				 	ON c.repo_id=r.id
				 	INNER JOIN sources s
					ON s.id=r.source AND s.name=:s_name
					ORDER BY i.identity
					;''',{'id_type':self.target_identity_type,'s_name':self.source_name})
		else:
			if self.db.db_type == 'postgres':
				self.db.cursor.execute('''
					SELECT s.id,r.owner,r.name,c.repo_id,c.sha,i.identity,i.id,i.identity_type_id
					FROM (
						SELECT ii.id,ii.identity,ii.identity_type_id FROM
					 		(SELECT iii.id,iii.identity,iii.identity_type_id FROM identities iii
							WHERE (SELECT iiii.id FROM identities iiii
								INNER JOIN identity_types iiiit
								ON iiii.user_id=iii.user_id AND iiiit.id=iiii.identity_type_id AND iiiit.name=%(id_type)s) IS NULL) AS ii
							LEFT JOIN table_updates tu
							ON tu.identity_id=ii.id AND tu.table_name='login'
							GROUP BY ii.id,ii.identity,ii.identity_type_id,tu.identity_id
							HAVING tu.identity_id IS NULL OR NOT BOOL_OR(tu.success)
						) AS i
					JOIN LATERAL (SELECT cc.sha,cc.repo_id FROM commits cc
									INNER JOIN repositories r2
									ON cc.author_id=i.id
									AND r2.id=cc.repo_id
									INNER JOIN sources s2
									ON s2.id=r2.source AND s2.name=%(s_name)s
							ORDER BY RANDOM() LIMIT 1) AS c
					ON true
					INNER JOIN repositories r
					ON r.id=c.repo_id
				 	INNER JOIN sources s
					ON s.id=r.source AND s.name=%(s_name)s
					ORDER BY i.id
					;''',{'id_type':self.target_identity_type,'s_name':self.source_name})
			else:
				self.db.cursor.execute('''
					SELECT s.id,r.owner,r.name,c.repo_id,c.sha,i.identity,i.id,i.identity_type_id
					FROM (
						SELECT ii.id,ii.identity,ii.identity_type_id FROM
					 		(SELECT iii.id,iii.identity,iii.identity_type_id FROM identities iii
							WHERE (SELECT iiii.id FROM identities iiii
								INNER JOIN identity_types iiiit
								ON iiii.user_id=iii.user_id AND iiiit.id=iiii.identity_type_id AND iiiit.name=:id_type) IS NULL) AS ii
							LEFT JOIN table_updates tu
							ON tu.identity_id=ii.id AND tu.table_name='login'
							GROUP BY ii.id,ii.identity,ii.identity_type_id,tu.identity_id
							HAVING tu.identity_id IS NULL OR NOT SUM(tu.success)
						) AS i
					JOIN commits c
						ON
						c.id IN (SELECT cc.id FROM commits cc
									INNER JOIN repositories r2
									ON cc.author_id=i.id
									AND r2.id=cc.repo_id
									INNER JOIN sources s2
									ON s2.id=r2.source AND s2.name=:s_name
							ORDER BY RANDOM() LIMIT 1)
					INNER JOIN repositories r
					ON r.id=c.repo_id
					INNER JOIN sources s
					ON s.id=r.source AND s.name=:s_name
					ORDER BY i.id
					;''',{'id_type':self.target_identity_type,'s_name':self.source_name})

		self.elt_list = list(self.db.cursor.fetchall())
		self.logger.info(self.force)

class CommitCommentReactionsGQLFiller(GHGQLFiller):
	def __init__(self,**kwargs):
		self.items_name = 'commit_comment_reactions'
		self.queried_obj = 'repo'
		self.sub_queried_obj = 'commit_comment'
		self.pageinfo_path = ['node','reactions','pageInfo']
		GHGQLFiller.__init__(self,**kwargs)

	def check_requirements(self):
		self.db.cursor.execute('''
			SELECT update_type,updated_at FROM full_updates
			WHERE update_type='commit_comments'
			ORDER BY updated_at DESC
			LIMIT 1
			;
			''')
		ans = list(self.db.cursor.fetchall())
		if len(ans) == 0:
			self.logger.info('missing commit_comments filler, necessary prior to commit_comment_reactions')
			return False
		else:
			return True

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
					repository(owner: "{repo_owner}", name: "{repo_name}") {{
  							nameWithOwner
    						id }}
  					node(id:"{commit_comment_gql_id}") {{
  					    ... on CommitComment {{

        							commit {{ oid }}
									databaseId
									id
									author {{ login }}
									bodyText
									createdAt
									reactions(first:{page_size}  {after_end_cursor}) {{
										totalCount
										pageInfo {{
											hasNextPage
											endCursor
											}}
										nodes {{
											createdAt
											user {{ login }}
											content
											id
											}}
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
			repo_owner,repo_name = query_result['repository']['nameWithOwner'].split('/')
		for e in [query_result['node']]:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'commit_comment'}
			try:
				# d['created_at'] = e['createdAt']
				# d['closed_at'] = e['closedAt']
				d['commit_comment_id'] = e['databaseId']
				d['commit_comment_gql_id'] = e['id']
				# d['issue_title'] = e['title']
				# d['comment_text'] = e['bodyText']
				d['commit_sha'] = e['commit']['oid']

			except (KeyError,TypeError) as err:
				self.logger.info('Result triggering error: {} \nError when parsing commit_comments for {}/{}: {}'.format(e,repo_owner,repo_name,err))
				continue
			else:
				ans.append(d)
				if e['reactions']['totalCount']>0:
					for ee in e['reactions']['nodes']:
						r = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'commit_comment_reaction'}
						r['commit_comment_id'] = d['commit_comment_id']
						r['commit_comment_gql_id'] = d['commit_comment_gql_id']
						r['commit_sha'] = d['commit_sha']
						try:
							r['created_at'] = ee['createdAt']
							r['commit_comment_reaction'] = ee['content']
							try:
								r['author_login'] = ee['user']['login']
							except:
								# r['author_login'] = None
								r['author_login'] = 'nullauthor_'+ee['id']
						except (KeyError,TypeError) as err:
							self.logger.info('Result triggering error: {} \nError when parsing commit_comment_reactions for {}/{}: {}'.format(e,repo_owner,repo_name,err))
							continue
						else:
							ans.append(r)
		return ans

	def insert_items(self,**kwargs):
		CommitCommentsGQLFiller.insert_reactions(self,**kwargs)


	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['node']['reactions']['totalCount']


class CommitCommentsGQLFiller(GHGQLFiller):
	'''
	Querying issues through the GraphQL API
	'''
	def __init__(self,complete_reactions=True,**kwargs):
		self.items_name = 'commit_comments'
		self.queried_obj = 'repo'
		self.pageinfo_path = ['repository','commitComments','pageInfo']
		self.complete_reactions = complete_reactions
		GHGQLFiller.__init__(self,**kwargs)

	def after_insert(self):
		if self.complete_reactions:
			self.db.add_filler(CommitCommentReactionsGQLFiller(**self.get_generic_kwargs()))

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
  					repository(owner: "{repo_owner}", name: "{repo_name}") {{
  							nameWithOwner
    						id
    						commitComments(first:{page_size} {after_end_cursor}) {{
    							totalCount
    							pageInfo {{
									endCursor
									hasNextPage
						 			}}
      							nodes {{
        							commit {{ oid }}
									databaseId
									id
									author {{ login }}
									bodyText
									createdAt
									reactions(first:{secondary_page_size}) {{
										totalCount
										pageInfo {{
											hasNextPage
											endCursor
											}}
										nodes {{
											createdAt
											user {{ login }}
											content
											id
											}}
										}}
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
			repo_owner,repo_name = query_result['repository']['nameWithOwner'].split('/')
		for e in query_result['repository']['commitComments']['nodes']:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'commit_comment'}
			try:
				d['created_at'] = e['createdAt']
				# d['closed_at'] = e['closedAt']
				d['commit_comment_id'] = e['databaseId']
				d['commit_comment_gql_id'] = e['id']
				# d['issue_title'] = e['title']
				d['comment_text'] = e['bodyText']
				d['commit_sha'] = e['commit']['oid']
				try:
					d['author_login'] = e['author']['login']
				except (KeyError,TypeError) as err:
					d['author_login'] = None

				d['reactions_pageinfo'] = e['reactions']['pageInfo']

			except (KeyError,TypeError) as err:
				self.logger.info('Result triggering error: {} \nError when parsing commit_comments for {}/{}: {}'.format(e,repo_owner,repo_name,err))
				continue
			else:
				ans.append(d)
				if e['reactions']['totalCount']>0:
					for ee in e['reactions']['nodes']:
						r = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'commit_comment_reaction'}
						r['commit_comment_id'] = d['commit_comment_id']
						r['commit_comment_gql_id'] = d['commit_comment_gql_id']
						r['commit_sha'] = d['commit_sha']
						try:
							r['created_at'] = ee['createdAt']
							r['commit_comment_reaction'] = ee['content']
							try:
								r['author_login'] = ee['user']['login']
							except:
								# r['author_login'] = None
								r['author_login'] = 'nullauthor_'+ee['id']
						except (KeyError,TypeError) as err:
							self.logger.info('Result triggering error: {} \nError when parsing commit_comment_reactions for {}/{}: {}'.format(e,repo_owner,repo_name,err))
							continue
						else:
							ans.append(r)
		return ans


	def insert_items(self,items_list,commit=True,db=None):
		'''
		In subclasses this has to be implemented
		inserts results in the DB
		'''
		if db is None:
			db = self.db
		if db.db_type == 'postgres':
			db.cursor.execute(f'''
				CREATE TEMPORARY TABLE IF NOT EXISTS temp_commits (commit_sha TEXT PRIMARY KEY,repo_id BIGINT)
				;''',)
			extras.execute_batch(db.cursor,'''
				INSERT INTO temp_commits(commit_sha,repo_id) SELECT %(commit_sha)s,%(repo_id)s WHERE NOT EXISTS (SELECT id FROM commits WHERE %(commit_sha)s=sha)
				ON CONFLICT DO NOTHING
				;''',(i for i in items_list if i['element_type']=='commit_comment'))
			db.cursor.execute('''
				INSERT INTO commits(sha,repo_id) SELECT commit_sha,repo_id FROM temp_commits
				ON CONFLICT DO NOTHING
				;''')
			db.cursor.execute('''
				INSERT INTO commit_repos(commit_id,repo_id) SELECT c.id,tc.repo_id FROM temp_commits tc
				INNER JOIN commits c
				ON c.sha=tc.commit_sha
				ON CONFLICT DO NOTHING
				;''')
			extras.execute_batch(db.cursor,'''
				WITH com_id AS (SELECT id FROM commits WHERE sha=%(commit_sha)s)
				INSERT INTO commit_comments(created_at,repo_id,commit_id,comment_id,comment_text,author_login,author_id,identity_type_id)
				VALUES(%(created_at)s,
						%(repo_id)s,
						(SELECT id FROM com_id),--COALESCE((SELECT id FROM commits WHERE sha=%(commit_sha)s),(INSERT INTO commits(sha,repo_id) SELECT %(commit_sha)s,%(repo_id)s RETURNING id)),
						%(commit_comment_id)s,
						%(comment_text)s,
						%(author_login)s,
						(SELECT id FROM identities WHERE identity=%(author_login)s AND identity_type_id=(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)),
						(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)
						)
				ON CONFLICT DO NOTHING
				;''',(i for i in items_list if i['element_type']=='commit_comment'))
			# extras.execute_batch(db.cursor,'''
			# 	CREATE OR REPLACE FUNCTION insert_commit_repo(sha TEXT,repo_id BIGINT) RETURNS BIGINT
			# 	LANGUAGE plpgsql
			# 	AS $$
			# 	DECLARE cid BIGINT;
			# 	BEGIN
			# 	INSERT INTO commits(sha,repo_id) SELECT sha,repo_id ON CONFLICT Do NOTHING RETURNING id INTO cid;
			# 	INSERT INTO commit_repos(commit_id,repo_id) SELECT cid,repo_id  ON CONFLICT Do NOTHING ;
			# 	RETURN cid;
			# 	END;
			# 	$$;
			# 	WITH com_id AS (SELECT id FROM commits WHERE sha=%(commit_sha)s),
			# 		ins_com_id AS ( SELECT insert_commit_repo(%(commit_sha)s,%(repo_id)s) AS cid )
			# 	INSERT INTO commit_comments(created_at,repo_id,commit_id,comment_id,comment_text,author_login,author_id,identity_type_id)
			# 	VALUES(%(created_at)s,
			# 			%(repo_id)s,
			# 			(SELECT COALESCE((SELECT id FROM com_id),(SELECT cid FROM ins_com_id))),--COALESCE((SELECT id FROM commits WHERE sha=%(commit_sha)s),(INSERT INTO commits(sha,repo_id) SELECT %(commit_sha)s,%(repo_id)s RETURNING id)),
			# 			%(commit_comment_id)s,
			# 			%(comment_text)s,
			# 			%(author_login)s,
			# 			(SELECT id FROM identities WHERE identity=%(author_login)s AND identity_type_id=(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)),
			# 			(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)
			# 			)

			# 	ON CONFLICT DO NOTHING
			# 	;''',(i for i in items_list if i['element_type']=='commit_comment'))
		else:
			db.cursor.execute(f'''
				CREATE TEMPORARY TABLE IF NOT EXISTS temp_commits (commit_sha TEXT PRIMARY KEY,repo_id INTEGER)
				;''',)
			db.cursor.executemany('''
				INSERT INTO temp_commits(commit_sha,repo_id) SELECT :commit_sha,:repo_id WHERE NOT EXISTS (SELECT id FROM commits WHERE :commit_sha=sha)
				;''',(i for i in items_list if i['element_type']=='commit_comment'))
			# db.cursor.executemany('''
			# 	INSERT INTO commits(sha,repo_id) SELECT :commit_sha,:repo_id WHERE NOT EXISTS (SELECT id FROM commits WHERE :commit_sha=sha)
			# 	;''',(i for i in items_list if i['element_type']=='commit_comment'))
			db.cursor.execute('''
				INSERT OR IGNORE INTO commits(sha,repo_id) SELECT commit_sha,repo_id FROM temp_commits
				;''')
			db.cursor.execute('''
				INSERT OR IGNORE INTO commit_repos(commit_id,repo_id) SELECT c.id,tc.repo_id FROM temp_commits tc
				INNER JOIN commits c
				ON c.sha=tc.commit_sha
				;''')
			db.cursor.executemany('''
				INSERT OR IGNORE INTO commit_comments(created_at,repo_id,commit_id,comment_id,comment_text,author_login,author_id,identity_type_id)
				SELECT :created_at,
						:repo_id,
						c.id,
						:commit_comment_id,
						:comment_text,
						:author_login,
						(SELECT id FROM identities WHERE identity=:author_login AND identity_type_id=(SELECT id FROM identity_types WHERE name=:target_identity_type)),
						(SELECT id FROM identity_types WHERE name=:target_identity_type)
					FROM commits c
					WHERE sha=:commit_sha
				;''',(i for i in items_list if i['element_type']=='commit_comment'))
				# ;''',((s['created_at'],s['closed_at'],s['repo_id'],s['issue_number'],s['issue_title'],s['issue_text'],s['author_login'],s['author_login'],self.target_identity_type,self.target_identity_type) for s in items_list))


		if commit:
			db.connection.commit()

		self.insert_reactions(items_list=items_list,commit=False,db=db)
		self.insert_reaction_updates(items_list=items_list,commit=commit,db=db)

		if len(items_list):
			db.cursor.execute('''INSERT INTO full_updates(update_type) SELECT 'commit_comments' ;''')

	def insert_reactions(self,items_list,commit=True,db=None):
		if db is None:
			db = self.db
		if db.db_type == 'postgres':
			extras.execute_batch(db.cursor,'''
				INSERT INTO commit_comment_reactions(created_at,repo_id,commit_id,comment_id,reaction,author_login,author_id,identity_type_id)
				VALUES(%(created_at)s,
						%(repo_id)s,
						(SELECT id FROM commits WHERE sha=%(commit_sha)s),
						%(commit_comment_id)s,
						%(commit_comment_reaction)s,
						%(author_login)s,
						(SELECT id FROM identities WHERE identity=%(author_login)s AND identity_type_id=(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)),
						(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)
						)

				ON CONFLICT DO NOTHING
				;''',(i for i in items_list if i['element_type']=='commit_comment_reaction'))
				# ;''',((s['created_at'],s['closed_at'],s['repo_id'],s['issue_number'],s['issue_title'],s['issue_text'],s['author_login'],s['author_login'],self.target_identity_type,self.target_identity_type) for s in items_list))
		else:
			db.cursor.executemany('''
					INSERT OR IGNORE INTO commit_comment_reactions(created_at,repo_id,commit_id,comment_id,reaction,author_login,author_id,identity_type_id)
				VALUES(:created_at,
						:repo_id,
						(SELECT id FROM commits WHERE sha=:commit_sha),
						:commit_comment_id,
						:commit_comment_reaction,
						:author_login,
						(SELECT id FROM identities WHERE identity=:author_login AND identity_type_id=(SELECT id FROM identity_types WHERE name=:target_identity_type)),
						(SELECT id FROM identity_types WHERE name=:target_identity_type)
						)
				;''',(i for i in items_list if i['element_type']=='commit_comment_reaction'))
				# ;''',((s['created_at'],s['closed_at'],s['repo_id'],s['issue_number'],s['issue_title'],s['issue_text'],s['author_login'],s['author_login'],self.target_identity_type,self.target_identity_type) for s in items_list))



		if commit:
			db.connection.commit()


	def insert_reaction_updates(self,items_list,commit=True,db=None):
		if db is None:
			db = self.db

		for i in items_list:
			if i['element_type'] == 'commit_comment':
				update_info = {'commit_comment_id':i['commit_comment_id'],
								'commit_comment_gql_id':i['commit_comment_gql_id']
								}
				if i['reactions_pageinfo']['hasNextPage']:
					success = None
					update_info['end_cursor'] = i['reactions_pageinfo']['endCursor']
					db.insert_update(table='commit_comment_reactions',repo_id=i['repo_id'],success=success,info=update_info)
				# else:
				# 	success = True
				# self.insert_update(db=db,repo_id=repo_id,success=success,info=update_info)



		if commit:
			db.connection.commit()



	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['repository']['commitComments']['totalCount']


class IssuesGQLFiller(GHGQLFiller):
	'''
	Querying issues through the GraphQL API
	'''
	def __init__(self,**kwargs):
		self.items_name = 'issues'
		self.queried_obj = 'repo'
		self.pageinfo_path = ['repository','issues','pageInfo']
		GHGQLFiller.__init__(self,**kwargs)

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
  					repository(owner: "{repo_owner}", name: "{repo_name}") {{
  							nameWithOwner
    						id
    						issues(first:{page_size} {after_end_cursor}) {{
    							totalCount
    							pageInfo {{
									endCursor
									hasNextPage
						 			}}
      							nodes {{
        							number
									title
									author {{ login }}
									bodyText
									createdAt
									closedAt
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
			repo_owner,repo_name = query_result['repository']['nameWithOwner'].split('/')
		for e in query_result['repository']['issues']['nodes']:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type}
			try:
				d['created_at'] = e['createdAt']
				d['closed_at'] = e['closedAt']
				d['issue_number'] = e['number']
				d['issue_title'] = e['title']
				d['issue_text'] = e['bodyText']
				try:
					d['author_login'] = e['author']['login']
				except (KeyError,TypeError) as err:
					d['author_login'] = None
			except (KeyError,TypeError) as err:
				self.logger.info('Result triggering error: {} \nError when parsing issues for {}/{}: {}'.format(e,repo_owner,repo_name,err))
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
				INSERT INTO issues(created_at,closed_at,repo_id,issue_number,issue_title,issue_text,author_login,author_id,identity_type_id)
				VALUES(%(created_at)s,
						%(closed_at)s,
						%(repo_id)s,
						%(issue_number)s,
						%(issue_title)s,
						%(issue_text)s,
						%(author_login)s,
						(SELECT id FROM identities WHERE identity=%(author_login)s AND identity_type_id=(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)),
						(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)
						)

				ON CONFLICT DO NOTHING
				;''',items_list)
				# ;''',((s['created_at'],s['closed_at'],s['repo_id'],s['issue_number'],s['issue_title'],s['issue_text'],s['author_login'],s['author_login'],self.target_identity_type,self.target_identity_type) for s in items_list))
		else:
			db.cursor.executemany('''
					INSERT OR IGNORE INTO issues(created_at,closed_at,repo_id,issue_number,issue_title,issue_text,author_login,author_id,identity_type_id)
					VALUES(:created_at,
						:closed_at,
						:repo_id,
						:issue_number,
						:issue_title,
						:issue_text,
						:author_login,
							(SELECT id FROM identities WHERE identity=:author_login AND identity_type_id=(SELECT id FROM identity_types WHERE name=:target_identity_type)),
							(SELECT id FROM identity_types WHERE name=:target_identity_type)
							)
				;''',items_list)
				# ;''',((s['created_at'],s['closed_at'],s['repo_id'],s['issue_number'],s['issue_title'],s['issue_text'],s['author_login'],s['author_login'],self.target_identity_type,self.target_identity_type) for s in items_list))


		if commit:
			db.connection.commit()

	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['repository']['issues']['totalCount']


class PullRequestsGQLFiller(GHGQLFiller):
	'''
	Querying issues through the GraphQL API
	'''
	def __init__(self,**kwargs):
		self.items_name = 'pullrequests'
		self.queried_obj = 'repo'
		self.pageinfo_path = ['repository','pullRequests','pageInfo']
		GHGQLFiller.__init__(self,**kwargs)

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
  					repository(owner: "{repo_owner}", name: "{repo_name}") {{
  							nameWithOwner
    						id
    						pullRequests(first:{page_size} {after_end_cursor}) {{
    							totalCount
    							pageInfo {{
									endCursor
									hasNextPage
						 			}}
      							nodes {{
        							number
									title
									author {{ login }}
									bodyText
									createdAt
									mergedAt
									mergedBy {{ login }}
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
			repo_owner,repo_name = query_result['repository']['nameWithOwner'].split('/')
		for e in query_result['repository']['pullRequests']['nodes']:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type}
			try:
				d['created_at'] = e['createdAt']
				d['merged_at'] = e['mergedAt']
				d['pullrequest_number'] = e['number']
				d['pullrequest_title'] = e['title']
				d['pullrequest_text'] = e['bodyText']
				try:
					d['author_login'] = e['author']['login']
				except (KeyError,TypeError) as err:
					d['author_login'] = None
				try:
					d['merger_login'] = e['mergedBy']['login']
				except (KeyError,TypeError) as err:
					d['merger_login'] = None
			except (KeyError,TypeError) as err:
				self.logger.info('Result triggering error: {} \nError when parsing pullrequests for {}/{}: {}'.format(e,repo_owner,repo_name,err))
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
				INSERT INTO pullrequests(created_at,merged_at,repo_id,pullrequest_number,pullrequest_title,pullrequest_text,author_login,author_id,merger_login,merger_id,identity_type_id)
				VALUES(%(created_at)s,
						%(merged_at)s,
						%(repo_id)s,
						%(pullrequest_number)s,
						%(pullrequest_title)s,
						%(pullrequest_text)s,
						%(author_login)s,
						(SELECT id FROM identities WHERE identity=%(author_login)s AND identity_type_id=(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)),
						%(merger_login)s,
						(SELECT id FROM identities WHERE identity=%(merger_login)s AND identity_type_id=(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)),
						(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)
						)

				ON CONFLICT DO NOTHING
				;''',items_list)
				# ;''',((s['created_at'],s['closed_at'],s['repo_id'],s['issue_number'],s['issue_title'],s['issue_text'],s['author_login'],s['author_login'],self.target_identity_type,self.target_identity_type) for s in items_list))
		else:
			db.cursor.executemany('''
					INSERT OR IGNORE INTO pullrequests(created_at,merged_at,repo_id,pullrequest_number,pullrequest_title,pullrequest_text,author_login,author_id,merger_login,merger_id,identity_type_id)
				VALUES(:created_at,
						:merged_at,
						:repo_id,
						:pullrequest_number,
						:pullrequest_title,
						:pullrequest_text,
						:author_login,
						(SELECT id FROM identities WHERE identity=:merger_login AND identity_type_id=(SELECT id FROM identity_types WHERE name=:target_identity_type)),
						:merger_login,
						(SELECT id FROM identities WHERE identity=:author_login AND identity_type_id=(SELECT id FROM identity_types WHERE name=:target_identity_type)),
						(SELECT id FROM identity_types WHERE name=:target_identity_type)
						)
				;''',items_list)

		if commit:
			db.connection.commit()

	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['repository']['pullRequests']['totalCount']


class CompleteIssuesGQLFiller(IssuesGQLFiller):
	'''
	Querying issues through the GraphQL API
	Adding comments, labels and reactions (first 100 per issue), and first 40 reactions to comments.
	'''

	def __init__(self,init_page_size=6,secondary_page_size=4,complete_info=True,**kwargs):
		IssuesGQLFiller.__init__(self,init_page_size=init_page_size,secondary_page_size=secondary_page_size,other_update_names=['issues'],**kwargs)
		self.items_name = 'complete_issues'
		self.complete_info = complete_info

	def after_insert(self):
		if self.complete_info:
			self.db.add_filler(IssueReactionsGQLFiller(**self.get_generic_kwargs()))
			self.db.add_filler(IssueLabelsGQLFiller(**self.get_generic_kwargs()))
			self.db.add_filler(IssueCommentsGQLFiller(**self.get_generic_kwargs()))
			self.db.add_filler(IssueCommentReactionsGQLFiller(**self.get_generic_kwargs()))

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
  					repository(owner: "{repo_owner}", name: "{repo_name}") {{
  							nameWithOwner
    						id
    						issues(first:{page_size} {after_end_cursor}) {{
    							totalCount
    							pageInfo {{
									endCursor
									hasNextPage
						 			}}
      							nodes {{
        							number
									title
									id
									author {{ login }}
									bodyText
									createdAt
									closedAt
									comments(first:{secondary_page_size}) {{
										totalCount
										pageInfo {{
											endCursor
											hasNextPage
											}}
										nodes {{
											id
											createdAt
											author {{ login }}
											bodyText
											databaseId

											reactions(first:{secondary_page_size}) {{
												totalCount
												pageInfo {{
													endCursor
													hasNextPage
													}}
												nodes {{
													user {{ login }}
													createdAt
													content
													id
													}}
												}}
											}}
										}}

									labels(first:{secondary_page_size}) {{
										totalCount
										pageInfo {{
											endCursor
											hasNextPage
											}}
										nodes {{
											name
											createdAt
											}}
										}}

									reactions(first:{secondary_page_size}) {{
										totalCount
										pageInfo {{
											endCursor
											hasNextPage
											}}
										nodes {{
											user {{ login }}
											createdAt
											content
											id
											}}
										}}
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
			repo_owner,repo_name = query_result['repository']['nameWithOwner'].split('/')
		for e in query_result['repository']['issues']['nodes']:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'issue'}
			try:
				d['created_at'] = e['createdAt']
				d['closed_at'] = e['closedAt']
				d['issue_number'] = e['number']
				d['issue_title'] = e['title']
				d['issue_gql_id'] = e['id']
				d['issue_text'] = e['bodyText']

				d['reactions_pageinfo'] = e['reactions']['pageInfo']
				d['labels_pageinfo'] = e['labels']['pageInfo']
				d['comments_pageinfo'] = e['comments']['pageInfo']

				try:
					d['author_login'] = e['author']['login']
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
							r['issue_label'] = ee['name']
							# try:
							# 	r['author_login'] = ee['user']['login']
							# except:
							# 	r['author_login'] = None
						except (KeyError,TypeError) as err:
							self.logger.info('Result triggering error: {} \nError when parsing issue_labels for {}/{}: {}'.format(e,repo_owner,repo_name,err))
							continue
						else:
							ans.append(r)

				if e['reactions']['totalCount']>0:
					for ee in e['reactions']['nodes']:
						r = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'issue_reaction'}
						r['issue_number'] = d['issue_number']
						r['issue_gql_id'] = d['issue_gql_id']
						try:
							r['created_at'] = ee['createdAt']
							r['issue_reaction'] = ee['content']
							try:
								r['author_login'] = ee['user']['login']
							except:
								# r['author_login'] = None
								r['author_login'] = 'nullauthor_'+ee['id']
						except (KeyError,TypeError) as err:
							self.logger.info('Result triggering error: {} \nError when parsing issue_reactions for {}/{}: {}'.format(e,repo_owner,repo_name,err))
							continue
						else:
							ans.append(r)

				if e['comments']['totalCount']>0:
					for ee in e['comments']['nodes']:
						r = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'issue_comment'}
						r['issue_number'] = d['issue_number']
						r['issue_gql_id'] = d['issue_gql_id']
						try:
							r['created_at'] = ee['createdAt']
							r['issue_comment_text'] = ee['bodyText']
							r['issue_comment_id'] = ee['databaseId']
							r['issue_comment_gql_id'] = ee['id']
							r['comment_reactions_pageinfo'] = ee['reactions']['pageInfo']
							try:
								r['author_login'] = ee['author']['login']
							except:
								r['author_login'] = None


						except (KeyError,TypeError) as err:
							self.logger.info('Result triggering error: {} \nError when parsing issue_comments for {}/{}: {}'.format(e,repo_owner,repo_name,err))
							continue
						else:
							ans.append(r)

						if ee['reactions']['totalCount']>0:
							for eee in ee['reactions']['nodes']:
								r = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'issue_comment_reaction'}
								r['issue_number'] = d['issue_number']
								r['issue_gql_id'] = d['issue_gql_id']
								r['issue_comment_id'] = ee['databaseId']
								r['issue_comment_gql_id'] = ee['id']
								try:
									r['created_at'] = eee['createdAt']
									r['issue_comment_reaction'] = eee['content']
									try:
										r['author_login'] = eee['user']['login']
									except:
										# r['author_login'] = None
										r['author_login'] = 'nullauthor_'+eee['id']
								except (KeyError,TypeError) as err:
									self.logger.info('Result triggering error: {} \nError when parsing issue_comment_reactions for {}/{}: {}'.format(e,repo_owner,repo_name,err))
									continue
								else:
									ans.append(r)
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
				INSERT INTO issues(created_at,closed_at,repo_id,issue_number,issue_title,issue_text,author_login,author_id,identity_type_id)
				VALUES(%(created_at)s,
						%(closed_at)s,
						%(repo_id)s,
						%(issue_number)s,
						%(issue_title)s,
						%(issue_text)s,
						%(author_login)s,
						(SELECT id FROM identities WHERE identity=%(author_login)s AND identity_type_id=(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)),
						(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)
						)

				ON CONFLICT DO NOTHING
				;''',(i for i in items_list if i['element_type']=='issue'))
				# ;''',((s['created_at'],s['closed_at'],s['repo_id'],s['issue_number'],s['issue_title'],s['issue_text'],s['author_login'],s['author_login'],self.target_identity_type,self.target_identity_type) for s in items_list))
		else:
			db.cursor.executemany('''
					INSERT OR IGNORE INTO issues(created_at,closed_at,repo_id,issue_number,issue_title,issue_text,author_login,author_id,identity_type_id)
					VALUES(:created_at,
						:closed_at,
						:repo_id,
						:issue_number,
						:issue_title,
						:issue_text,
						:author_login,
							(SELECT id FROM identities WHERE identity=:author_login AND identity_type_id=(SELECT id FROM identity_types WHERE name=:target_identity_type)),
							(SELECT id FROM identity_types WHERE name=:target_identity_type)
							)
				;''',(i for i in items_list if i['element_type']=='issue'))
				# ;''',((s['created_at'],s['closed_at'],s['repo_id'],s['issue_number'],s['issue_title'],s['issue_text'],s['author_login'],s['author_login'],self.target_identity_type,self.target_identity_type) for s in items_list))


		self.insert_reactions(items_list=items_list,commit=commit,db=db)
		self.insert_reaction_updates(items_list=items_list,commit=commit,db=db)

		self.insert_labels(items_list=items_list,commit=commit,db=db)
		self.insert_label_updates(items_list=items_list,commit=commit,db=db)

		self.insert_comments(items_list=items_list,commit=commit,db=db)
		self.insert_comment_updates(items_list=items_list,commit=commit,db=db)

		self.insert_comment_reactions(items_list=items_list,commit=commit,db=db)
		self.insert_comment_reaction_updates(items_list=items_list,commit=commit,db=db)


		if len(items_list):
			db.cursor.execute('''INSERT INTO full_updates(update_type) SELECT 'complete_issues' ;''')

		if commit:
			db.connection.commit()

	def insert_reactions(self,items_list,commit=True,db=None):
		if db is None:
			db = self.db
		if db.db_type == 'postgres':
			extras.execute_batch(db.cursor,'''
				INSERT INTO issue_reactions(created_at,repo_id,issue_number,reaction,author_login,author_id,identity_type_id)
				VALUES(%(created_at)s,
						%(repo_id)s,
						%(issue_number)s,
						%(issue_reaction)s,
						%(author_login)s,
						(SELECT id FROM identities WHERE identity=%(author_login)s AND identity_type_id=(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)),
						(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)
						)

				ON CONFLICT DO NOTHING
				;''',(i for i in items_list if i['element_type']=='issue_reaction'))

		else:
			db.cursor.executemany('''
					INSERT OR IGNORE INTO issue_reactions(created_at,repo_id,issue_number,reaction,author_login,author_id,identity_type_id)
				VALUES(:created_at,
						:repo_id,
						:issue_number,
						:issue_reaction,
						:author_login,
						(SELECT id FROM identities WHERE identity=:author_login AND identity_type_id=(SELECT id FROM identity_types WHERE name=:target_identity_type)),
						(SELECT id FROM identity_types WHERE name=:target_identity_type)
						)
				;''',(i for i in items_list if i['element_type']=='issue_reaction'))


		if commit:
			db.connection.commit()

	def insert_comment_reactions(self,items_list,commit=True,db=None):
		if db is None:
			db = self.db
		if db.db_type == 'postgres':
			extras.execute_batch(db.cursor,'''
				INSERT INTO issue_comment_reactions(created_at,repo_id,issue_number,comment_id,reaction,author_login,author_id,identity_type_id)
				VALUES(%(created_at)s,
						%(repo_id)s,
						%(issue_number)s,
						%(issue_comment_id)s,
						%(issue_comment_reaction)s,
						%(author_login)s,
						(SELECT id FROM identities WHERE identity=%(author_login)s AND identity_type_id=(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)),
						(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)
						)

				ON CONFLICT DO NOTHING
				;''',(i for i in items_list if i['element_type']=='issue_comment_reaction'))

		else:
			db.cursor.executemany('''
					INSERT OR IGNORE INTO issue_comment_reactions(created_at,repo_id,issue_number,comment_id,reaction,author_login,author_id,identity_type_id)
				VALUES(:created_at,
						:repo_id,
						:issue_number,
						:issue_comment_id,
						:issue_comment_reaction,
						:author_login,
						(SELECT id FROM identities WHERE identity=:author_login AND identity_type_id=(SELECT id FROM identity_types WHERE name=:target_identity_type)),
						(SELECT id FROM identity_types WHERE name=:target_identity_type)
						)
				;''',(i for i in items_list if i['element_type']=='issue_comment_reaction'))


		if commit:
			db.connection.commit()

	def insert_comments(self,items_list,commit=True,db=None):
		if db is None:
			db = self.db
		if db.db_type == 'postgres':
			extras.execute_batch(db.cursor,'''
				INSERT INTO issue_comments(created_at,repo_id,issue_number,comment_id,comment_text,author_login,author_id,identity_type_id)
				VALUES(%(created_at)s,
						%(repo_id)s,
						%(issue_number)s,
						%(issue_comment_id)s,
						%(issue_comment_text)s,
						%(author_login)s,
						(SELECT id FROM identities WHERE identity=%(author_login)s AND identity_type_id=(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)),
						(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)
						)

				ON CONFLICT DO NOTHING
				;''',(i for i in items_list if i['element_type']=='issue_comment'))

		else:
			db.cursor.executemany('''
					INSERT OR IGNORE INTO issue_comments(created_at,repo_id,issue_number,comment_id,comment_text,author_login,author_id,identity_type_id)
				VALUES(:created_at,
						:repo_id,
						:issue_number,
						:issue_comment_id,
						:issue_comment_text,
						:author_login,
						(SELECT id FROM identities WHERE identity=:author_login AND identity_type_id=(SELECT id FROM identity_types WHERE name=:target_identity_type)),
						(SELECT id FROM identity_types WHERE name=:target_identity_type)
						)
				;''',(i for i in items_list if i['element_type']=='issue_comment'))


		if commit:
			db.connection.commit()

	def insert_labels(self,items_list,commit=True,db=None):
		if db is None:
			db = self.db
		if db.db_type == 'postgres':
			extras.execute_batch(db.cursor,'''
				INSERT INTO issue_labels(repo_id,issue_number,label)
				VALUES(%(repo_id)s,
						%(issue_number)s,
						%(issue_label)s
						)

				ON CONFLICT DO NOTHING
				;''',(i for i in items_list if i['element_type']=='issue_label'))

		else:
			db.cursor.executemany('''
					INSERT OR IGNORE INTO issue_labels(repo_id,issue_number,label)
				VALUES(:repo_id,
						:issue_number,
						:issue_label
						)
				;''',(i for i in items_list if i['element_type']=='issue_label'))


		if commit:
			db.connection.commit()


	def insert_reaction_updates(self,items_list,commit=True,db=None):
		if db is None:
			db = self.db

		for i in items_list:
			if i['element_type'] == 'issue':
				update_info = {'issue_id':i['issue_number'],
								'issue_gql_id':i['issue_gql_id']
								}
				if i['reactions_pageinfo']['hasNextPage']:
					success = None
					update_info['end_cursor'] = i['reactions_pageinfo']['endCursor']
					db.insert_update(table='issue_reactions',repo_id=i['repo_id'],success=success,info=update_info)

		if commit:
			db.connection.commit()

	def insert_label_updates(self,items_list,commit=True,db=None):
		if db is None:
			db = self.db

		for i in items_list:
			if i['element_type'] == 'issue':
				update_info = {'issue_id':i['issue_number'],
								'issue_gql_id':i['issue_gql_id']
								}
				if i['labels_pageinfo']['hasNextPage']:
					success = None
					update_info['end_cursor'] = i['labels_pageinfo']['endCursor']
					db.insert_update(table='issue_labels',repo_id=i['repo_id'],success=success,info=update_info)

		if commit:
			db.connection.commit()

	def insert_comment_updates(self,items_list,commit=True,db=None):
		if db is None:
			db = self.db

		for i in items_list:
			if i['element_type'] == 'issue':
				update_info = {'issue_id':i['issue_number'],
								'issue_gql_id':i['issue_gql_id']
								}
				if i['comments_pageinfo']['hasNextPage']:
					success = None
					update_info['end_cursor'] = i['comments_pageinfo']['endCursor']
					db.insert_update(table='issue_comments',repo_id=i['repo_id'],success=success,info=update_info)

		if commit:
			db.connection.commit()

	def insert_comment_reaction_updates(self,items_list,commit=True,db=None):
		if db is None:
			db = self.db

		for i in items_list:
			if i['element_type'] == 'issue_comment':
				update_info = {'issue_comment_id':i['issue_comment_id'],
								'issue_comment_gql_id':i['issue_comment_gql_id']
								}
				if i['comment_reactions_pageinfo']['hasNextPage']:
					success = None
					update_info['end_cursor'] = i['comment_reactions_pageinfo']['endCursor']
					db.insert_update(table='issue_comment_reactions',repo_id=i['repo_id'],success=success,info=update_info)

		if commit:
			db.connection.commit()

class IssueReactionsGQLFiller(GHGQLFiller):
	def __init__(self,**kwargs):
		self.items_name = 'issue_reactions'
		self.queried_obj = 'repo'
		self.sub_queried_obj = 'issue'
		self.pageinfo_path = ['node','reactions','pageInfo']
		GHGQLFiller.__init__(self,**kwargs)

	def check_requirements(self):
		self.db.cursor.execute('''
			SELECT update_type,updated_at FROM full_updates
			WHERE update_type='complete_issues'
			ORDER BY updated_at DESC
			LIMIT 1
			;
			''')
		ans = list(self.db.cursor.fetchall())
		if len(ans) == 0:
			self.logger.info('missing complete_issues filler, necessary prior to issue_reactions')
			return False
		else:
			return True

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
					repository(owner: "{repo_owner}", name: "{repo_name}") {{
  							nameWithOwner
    						id }}
  					node(id:"{issue_gql_id}") {{
  					    ... on Issue {{

									id
									number
									reactions(first:{page_size}  {after_end_cursor}) {{
										totalCount
										pageInfo {{
											hasNextPage
											endCursor
											}}
										nodes {{
											createdAt
											user {{ login }}
											content
											id
											}}
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
			repo_owner,repo_name = query_result['repository']['nameWithOwner'].split('/')
		for e in [query_result['node']]:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'issue'}
			try:
				d['issue_gql_id'] = e['id']
				d['issue_number'] = e['number']
				d['issue_id'] = e['number']
				
			except (KeyError,TypeError) as err:
				self.logger.info('Result triggering error: {} \nError when parsing issues for {}/{}: {}'.format(e,repo_owner,repo_name,err))
				continue
			else:
				ans.append(d)
				if e['reactions']['totalCount']>0:
					for ee in e['reactions']['nodes']:
						r = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'issue_reaction'}
						r['issue_id'] = d['issue_id']
						r['issue_number'] = d['issue_id']
						r['issue_gql_id'] = d['issue_gql_id']
						try:
							r['created_at'] = ee['createdAt']
							r['issue_reaction'] = ee['content']
							try:
								r['author_login'] = ee['user']['login']
							except:
								# r['author_login'] = None
								r['author_login'] = 'nullauthor_'+ee['id']
						except (KeyError,TypeError) as err:
							self.logger.info('Result triggering error: {} \nError when parsing issue_reactions for {}/{}: {}'.format(e,repo_owner,repo_name,err))
							continue
						else:
							ans.append(r)
		return ans

	def insert_items(self,**kwargs):
		CompleteIssuesGQLFiller.insert_reactions(self,**kwargs)


	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['node']['reactions']['totalCount']


class IssueCommentsGQLFiller(GHGQLFiller):
	def __init__(self,**kwargs):
		self.items_name = 'issue_comments'
		self.queried_obj = 'repo'
		self.sub_queried_obj = 'issue'
		self.pageinfo_path = ['node','comments','pageInfo']
		GHGQLFiller.__init__(self,**kwargs)

	def check_requirements(self):
		self.db.cursor.execute('''
			SELECT update_type,updated_at FROM full_updates
			WHERE update_type='complete_issues'
			ORDER BY updated_at DESC
			LIMIT 1
			;
			''')
		ans = list(self.db.cursor.fetchall())
		if len(ans) == 0:
			self.logger.info('missing complete_issues filler, necessary prior to issue_comments')
			return False
		else:
			return True

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
					repository(owner: "{repo_owner}", name: "{repo_name}") {{
  							nameWithOwner
    						id }}
  					node(id:"{issue_gql_id}") {{
  					    ... on Issue {{

									id
									number
									comments(first:{page_size} {after_end_cursor}) {{
										totalCount
										pageInfo {{
											endCursor
											hasNextPage
											}}
										nodes {{
											id
											createdAt
											author {{ login }}
											bodyText
											databaseId

											reactions(first:{secondary_page_size}) {{
												totalCount
												pageInfo {{
													endCursor
													hasNextPage
													}}
												nodes {{
													user {{ login }}
													createdAt
													content
													id
													}}
												}}
											}}
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
			repo_owner,repo_name = query_result['repository']['nameWithOwner'].split('/')
		for e in [query_result['node']]:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'issue'}
			try:
				d['issue_gql_id'] = e['id']
				d['issue_number'] = e['number']
				d['issue_id'] = e['number']
				
			except (KeyError,TypeError) as err:
				self.logger.info('Result triggering error: {} \nError when parsing issues for {}/{}: {}'.format(e,repo_owner,repo_name,err))
				continue
			else:
				ans.append(d)
				if e['comments']['totalCount']>0:
					for ee in e['comments']['nodes']:
						r = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'issue_comment'}
						r['issue_number'] = d['issue_number']
						r['issue_gql_id'] = d['issue_gql_id']
						try:
							r['created_at'] = ee['createdAt']
							r['issue_comment_text'] = ee['bodyText']
							r['issue_comment_id'] = ee['databaseId']
							r['issue_comment_gql_id'] = ee['id']
							r['comment_reactions_pageinfo'] = ee['reactions']['pageInfo']
							try:
								r['author_login'] = ee['author']['login']
							except:
								r['author_login'] = None


						except (KeyError,TypeError) as err:
							self.logger.info('Result triggering error: {} \nError when parsing issue_comments for {}/{}: {}'.format(e,repo_owner,repo_name,err))
							continue
						else:
							ans.append(r)

						if ee['reactions']['totalCount']>0:
							for eee in ee['reactions']['nodes']:
								r = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'issue_comment_reaction'}
								r['issue_number'] = d['issue_number']
								r['issue_gql_id'] = d['issue_gql_id']
								r['issue_comment_id'] = ee['databaseId']
								r['issue_comment_gql_id'] = ee['id']
								try:
									r['created_at'] = eee['createdAt']
									r['issue_comment_reaction'] = eee['content']
									try:
										r['author_login'] = eee['user']['login']
									except:
										# r['author_login'] = None
										r['author_login'] = 'nullauthor_'+eee['id']
								except (KeyError,TypeError) as err:
									self.logger.info('Result triggering error: {} \nError when parsing issue_comment_reactions for {}/{}: {}'.format(e,repo_owner,repo_name,err))
									continue
								else:
									ans.append(r)
		return ans

	def insert_items(self,db=None,**kwargs):

		if db is None:
			db = self.db

		CompleteIssuesGQLFiller.insert_comments(self,db=db,**kwargs)
		CompleteIssuesGQLFiller.insert_comment_reactions(self,db=db,**kwargs)
		CompleteIssuesGQLFiller.insert_comment_reaction_updates(self,db=db,**kwargs)

		if len(kwargs['items_list']):
			db.cursor.execute('''INSERT INTO full_updates(update_type) SELECT 'issue_comments' ;''')

	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['node']['comments']['totalCount']


class IssueCommentReactionsGQLFiller(GHGQLFiller):
	def __init__(self,**kwargs):
		self.items_name = 'issue_comment_reactions'
		self.queried_obj = 'repo'
		self.sub_queried_obj = 'issue_comment'
		self.pageinfo_path = ['node','reactions','pageInfo']
		GHGQLFiller.__init__(self,**kwargs)

	def check_requirements(self):
		self.db.cursor.execute('''
			SELECT update_type,updated_at FROM full_updates
			WHERE update_type='complete_issues'
			ORDER BY updated_at DESC
			LIMIT 1
			;
			''')
		ans = list(self.db.cursor.fetchall())
		if len(ans) == 0:
			self.logger.info('missing complete_issues filler, necessary prior to issue_reactions')
			return False

		self.db.cursor.execute('''
			SELECT update_type,updated_at FROM full_updates
			WHERE update_type='issue_comments'
			ORDER BY updated_at DESC
			LIMIT 1
			;
			''')
		ans = list(self.db.cursor.fetchall())
		if len(ans) == 0:
			self.logger.info('missing issue_comments filler, necessary prior to issue_reactions')
			return False
		else:
			return True

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
					repository(owner: "{repo_owner}", name: "{repo_name}") {{
  							nameWithOwner
    						id }}
  					node(id:"{issue_comment_gql_id}") {{
  					    ... on IssueComment {{
  					    			issue {{ number }}
									id
									databaseId
									reactions(first:{page_size}  {after_end_cursor}) {{
										totalCount
										pageInfo {{
											hasNextPage
											endCursor
											}}
										nodes {{
											createdAt
											user {{ login }}
											content
											id
											}}
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
			repo_owner,repo_name = query_result['repository']['nameWithOwner'].split('/')
		for e in [query_result['node']]:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'issue_comment'}
			try:
				# d['issue_gql_id'] = e['id']
				d['issue_number'] = e['issue']['number']
				# d['issue_id'] = e['number']
				d['issue_comment_id'] = e['databaseId']
				d['issue_comment_gql_id'] = e['id']
				d['comment_reactions_pageinfo'] = e['reactions']['pageInfo']
				
			except (KeyError,TypeError) as err:
				self.logger.info('Result triggering error: {} \nError when parsing issue_comments for {}/{}: {}'.format(e,repo_owner,repo_name,err))
				continue
			else:
				ans.append(d)
				
				if e['reactions']['totalCount']>0:
					for ee in e['reactions']['nodes']:
						r = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'issue_comment_reaction'}
						r['issue_number'] = d['issue_number']
						r['issue_comment_id'] = e['databaseId']
						r['issue_comment_gql_id'] = e['id']
						try:
							r['created_at'] = ee['createdAt']
							r['issue_comment_reaction'] = ee['content']
							try:
								r['author_login'] = ee['user']['login']
							except:
								# r['author_login'] = None
								r['author_login'] = 'nullauthor_'+ee['id']
						except (KeyError,TypeError) as err:
							self.logger.info('Result triggering error: {} \nError when parsing issue_comment_reactions for {}/{}: {}'.format(e,repo_owner,repo_name,err))
							continue
						else:
							ans.append(r)
		return ans

	def insert_items(self,**kwargs):
		CompleteIssuesGQLFiller.insert_comment_reactions(self,**kwargs)


	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['node']['reactions']['totalCount']


class IssueLabelsGQLFiller(GHGQLFiller):
	def __init__(self,**kwargs):
		self.items_name = 'issue_labels'
		self.queried_obj = 'repo'
		self.sub_queried_obj = 'issue'
		self.pageinfo_path = ['node','labels','pageInfo']
		GHGQLFiller.__init__(self,**kwargs)

	def check_requirements(self):
		self.db.cursor.execute('''
			SELECT update_type,updated_at FROM full_updates
			WHERE update_type='complete_issues'
			ORDER BY updated_at DESC
			LIMIT 1
			;
			''')
		ans = list(self.db.cursor.fetchall())
		if len(ans) == 0:
			self.logger.info('missing complete_issues filler, necessary prior to issue_labels')
			return False
		else:
			return True

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
					repository(owner: "{repo_owner}", name: "{repo_name}") {{
  							nameWithOwner
    						id }}
  					node(id:"{issue_gql_id}") {{
  					    ... on Issue {{

									id
									number
									
									labels(first:{page_size} {after_end_cursor}) {{
										totalCount
										pageInfo {{
											endCursor
											hasNextPage
											}}
										nodes {{
											name
											createdAt
											}}
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
			repo_owner,repo_name = query_result['repository']['nameWithOwner'].split('/')
		for e in [query_result['node']]:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'issue'}
			try:
				d['issue_gql_id'] = e['id']
				d['issue_number'] = e['number']
				d['issue_id'] = e['number']
				
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
							r['issue_label'] = ee['name']
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

	def insert_items(self,**kwargs):
		CompleteIssuesGQLFiller.insert_labels(self,**kwargs)


	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['node']['labels']['totalCount']



class CompletePullRequestsGQLFiller(PullRequestsGQLFiller):
	'''
	Querying issues through the GraphQL API
	Adding comments, labels and reactions (first 100 per issue), and first 40 reactions to comments.
	'''

	def __init__(self,init_page_size=6,secondary_page_size=4,complete_info=True,**kwargs):
		PullRequestsGQLFiller.__init__(self,init_page_size=init_page_size,secondary_page_size=secondary_page_size,other_update_names=['pullrequests'],**kwargs)
		self.items_name = 'complete_pullrequests'
		self.complete_info = complete_info

	def after_insert(self):
		if self.complete_info:
			self.db.add_filler(PRReactionsGQLFiller(**self.get_generic_kwargs()))
			self.db.add_filler(PRLabelsGQLFiller(**self.get_generic_kwargs()))
			self.db.add_filler(PRCommentsGQLFiller(**self.get_generic_kwargs()))
			self.db.add_filler(PRCommentReactionsGQLFiller(**self.get_generic_kwargs()))

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
  					repository(owner: "{repo_owner}", name: "{repo_name}") {{
  							nameWithOwner
    						id
    						pullRequests(first:{page_size} {after_end_cursor}) {{
    							totalCount
    							pageInfo {{
									endCursor
									hasNextPage
						 			}}
      							nodes {{
        							number
									title
									id
									author {{ login }}
									bodyText
									createdAt
									mergedAt
									mergedBy {{ login }}
									comments(first:{secondary_page_size}) {{
										totalCount
										pageInfo {{
											endCursor
											hasNextPage
											}}
										nodes {{
											id
											createdAt
											author {{ login }}
											bodyText
											databaseId

											reactions(first:{secondary_page_size}) {{
												totalCount
												pageInfo {{
													endCursor
													hasNextPage
													}}
												nodes {{
													user {{ login }}
													createdAt
													content
													id
													}}
												}}
											}}
										}}

									labels(first:{secondary_page_size}) {{
										totalCount
										pageInfo {{
											endCursor
											hasNextPage
											}}
										nodes {{
											name
											createdAt
											}}
										}}

									reactions(first:{secondary_page_size}) {{
										totalCount
										pageInfo {{
											endCursor
											hasNextPage
											}}
										nodes {{
											user {{ login }}
											createdAt
											content
											id
											}}
										}}
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
			repo_owner,repo_name = query_result['repository']['nameWithOwner'].split('/')
		for e in query_result['repository']['pullRequests']['nodes']:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'pullrequest'}
			try:
				d['created_at'] = e['createdAt']
				d['merged_at'] = e['mergedAt']
				d['pullrequest_number'] = e['number']
				d['pullrequest_title'] = e['title']
				d['pullrequest_gql_id'] = e['id']
				d['pullrequest_text'] = e['bodyText']

				d['reactions_pageinfo'] = e['reactions']['pageInfo']
				d['labels_pageinfo'] = e['labels']['pageInfo']
				d['comments_pageinfo'] = e['comments']['pageInfo']

				try:
					d['author_login'] = e['author']['login']
				except (KeyError,TypeError) as err:
					d['author_login'] = None
				try:
					d['merger_login'] = e['mergedBy']['login']
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
							r['pullrequest_label'] = ee['name']
							# try:
							# 	r['author_login'] = ee['user']['login']
							# except:
							# 	r['author_login'] = None
						except (KeyError,TypeError) as err:
							self.logger.info('Result triggering error: {} \nError when parsing pullrequest_labels for {}/{}: {}'.format(e,repo_owner,repo_name,err))
							continue
						else:
							ans.append(r)

				if e['reactions']['totalCount']>0:
					for ee in e['reactions']['nodes']:
						r = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'pullrequest_reaction'}
						r['pullrequest_number'] = d['pullrequest_number']
						r['pullrequest_gql_id'] = d['pullrequest_gql_id']
						try:
							r['created_at'] = ee['createdAt']
							r['pullrequest_reaction'] = ee['content']
							try:
								r['author_login'] = ee['user']['login']
							except:
								# r['author_login'] = None
								r['author_login'] = 'nullauthor_'+ee['id']
						except (KeyError,TypeError) as err:
							self.logger.info('Result triggering error: {} \nError when parsing pullrequest_reactions for {}/{}: {}'.format(e,repo_owner,repo_name,err))
							continue
						else:
							ans.append(r)

				if e['comments']['totalCount']>0:
					for ee in e['comments']['nodes']:
						r = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'pullrequest_comment'}
						r['pullrequest_number'] = d['pullrequest_number']
						r['pullrequest_gql_id'] = d['pullrequest_gql_id']
						try:
							r['created_at'] = ee['createdAt']
							r['pullrequest_comment_text'] = ee['bodyText']
							r['pullrequest_comment_id'] = ee['databaseId']
							r['pullrequest_comment_gql_id'] = ee['id']
							r['comment_reactions_pageinfo'] = ee['reactions']['pageInfo']
							try:
								r['author_login'] = ee['author']['login']
							except:
								r['author_login'] = None


						except (KeyError,TypeError) as err:
							self.logger.info('Result triggering error: {} \nError when parsing pullrequest_comments for {}/{}: {}'.format(e,repo_owner,repo_name,err))
							continue
						else:
							ans.append(r)

						if ee['reactions']['totalCount']>0:
							for eee in ee['reactions']['nodes']:
								r = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'pullrequest_comment_reaction'}
								r['pullrequest_number'] = d['pullrequest_number']
								r['pullrequest_comment_id'] = ee['databaseId']
								r['pullrequest_comment_gql_id'] = ee['id']
								try:
									r['created_at'] = eee['createdAt']
									r['pullrequest_comment_reaction'] = eee['content']
									try:
										r['author_login'] = eee['user']['login']
									except:
										# r['author_login'] = None
										r['author_login'] = 'nullauthor_'+eee['id']
								except (KeyError,TypeError) as err:
									self.logger.info('Result triggering error: {} \nError when parsing pullrequest_comment_reactions for {}/{}: {}'.format(e,repo_owner,repo_name,err))
									continue
								else:
									ans.append(r)
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
				INSERT INTO pullrequests(created_at,merged_at,repo_id,pullrequest_number,pullrequest_title,pullrequest_text,author_login,author_id,merger_login,merger_id,identity_type_id)
				VALUES(%(created_at)s,
						%(merged_at)s,
						%(repo_id)s,
						%(pullrequest_number)s,
						%(pullrequest_title)s,
						%(pullrequest_text)s,
						%(author_login)s,
						(SELECT id FROM identities WHERE identity=%(author_login)s AND identity_type_id=(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)),
						%(merger_login)s,
						(SELECT id FROM identities WHERE identity=%(merger_login)s AND identity_type_id=(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)),
						(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)
						)

				ON CONFLICT DO NOTHING
				;''',(i for i in items_list if i['element_type']=='pullrequest'))
				# ;''',((s['created_at'],s['closed_at'],s['repo_id'],s['pullrequest_number'],s['pullrequest_title'],s['pullrequest_text'],s['author_login'],s['author_login'],self.target_identity_type,self.target_identity_type) for s in items_list))
		else:
			db.cursor.executemany('''
					INSERT OR IGNORE INTO pullrequests(created_at,merged_at,repo_id,pullrequest_number,pullrequest_title,pullrequest_text,author_login,author_id,merger_login,merger_id,identity_type_id)
				VALUES(:created_at,
						:merged_at,
						:repo_id,
						:pullrequest_number,
						:pullrequest_title,
						:pullrequest_text,
						:author_login,
						(SELECT id FROM identities WHERE identity=:merger_login AND identity_type_id=(SELECT id FROM identity_types WHERE name=:target_identity_type)),
						:merger_login,
						(SELECT id FROM identities WHERE identity=:author_login AND identity_type_id=(SELECT id FROM identity_types WHERE name=:target_identity_type)),
						(SELECT id FROM identity_types WHERE name=:target_identity_type)
						)
				;''',(i for i in items_list if i['element_type']=='pullrequest'))

		if commit:
			db.connection.commit()


		self.insert_reactions(items_list=items_list,commit=commit,db=db)
		self.insert_reaction_updates(items_list=items_list,commit=commit,db=db)

		self.insert_labels(items_list=items_list,commit=commit,db=db)
		self.insert_label_updates(items_list=items_list,commit=commit,db=db)

		self.insert_comments(items_list=items_list,commit=commit,db=db)
		self.insert_comment_updates(items_list=items_list,commit=commit,db=db)

		self.insert_comment_reactions(items_list=items_list,commit=commit,db=db)
		self.insert_comment_reaction_updates(items_list=items_list,commit=commit,db=db)


		if len(items_list):
			db.cursor.execute('''INSERT INTO full_updates(update_type) SELECT 'complete_pullrequests' ;''')

		if commit:
			db.connection.commit()

	def insert_reactions(self,items_list,commit=True,db=None):
		if db is None:
			db = self.db
		if db.db_type == 'postgres':
			extras.execute_batch(db.cursor,'''
				INSERT INTO pullrequest_reactions(created_at,repo_id,pullrequest_number,reaction,author_login,author_id,identity_type_id)
				VALUES(%(created_at)s,
						%(repo_id)s,
						%(pullrequest_number)s,
						%(pullrequest_reaction)s,
						%(author_login)s,
						(SELECT id FROM identities WHERE identity=%(author_login)s AND identity_type_id=(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)),
						(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)
						)

				ON CONFLICT DO NOTHING
				;''',(i for i in items_list if i['element_type']=='pullrequest_reaction'))

		else:
			db.cursor.executemany('''
					INSERT OR IGNORE INTO pullrequest_reactions(created_at,repo_id,pullrequest_number,reaction,author_login,author_id,identity_type_id)
				VALUES(:created_at,
						:repo_id,
						:pullrequest_number,
						:pullrequest_reaction,
						:author_login,
						(SELECT id FROM identities WHERE identity=:author_login AND identity_type_id=(SELECT id FROM identity_types WHERE name=:target_identity_type)),
						(SELECT id FROM identity_types WHERE name=:target_identity_type)
						)
				;''',(i for i in items_list if i['element_type']=='pullrequest_reaction'))


		if commit:
			db.connection.commit()

	def insert_comment_reactions(self,items_list,commit=True,db=None):
		if db is None:
			db = self.db
		if db.db_type == 'postgres':
			extras.execute_batch(db.cursor,'''
				INSERT INTO pullrequest_comment_reactions(created_at,repo_id,pullrequest_number,comment_id,reaction,author_login,author_id,identity_type_id)
				VALUES(%(created_at)s,
						%(repo_id)s,
						%(pullrequest_number)s,
						%(pullrequest_comment_id)s,
						%(pullrequest_comment_reaction)s,
						%(author_login)s,
						(SELECT id FROM identities WHERE identity=%(author_login)s AND identity_type_id=(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)),
						(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)
						)

				ON CONFLICT DO NOTHING
				;''',(i for i in items_list if i['element_type']=='pullrequest_comment_reaction'))

		else:
			db.cursor.executemany('''
					INSERT OR IGNORE INTO pullrequest_comment_reactions(created_at,repo_id,pullrequest_number,comment_id,reaction,author_login,author_id,identity_type_id)
				VALUES(:created_at,
						:repo_id,
						:pullrequest_number,
						:pullrequest_comment_id,
						:pullrequest_comment_reaction,
						:author_login,
						(SELECT id FROM identities WHERE identity=:author_login AND identity_type_id=(SELECT id FROM identity_types WHERE name=:target_identity_type)),
						(SELECT id FROM identity_types WHERE name=:target_identity_type)
						)
				;''',(i for i in items_list if i['element_type']=='pullrequest_comment_reaction'))


		if commit:
			db.connection.commit()

	def insert_comments(self,items_list,commit=True,db=None):
		if db is None:
			db = self.db
		if db.db_type == 'postgres':
			extras.execute_batch(db.cursor,'''
				INSERT INTO pullrequest_comments(created_at,repo_id,pullrequest_number,comment_id,comment_text,author_login,author_id,identity_type_id)
				VALUES(%(created_at)s,
						%(repo_id)s,
						%(pullrequest_number)s,
						%(pullrequest_comment_id)s,
						%(pullrequest_comment_text)s,
						%(author_login)s,
						(SELECT id FROM identities WHERE identity=%(author_login)s AND identity_type_id=(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)),
						(SELECT id FROM identity_types WHERE name=%(target_identity_type)s)
						)

				ON CONFLICT DO NOTHING
				;''',(i for i in items_list if i['element_type']=='pullrequest_comment'))

		else:
			db.cursor.executemany('''
					INSERT OR IGNORE INTO pullrequest_comments(created_at,repo_id,pullrequest_number,comment_id,comment_text,author_login,author_id,identity_type_id)
				VALUES(:created_at,
						:repo_id,
						:pullrequest_number,
						:pullrequest_comment_id,
						:pullrequest_comment_text,
						:author_login,
						(SELECT id FROM identities WHERE identity=:author_login AND identity_type_id=(SELECT id FROM identity_types WHERE name=:target_identity_type)),
						(SELECT id FROM identity_types WHERE name=:target_identity_type)
						)
				;''',(i for i in items_list if i['element_type']=='pullrequest_comment'))


		if commit:
			db.connection.commit()

	def insert_labels(self,items_list,commit=True,db=None):
		if db is None:
			db = self.db
		if db.db_type == 'postgres':
			extras.execute_batch(db.cursor,'''
				INSERT INTO pullrequest_labels(repo_id,pullrequest_number,label)
				VALUES(%(repo_id)s,
						%(pullrequest_number)s,
						%(pullrequest_label)s
						)

				ON CONFLICT DO NOTHING
				;''',(i for i in items_list if i['element_type']=='pullrequest_label'))

		else:
			db.cursor.executemany('''
					INSERT OR IGNORE INTO pullrequest_labels(repo_id,pullrequest_number,label)
				VALUES(:repo_id,
						:pullrequest_number,
						:pullrequest_label
						)
				;''',(i for i in items_list if i['element_type']=='pullrequest_label'))


		if commit:
			db.connection.commit()


	def insert_reaction_updates(self,items_list,commit=True,db=None):
		if db is None:
			db = self.db

		for i in items_list:
			if i['element_type'] == 'pullrequest':
				update_info = {'pullrequest_id':i['pullrequest_number'],
								'pullrequest_gql_id':i['pullrequest_gql_id']
								}
				if i['reactions_pageinfo']['hasNextPage']:
					success = None
					update_info['end_cursor'] = i['reactions_pageinfo']['endCursor']
					db.insert_update(table='pullrequest_reactions',repo_id=i['repo_id'],success=success,info=update_info)

		if commit:
			db.connection.commit()

	def insert_label_updates(self,items_list,commit=True,db=None):
		if db is None:
			db = self.db

		for i in items_list:
			if i['element_type'] == 'pullrequest':
				update_info = {'pullrequest_id':i['pullrequest_number'],
								'pullrequest_gql_id':i['pullrequest_gql_id']
								}
				if i['labels_pageinfo']['hasNextPage']:
					success = None
					update_info['end_cursor'] = i['labels_pageinfo']['endCursor']
					db.insert_update(table='pullrequest_labels',repo_id=i['repo_id'],success=success,info=update_info)

		if commit:
			db.connection.commit()

	def insert_comment_updates(self,items_list,commit=True,db=None):
		if db is None:
			db = self.db

		for i in items_list:
			if i['element_type'] == 'pullrequest':
				update_info = {'pullrequest_id':i['pullrequest_number'],
								'pullrequest_gql_id':i['pullrequest_gql_id']
								}
				if i['comments_pageinfo']['hasNextPage']:
					success = None
					update_info['end_cursor'] = i['comments_pageinfo']['endCursor']
					db.insert_update(table='pullrequest_comments',repo_id=i['repo_id'],success=success,info=update_info)

		if commit:
			db.connection.commit()

	def insert_comment_reaction_updates(self,items_list,commit=True,db=None):
		if db is None:
			db = self.db

		for i in items_list:
			if i['element_type'] == 'pullrequest_comment':
				update_info = {'pullrequest_comment_id':i['pullrequest_comment_id'],
								'pullrequest_comment_gql_id':i['pullrequest_comment_gql_id']
								}
				if i['comment_reactions_pageinfo']['hasNextPage']:
					success = None
					update_info['end_cursor'] = i['comment_reactions_pageinfo']['endCursor']
					db.insert_update(table='pullrequest_comment_reactions',repo_id=i['repo_id'],success=success,info=update_info)

		if commit:
			db.connection.commit()


class PRReactionsGQLFiller(GHGQLFiller):
	def __init__(self,**kwargs):
		self.items_name = 'pullrequest_reactions'
		self.queried_obj = 'repo'
		self.sub_queried_obj = 'pullrequest'
		self.pageinfo_path = ['node','reactions','pageInfo']
		GHGQLFiller.__init__(self,**kwargs)

	def check_requirements(self):
		self.db.cursor.execute('''
			SELECT update_type,updated_at FROM full_updates
			WHERE update_type='complete_pullrequests'
			ORDER BY updated_at DESC
			LIMIT 1
			;
			''')
		ans = list(self.db.cursor.fetchall())
		if len(ans) == 0:
			self.logger.info('missing complete_pullrequests filler, necessary prior to pullrequest_reactions')
			return False
		else:
			return True

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
					repository(owner: "{repo_owner}", name: "{repo_name}") {{
  							nameWithOwner
    						id }}
  					node(id:"{pullrequest_gql_id}") {{
  					    ... on PullRequest {{

									id
									number
									reactions(first:{page_size}  {after_end_cursor}) {{
										totalCount
										pageInfo {{
											hasNextPage
											endCursor
											}}
										nodes {{
											createdAt
											user {{ login }}
											content
											id
											}}
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
			repo_owner,repo_name = query_result['repository']['nameWithOwner'].split('/')
		for e in [query_result['node']]:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'pullrequest'}
			try:
				d['pullrequest_gql_id'] = e['id']
				d['pullrequest_number'] = e['number']
				d['pullrequest_id'] = e['number']
				
			except (KeyError,TypeError) as err:
				self.logger.info('Result triggering error: {} \nError when parsing pullrequests for {}/{}: {}'.format(e,repo_owner,repo_name,err))
				continue
			else:
				ans.append(d)
				if e['reactions']['totalCount']>0:
					for ee in e['reactions']['nodes']:
						r = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'pullrequest_reaction'}
						r['pullrequest_id'] = d['pullrequest_id']
						r['pullrequest_number'] = d['pullrequest_id']
						r['pullrequest_gql_id'] = d['pullrequest_gql_id']
						try:
							r['created_at'] = ee['createdAt']
							r['pullrequest_reaction'] = ee['content']
							try:
								r['author_login'] = ee['user']['login']
							except:
								# r['author_login'] = None
								r['author_login'] = 'nullauthor_'+ee['id']
						except (KeyError,TypeError) as err:
							self.logger.info('Result triggering error: {} \nError when parsing pullrequest_reactions for {}/{}: {}'.format(e,repo_owner,repo_name,err))
							continue
						else:
							ans.append(r)
		return ans

	def insert_items(self,**kwargs):
		CompletePullRequestsGQLFiller.insert_reactions(self,**kwargs)


	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['node']['reactions']['totalCount']


class PRCommentsGQLFiller(GHGQLFiller):
	def __init__(self,**kwargs):
		self.items_name = 'pullrequest_comments'
		self.queried_obj = 'repo'
		self.sub_queried_obj = 'pullrequest'
		self.pageinfo_path = ['node','comments','pageInfo']
		GHGQLFiller.__init__(self,**kwargs)

	def check_requirements(self):
		self.db.cursor.execute('''
			SELECT update_type,updated_at FROM full_updates
			WHERE update_type='complete_pullrequests'
			ORDER BY updated_at DESC
			LIMIT 1
			;
			''')
		ans = list(self.db.cursor.fetchall())
		if len(ans) == 0:
			self.logger.info('missing complete_pullrequests filler, necessary prior to pullrequest_comments')
			return False
		else:
			return True

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
					repository(owner: "{repo_owner}", name: "{repo_name}") {{
  							nameWithOwner
    						id }}
  					node(id:"{pullrequest_gql_id}") {{
  					    ... on PullRequest {{

									id
									number
									comments(first:{page_size} {after_end_cursor}) {{
										totalCount
										pageInfo {{
											endCursor
											hasNextPage
											}}
										nodes {{
											id
											createdAt
											author {{ login }}
											bodyText
											databaseId

											reactions(first:{secondary_page_size}) {{
												totalCount
												pageInfo {{
													endCursor
													hasNextPage
													}}
												nodes {{
													user {{ login }}
													createdAt
													content
													id
													}}
												}}
											}}
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
			repo_owner,repo_name = query_result['repository']['nameWithOwner'].split('/')
		for e in [query_result['node']]:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'pullrequest'}
			try:
				d['pullrequest_gql_id'] = e['id']
				d['pullrequest_number'] = e['number']
				d['pullrequest_id'] = e['number']
				
			except (KeyError,TypeError) as err:
				self.logger.info('Result triggering error: {} \nError when parsing pullrequests for {}/{}: {}'.format(e,repo_owner,repo_name,err))
				continue
			else:
				ans.append(d)
				if e['comments']['totalCount']>0:
					for ee in e['comments']['nodes']:
						r = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'pullrequest_comment'}
						r['pullrequest_number'] = d['pullrequest_number']
						r['pullrequest_gql_id'] = d['pullrequest_gql_id']
						try:
							r['created_at'] = ee['createdAt']
							r['pullrequest_comment_text'] = ee['bodyText']
							r['pullrequest_comment_id'] = ee['databaseId']
							r['pullrequest_comment_gql_id'] = ee['id']
							r['comment_reactions_pageinfo'] = ee['reactions']['pageInfo']
							try:
								r['author_login'] = ee['author']['login']
							except:
								r['author_login'] = None


						except (KeyError,TypeError) as err:
							self.logger.info('Result triggering error: {} \nError when parsing pullrequest_comments for {}/{}: {}'.format(e,repo_owner,repo_name,err))
							continue
						else:
							ans.append(r)

						if ee['reactions']['totalCount']>0:
							for eee in ee['reactions']['nodes']:
								r = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'pullrequest_comment_reaction'}
								r['pullrequest_number'] = d['pullrequest_number']
								r['pullrequest_gql_id'] = d['pullrequest_gql_id']
								r['pullrequest_comment_id'] = ee['databaseId']
								r['pullrequest_comment_gql_id'] = ee['id']
								try:
									r['created_at'] = eee['createdAt']
									r['pullrequest_comment_reaction'] = eee['content']
									try:
										r['author_login'] = eee['user']['login']
									except:
										# r['author_login'] = None
										r['author_login'] = 'nullauthor_'+eee['id']
								except (KeyError,TypeError) as err:
									self.logger.info('Result triggering error: {} \nError when parsing pullrequest_comment_reactions for {}/{}: {}'.format(e,repo_owner,repo_name,err))
									continue
								else:
									ans.append(r)
		return ans

	def insert_items(self,db=None,**kwargs):

		if db is None:
			db = self.db

		CompletePullRequestsGQLFiller.insert_comments(self,db=db,**kwargs)
		CompletePullRequestsGQLFiller.insert_comment_reactions(self,db=db,**kwargs)
		CompletePullRequestsGQLFiller.insert_comment_reaction_updates(self,db=db,**kwargs)

		if len(kwargs['items_list']):
			db.cursor.execute('''INSERT INTO full_updates(update_type) SELECT 'pullrequest_comments' ;''')

	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['node']['comments']['totalCount']


class PRCommentReactionsGQLFiller(GHGQLFiller):
	def __init__(self,**kwargs):
		self.items_name = 'pullrequest_comment_reactions'
		self.queried_obj = 'repo'
		self.sub_queried_obj = 'pullrequest_comment'
		self.pageinfo_path = ['node','reactions','pageInfo']
		GHGQLFiller.__init__(self,**kwargs)

	def check_requirements(self):
		self.db.cursor.execute('''
			SELECT update_type,updated_at FROM full_updates
			WHERE update_type='complete_pullrequests'
			ORDER BY updated_at DESC
			LIMIT 1
			;
			''')
		ans = list(self.db.cursor.fetchall())
		if len(ans) == 0:
			self.logger.info('missing complete_pullrequests filler, necessary prior to pullrequest_reactions')
			return False

		self.db.cursor.execute('''
			SELECT update_type,updated_at FROM full_updates
			WHERE update_type='pullrequest_comments'
			ORDER BY updated_at DESC
			LIMIT 1
			;
			''')
		ans = list(self.db.cursor.fetchall())
		if len(ans) == 0:
			self.logger.info('missing pullrequest_comments filler, necessary prior to pullrequest_reactions')
			return False
		else:
			return True

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
					repository(owner: "{repo_owner}", name: "{repo_name}") {{
  							nameWithOwner
    						id }}
  					node(id:"{pullrequest_comment_gql_id}") {{
  					    ... on IssueComment {{
  					    			pullRequest {{ number }}
									id
									databaseId
									reactions(first:{page_size}  {after_end_cursor}) {{
										totalCount
										pageInfo {{
											hasNextPage
											endCursor
											}}
										nodes {{
											createdAt
											user {{ login }}
											content
											id
											}}
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
			repo_owner,repo_name = query_result['repository']['nameWithOwner'].split('/')
		for e in [query_result['node']]:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'pullrequest_comment'}
			try:
				# d['pullrequest_gql_id'] = e['id']
				d['pullrequest_number'] = e['pullRequest']['number']
				# d['pullrequest_id'] = e['number']
				d['pullrequest_comment_id'] = e['databaseId']
				d['pullrequest_comment_gql_id'] = e['id']
				d['comment_reactions_pageinfo'] = e['reactions']['pageInfo']
				
			except (KeyError,TypeError) as err:
				self.logger.info('Result triggering error: {} \nError when parsing pullrequest_comments for {}/{}: {}'.format(e,repo_owner,repo_name,err))
				continue
			else:
				ans.append(d)
				
				if e['reactions']['totalCount']>0:
					for ee in e['reactions']['nodes']:
						r = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'pullrequest_comment_reaction'}
						r['pullrequest_number'] = d['pullrequest_number']
						r['pullrequest_comment_id'] = e['databaseId']
						r['pullrequest_comment_gql_id'] = e['id']
						try:
							r['created_at'] = ee['createdAt']
							r['pullrequest_comment_reaction'] = ee['content']
							try:
								r['author_login'] = ee['user']['login']
							except:
								# r['author_login'] = None
								r['author_login'] = 'nullauthor_'+ee['id']
						except (KeyError,TypeError) as err:
							self.logger.info('Result triggering error: {} \nError when parsing pullrequest_comment_reactions for {}/{}: {}'.format(e,repo_owner,repo_name,err))
							continue
						else:
							ans.append(r)
		return ans

	def insert_items(self,**kwargs):
		CompletePullRequestsGQLFiller.insert_comment_reactions(self,**kwargs)


	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['node']['reactions']['totalCount']


class PRLabelsGQLFiller(GHGQLFiller):
	def __init__(self,**kwargs):
		self.items_name = 'pullrequest_labels'
		self.queried_obj = 'repo'
		self.sub_queried_obj = 'pullrequest'
		self.pageinfo_path = ['node','labels','pageInfo']
		GHGQLFiller.__init__(self,**kwargs)

	def check_requirements(self):
		self.db.cursor.execute('''
			SELECT update_type,updated_at FROM full_updates
			WHERE update_type='complete_pullrequests'
			ORDER BY updated_at DESC
			LIMIT 1
			;
			''')
		ans = list(self.db.cursor.fetchall())
		if len(ans) == 0:
			self.logger.info('missing complete_pullrequests filler, necessary prior to pullrequest_labels')
			return False
		else:
			return True

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
					repository(owner: "{repo_owner}", name: "{repo_name}") {{
  							nameWithOwner
    						id }}
  					node(id:"{pullrequest_gql_id}") {{
  					    ... on PullRequest {{

									id
									number
									
									labels(first:{page_size} {after_end_cursor}) {{
										totalCount
										pageInfo {{
											endCursor
											hasNextPage
											}}
										nodes {{
											name
											createdAt
											}}
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
			repo_owner,repo_name = query_result['repository']['nameWithOwner'].split('/')
		for e in [query_result['node']]:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name,'target_identity_type':self.target_identity_type,'element_type':'pullrequest'}
			try:
				d['pullrequest_gql_id'] = e['id']
				d['pullrequest_number'] = e['number']
				d['pullrequest_id'] = e['number']
				
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
							r['pullrequest_label'] = ee['name']
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

	def insert_items(self,**kwargs):
		CompletePullRequestsGQLFiller.insert_labels(self,**kwargs)


	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['node']['labels']['totalCount']



class SponsorablesGQLFiller(GHGQLFiller):
	'''
	Querying sponsorable logins through the GraphQL API
	'''
	def __init__(self,**kwargs):
		self.items_name = 'sponsorable'
		self.queried_obj = None
		self.pageinfo_path = ['sponsorables','pageInfo']
		GHGQLFiller.__init__(self,**kwargs)

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
  					sponsorables(first:{page_size} {after_end_cursor}) {{
  						totalCount
  						pageInfo{{
  							hasNextPage
  							endCursor
  							}}
  						nodes{{
  							__typename
  							... on User{{
  								login
  								sponsorsListing{{
  										createdAt
  									}}
  								}}
  							}}
  						}}
					}}'''

	def parse_query_result(self,query_result,**kwargs):
		'''
		In subclasses this has to be implemented
		output: [{'login':lo}]
		'''
		# ans = [{'login':qr['login'],'sponsorsListing_createdat':qr['sponsorsListing']['createdAt']} for qr in query_result['sponsorables']['nodes'] if qr['__typename']=='User']
		ans = []
		for qr in query_result['sponsorables']['nodes']:
			if qr['__typename']=='User':
				try:
					elt = {'identity_type':'github_login','login':qr['login'],'sponsorsListing_createdat':qr['sponsorsListing']['createdAt']}
				except KeyError:
					elt = {'identity_type':'github_login','login':qr['login'],'sponsorsListing_createdat':None}
				ans.append(elt)
		return ans

	def insert_items(self,items_list,commit=True,db=None):
		'''
		In subclasses this has to be implemented
		inserts results in the DB
		'''
		if db is None:
			db = self.db
		if db.db_type == 'postgres':
			extras.execute_batch(db.cursor,''' INSERT INTO users(creation_identity_type_id,creation_identity) VALUES(
												(SELECT id FROM identity_types WHERE name=%s),
												%s
												) ON CONFLICT DO NOTHING;''',((self.target_identity_type,item['login'],) for item in items_list))

			extras.execute_batch(db.cursor,''' INSERT INTO identities(identity_type_id,user_id,identity)
													VALUES((SELECT id FROM identity_types WHERE name=%s),
															(SELECT id FROM users
															WHERE creation_identity_type_id=(SELECT id FROM identity_types WHERE name=%s)
																AND creation_identity=%s),
															%s)
													ON CONFLICT DO NOTHING;''',((self.target_identity_type,self.target_identity_type,item['login'],item['login'],) for item in items_list))

			extras.execute_batch(db.cursor,''' INSERT INTO sponsors_listings(identity_type_id,login,created_at)
													VALUES((SELECT id FROM identity_types WHERE name=%(identity_type)s),
															%(login)s,
															%(sponsorsListing_createdat)s)
													ON CONFLICT DO NOTHING;''',items_list)

		else:

			db.cursor.executemany(''' INSERT OR IGNORE INTO users(creation_identity_type_id,creation_identity) VALUES(
												(SELECT id FROM identity_types WHERE name=?),
												?
												);''',((self.target_identity_type,item['login'],) for item in items_list))

			db.cursor.executemany(''' INSERT OR IGNORE INTO identities(identity_type_id,user_id,identity)
													VALUES((SELECT id FROM identity_types WHERE name=?),
															(SELECT id FROM users
															WHERE creation_identity_type_id=(SELECT id FROM identity_types WHERE name=?)
																AND creation_identity=?),
															?);''',((self.target_identity_type,self.target_identity_type,item['login'],item['login'],) for item in items_list))

			db.cursor.executemany(''' INSERT OR IGNORE INTO sponsors_listings(identity_type_id,login,created_at)
													VALUES((SELECT id FROM identity_types WHERE name=:identity_type),
															:login,
															:sponsorsListing_createdat)
													;''',items_list)

		if commit:
			db.connection.commit()


	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['sponsorables']['totalCount']

	def set_element_list(self):
		'''
		[None],[] or [end_cursor]

		'''
		if self.db.db_type == 'postgres':
			if self.force:
				self.db.cursor.execute('''
						SELECT DISTINCT
 							FIRST_VALUE(tu.info->> 'end_cursor') OVER (PARTITION BY tu.table_name,tu.success ORDER BY tu.updated_at DESC) AS end_cursor
							,FIRST_VALUE(tu.updated_at) OVER (PARTITION BY tu.table_name,tu.success ORDER BY tu.updated_at DESC) AS updated_at
							,FIRST_VALUE(tu.table_name) OVER (PARTITION BY tu.table_name,tu.success ORDER BY tu.updated_at DESC) AS table_name
							,FIRST_VALUE(tu.success) OVER (PARTITION BY tu.table_name,tu.success ORDER BY tu.updated_at DESC) AS success
						FROM table_updates tu
						WHERE table_name=%(table_name)s
				;''',{'table_name':self.items_name})

				ans = self.db.cursor.fetchone()
				if ans is None:
					self.elt_list = [None]
				else:
					end_cursor,upd_at,t_name,succ = ans
					self.elt_list = [end_cursor]

			elif self.retry:
				self.elt_list = [None]
			else:
				self.db.cursor.execute('''
						SELECT DISTINCT
 							FIRST_VALUE(tu.info->> 'end_cursor') OVER (PARTITION BY tu.table_name,tu.success ORDER BY tu.updated_at DESC) AS end_cursor
							,FIRST_VALUE(tu.updated_at) OVER (PARTITION BY tu.table_name,tu.success ORDER BY tu.updated_at DESC) AS updated_at
							,FIRST_VALUE(tu.table_name) OVER (PARTITION BY tu.table_name,tu.success ORDER BY tu.updated_at DESC) AS table_name
							,FIRST_VALUE(tu.success) OVER (PARTITION BY tu.table_name,tu.success ORDER BY tu.updated_at DESC) AS success
						FROM table_updates tu
						WHERE table_name=%(table_name)s
				;''',{'table_name':self.items_name})

			ans = self.db.cursor.fetchone()
			if ans is None:
				self.elt_list = [None]
			else:
				end_cursor,upd_at,t_name,succ = ans
				if succ is True or succ is False:
					self.elt_list = []
				else:
					self.elt_list = [end_cursor]

		else:
			if self.force:
				self.db.cursor.execute('''
						SELECT DISTINCT
 							FIRST_VALUE(tu.info) OVER (PARTITION BY tu.table_name,tu.success ORDER BY tu.updated_at DESC) AS end_cursor
							,FIRST_VALUE(tu.updated_at) OVER (PARTITION BY tu.table_name,tu.success ORDER BY tu.updated_at DESC) AS updated_at
							,FIRST_VALUE(tu.table_name) OVER (PARTITION BY tu.table_name,tu.success ORDER BY tu.updated_at DESC) AS table_name
							,FIRST_VALUE(tu.success) OVER (PARTITION BY tu.table_name,tu.success ORDER BY tu.updated_at DESC) AS success
						FROM table_updates tu
						WHERE table_name=:table_name
				;''',{'table_name':self.items_name})

				ans = self.db.cursor.fetchone()
				if ans is None:
					self.elt_list = [None]
				else:
					end_cursor_info,upd_at,t_name,succ = ans
					if end_cursor_info is None:
						self.elt_list = [None]
					else:
						self.elt_list = [json.loads(end_cursor_info)['end_cursor']]
			elif self.retry:
				self.elt_list = [None]
			else:
				self.db.cursor.execute('''
						SELECT DISTINCT
 							FIRST_VALUE(tu.info) OVER (PARTITION BY tu.table_name,tu.success ORDER BY tu.updated_at DESC) AS end_cursor
							,FIRST_VALUE(tu.updated_at) OVER (PARTITION BY tu.table_name,tu.success ORDER BY tu.updated_at DESC) AS updated_at
							,FIRST_VALUE(tu.table_name) OVER (PARTITION BY tu.table_name,tu.success ORDER BY tu.updated_at DESC) AS table_name
							,FIRST_VALUE(tu.success) OVER (PARTITION BY tu.table_name,tu.success ORDER BY tu.updated_at DESC) AS success
						FROM table_updates tu
						WHERE table_name=:table_name
				;''',{'table_name':self.items_name})

			ans = self.db.cursor.fetchone()
			if ans is None:
				self.elt_list = [None]
			else:
				end_cursor_info,upd_at,t_name,succ = ans
				if succ is True or succ is False:
					self.elt_list = []
				elif end_cursor_info is None:
					self.elt_list = [None]
				else:
					self.elt_list = [json.loads(end_cursor_info)['end_cursor']]


	def fill_items(self,elt_list=None,workers=1,in_thread=False,incremental_update=True,elt_nb=None,total_elt=None):
		'''
		Main loop to batch query on list of repos or users
		'''

		db = self.db
		if elt_list is None:
			elt_list = self.elt_list

		elt_list = copy.deepcopy(elt_list)
		if len(elt_list):
			end_cursor = elt_list[0]
			try:
				requester_gen = self.get_requester(in_thread=in_thread)
				self.logger.info('Filling {}'.format(self.items_name))
				requester = next(requester_gen)

				params = {'after_end_cursor':end_cursor,'page_size':self.get_page_size(),'secondary_page_size':self.secondary_page_size}
				params.update(self.additional_query_attributes())
				paginated_query = requester.paginated_query(gql_query=self.query_string(),params=params,pageinfo_path=self.pageinfo_path)
				try:
					result,pageinfo = next(paginated_query)
				except asyncio.TimeoutError as e:
					if self.retry_fails_permanent:
						err_text = 'Timeout threshold reached {}, marking query as to be discarded {}: {}'.format(requester.retries,e.__class__,e)
						self.logger.error(err_text)
						db.log_error(err_text)
						self.insert_update(db=db,success=False)
						elt_list.pop(0)
						return
					else:
						raise

				# detect 0 elts
				parsed_result = self.parse_query_result(result,)
				if len(parsed_result) == 0:
					self.logger.info('No new {}'.format(self.items_name,))
					if end_cursor is None:
						end_cursor_json = None
					else:
						end_cursor_json = json.dumps({'end_cursor':end_cursor})
					self.insert_update(db=db,success=True,info=end_cursor_json)
				else:
					# loop
					while pageinfo['hasNextPage']:
						# raise ValueError
						while requester.get_rate_limit()>self.querymin_threshold and pageinfo['hasNextPage']:
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
								self.insert_update(db=db,success=True,info=end_cursor_json,autocommit=True)
								nb_items = self.get_nb_items(result)
								if nb_items is not None:
									self.logger.info('Filled {}: {}'.format(self.items_name,nb_items))
								else:
									self.logger.info('Filled {}'.format(self.items_name,))
							else:

								# insert partial update with endcursor value and success NULL (?)
								self.insert_update(db=db,success=None,info=end_cursor_json)
								db.connection.commit()
								# continue query
								try:
									result,pageinfo = next(paginated_query)
								except asyncio.TimeoutError as e:
									if self.retry_fails_permanent:
										err_text = 'Timeout threshold reached {}, marking query as to be discarded {}: {}'.format(requester.retries,e.__class__,e)
										self.logger.error(err_text)
										db.log_error(err_text)
										self.insert_update(db=db,success=True,info=end_cursor_json)
										elt_list.pop(0)
										break
									else:
										raise
								parsed_result = self.parse_query_result(result,)
						if pageinfo['hasNextPage']:
							requester = next(requester_gen)

			except Exception as e:
				db.log_error('Exception in {}: \n {}: {}'.format(self.items_name,e.__class__.__name__,e))
				raise Exception('Exception in {}'.format(self.items_name)) from e


class RepoLanguagesGQLFiller(GHGQLFiller):
	'''
	Querying repository languages through the GraphQL API
	'''
	def __init__(self,reset_shares=False,**kwargs):
		self.items_name = 'repo_languages'
		self.queried_obj = 'repo'
		self.pageinfo_path = ['repository','languages','pageInfo']
		self.reset_shares = reset_shares
		GHGQLFiller.__init__(self,**kwargs)

	def apply(self):
		GHGQLFiller.apply(self)
		self.compute_shares()

	def compute_shares(self):
		if self.reset_shares:
			self.db.cursor.execute('''
				UPDATE repo_languages SET share=NULL;
				;''')

		if self.db.db_type == 'postgres':
			self.db.cursor.execute('''
				WITH repo_list AS (SELECT DISTINCT repo_id FROM repo_languages WHERE share IS NULL),
					shares AS (SELECT r.repo_id,rl.language,rl.size::DOUBLE PRECISION /(SUM(rl.size::DOUBLE PRECISION ) OVER (PARTITION BY r.repo_id)) AS share FROM repo_list r
								INNER JOIN repo_languages rl
								ON rl.repo_id=r.repo_id
								)
				UPDATE repo_languages
				SET share=s.share
				FROM shares s
				WHERE s.repo_id=repo_languages.repo_id AND s.language=repo_languages.language
				;''')
		else:
			self.db.cursor.execute('''
				WITH repo_list AS (SELECT DISTINCT repo_id FROM repo_languages WHERE share IS NULL),
					shares AS (SELECT r.repo_id,rl.language,rl.size*1.0/(SUM(rl.size*1.0) OVER (PARTITION BY r.repo_id)) AS share FROM repo_list r
								INNER JOIN repo_languages rl
								ON rl.repo_id=r.repo_id
								)
				UPDATE repo_languages
				SET share= (SELECT s.share FROM shares s
							WHERE s.repo_id=repo_languages.repo_id AND s.language=repo_languages.language)
				WHERE EXISTS (SELECT s.share FROM shares s
							WHERE s.repo_id=repo_languages.repo_id AND s.language=repo_languages.language)
				;''')

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
  					repository(owner: "{repo_owner}", name: "{repo_name}") {{
  							nameWithOwner
    						id
    						languages(first:{page_size} {after_end_cursor},orderBy:{{field:SIZE,direction:DESC}}) {{
    							totalCount
    							pageInfo {{
									endCursor
									hasNextPage
						 			}}
      							edges {{
        							size
									node {{
										name
										}}
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
			repo_owner,repo_name = query_result['repository']['nameWithOwner'].split('/')
		for e in query_result['repository']['languages']['edges']:
			d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name}
			try:
				d['language_size'] = e['size']
				d['language_name'] = e['node']['name']
			except (KeyError,TypeError) as err:
				self.logger.info('Result triggering error: {} \nError when parsing languages for {}/{}: {}'.format(e,repo_owner,repo_name,err))
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
				INSERT INTO repo_languages(repo_id,language,size)
				VALUES(%s,
						%s,
						%s
						)

				ON CONFLICT DO NOTHING
				;''',((s['repo_id'],s['language_name'],s['language_size']) for s in items_list))
		else:
			db.cursor.executemany('''
					INSERT OR IGNORE INTO repo_languages(repo_id,language,size)
					VALUES(?,
							?,
							?
							)
				;''',((s['repo_id'],s['language_name'],s['language_size']) for s in items_list))


		if commit:
			db.connection.commit()

	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['repository']['languages']['totalCount']

class RepoCreatedAtGQLFiller(GHGQLFiller):
	'''
	Querying creation dates through the GraphQL API
	'''
	def __init__(self,**kwargs):
		self.items_name = 'repo_createdat'
		self.queried_obj = 'repo'
		self.pageinfo_path = None
		GHGQLFiller.__init__(self,**kwargs)

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
					repository(owner:"{repo_owner}", name:"{repo_name}") {{
						nameWithOwner
						createdAt
					}}
				}}'''

	def parse_query_result(self,query_result,repo_id,identity_id,repo_owner=None,repo_name=None,**kwargs):
		'''
		In subclasses this has to be implemented
		output: [ {'repo_id':r_id,'repo_owner':r_ow,'repo_name':r_na,'created_at':c_at} , ...]
		'''
		ans = []
		if repo_owner is None:
			repo_owner,repo_name = query_result['repository']['nameWithOwner'].split('/')
		d = {'repo_id':repo_id,'repo_owner':repo_owner,'repo_name':repo_name}
		try:
			d['created_at'] = query_result['repository']['createdAt']
		except (KeyError,TypeError) as err:
			self.logger.info('Error when parsing creation date for {}/{}: {}'.format(repo_owner,repo_name,err))
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
				UPDATE repositories SET created_at=%(created_at)s
				WHERE id=%(repo_id)s
				;''',(s for s in items_list))
		else:
			db.cursor.executemany('''
					UPDATE repositories SET created_at=date(:created_at)
				WHERE id=:repo_id
						;''',(s for s in items_list))

		if commit:
			db.connection.commit()

	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return 1



class UserCreatedAtGQLFiller(GHGQLFiller):
	'''
	Querying creation dates through the GraphQL API
	'''
	def __init__(self,**kwargs):
		self.items_name = 'user_createdat'
		self.queried_obj = 'user'
		self.pageinfo_path = None
		GHGQLFiller.__init__(self,**kwargs)

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
					user(login:"{user_login}") {{
						login
						createdAt
					}}
				}}'''

	def parse_query_result(self,query_result,identity_id,identity_type_id,**kwargs):
		'''
		In subclasses this has to be implemented
		output: [ {'repo_id':r_id,'repo_owner':r_ow,'repo_name':r_na,'starrer_login':s_lo,'starred_at':st_at} , ...]
		'''
		ans = []
		user_login = query_result['user']['login']
		d = {'identity_id':identity_id,'user_login':user_login,'identity_type_id':identity_type_id}
		try:
			d['created_at'] = query_result['user']['createdAt']
		except (KeyError,TypeError) as err:
			self.logger.info('KeyError when parsing creation date for {}: {}'.format(user_login,err))
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
				UPDATE identities SET created_at=%(created_at)s
				WHERE id=%(identity_id)s
				AND identity_type_id=%(identity_type_id)s
				;''',(s for s in items_list))
		else:
			db.cursor.executemany('''
					UPDATE identities SET created_at=date(:created_at)
				WHERE id=:identity_id
				AND identity_type_id=:identity_type_id
						;''',(s for s in items_list))

		if commit:
			db.connection.commit()

	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return 1


class UserOrgsGQLFiller(GHGQLFiller):
	'''
	Querying organizations through the GraphQL API
	'''

	scopes = ('read:user','read:org',)

	def __init__(self,**kwargs):
		self.items_name = 'user_orgs'
		self.queried_obj = 'user'
		self.pageinfo_path = ('user','organizations','pageInfo')
		GHGQLFiller.__init__(self,**kwargs)

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
					user(login:"{user_login}") {{
						login
						organizations(first:{page_size} {after_end_cursor}) {{
							pageInfo {{ endCursor hasNextPage }}
							totalCount
							edges {{
								node {{ 
									login
									name
									description
									id
									createdAt }}
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
		d = {'identity_id':identity_id,'user_login':user_login,'identity_type_id':identity_type_id,'source':self.source_name}
		for e in query_result['user']['organizations']['edges']:
			try:
				d['created_at'] = e['node']['createdAt']
				d['name'] = e['node']['name']
				d['login'] = e['node']['login']
				d['description'] = e['node']['description']
			except (KeyError,TypeError) as err:
				self.logger.info('KeyError when parsing organizations for {}: {}'.format(user_login,err))
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
				INSERT INTO organizations(name,login,description,created_at,source)
				SELECT %(name)s,%(login)s,%(description)s,%(created_at)s,(SELECT id FROM sources WHERE name=%(source)s) 
				WHERE NOT EXISTS (SELECT o.id FROM organizations o INNER JOIN sources s ON s.name=%(source)s AND s.id=o.source AND o.login=%(login)s)
				;''',(s for s in items_list))
			extras.execute_batch(db.cursor,'''
				INSERT INTO org_memberships(member,org_id)
				SELECT %(identity_id)s,o.id
				FROM organizations o INNER JOIN sources s ON s.name=%(source)s AND s.id=o.source AND o.login=%(login)s
				ON CONFLICT DO NOTHING
				;''',(s for s in items_list))
		else:
			db.cursor.executemany('''
				INSERT INTO organizations(name,login,description,created_at,source)
				SELECT :name,:login,:description,:created_at,(SELECT id FROM sources WHERE name=:source)
				WHERE NOT EXISTS (SELECT o.id FROM organizations o INNER JOIN sources s ON s.name=:source AND s.id=o.source AND o.login=:login)
				;''',(s for s in items_list))
			db.cursor.executemany('''
				INSERT OR IGNORE INTO org_memberships(member,org_id)
				SELECT :identity_id,o.id
				FROM organizations o INNER JOIN sources s ON s.name=:source AND s.id=o.source AND o.login=:login
				;''',(s for s in items_list))

		if commit:
			db.connection.commit()

	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		return query_result['user']['organizations']['totalCount']

class SingleQueryUserLanguagesGQLFiller(GHGQLFiller):
	'''
	Querying languages of repositories of users through the GraphQL API
	'''
	def __init__(self,reset_shares=False,**kwargs):
		self.items_name = 'user_languages'
		self.queried_obj = 'user'
		self.pageinfo_path = None
		self.reset_shares = reset_shares
		GHGQLFiller.__init__(self,**kwargs)

	def apply(self):
		GHGQLFiller.apply(self)
		self.compute_shares()

	def compute_shares(self):
		if self.reset_shares:
			self.db.cursor.execute('''
				UPDATE user_languages SET share=NULL;
				;''')

		if self.db.db_type == 'postgres':
			self.db.cursor.execute('''
				WITH user_list AS (SELECT DISTINCT user_identity FROM user_languages WHERE share IS NULL),
					shares AS (SELECT r.user_identity,rl.language,rl.size::DOUBLE PRECISION /(SUM(rl.size::DOUBLE PRECISION) OVER (PARTITION BY r.user_identity)) AS share FROM user_list r
								INNER JOIN user_languages rl
								ON rl.user_identity=r.user_identity
								)
				UPDATE user_languages
				SET share=s.share
				FROM shares s
				WHERE s.user_identity=user_languages.user_identity AND s.language=user_languages.language
				;''')
		else:
			self.db.cursor.execute('''
				WITH user_list AS (SELECT DISTINCT user_identity FROM user_languages WHERE share IS NULL),
					shares AS (SELECT r.user_identity,rl.language,rl.size*1./(SUM(rl.size) OVER (PARTITION BY r.user_identity)) AS share FROM user_list r
								INNER JOIN user_languages rl
								ON rl.user_identity=r.user_identity
								)
				UPDATE user_languages
				SET share= (SELECT s.share FROM shares s
							WHERE s.user_identity=user_languages.user_identity AND s.language=user_languages.language)
				WHERE EXISTS (SELECT s.share FROM shares s
							WHERE s.user_identity=user_languages.user_identity AND s.language=user_languages.language)
				;''')

	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		return '''query {{
  					user(login: "{user_login}") {{
  							login
  							contributionsCollection {{
      							totalCommitContributions
      							commitContributionsByRepository {{
        							contributions{{
        								totalCount
        								}}
       								repository{{
       									nameWithOwner
      									languages(first:{page_size}){{
      										totalSize
        									edges{{
        										size
        										node {{
        											name}}
        										}}
        									}}
        								}}
        							}}
        						}}
  							}}
						}}'''



	def parse_query_result(self,query_result,identity_id,identity_type_id,**kwargs):
		'''
		In subclasses this has to be implemented
		output: [ {'user_identity':identity_id,'language':languagename,'size':size,'identity_type_id':it_id,'user_login':user_login} , ...]
		'''
		ans_dict = dict()
		user_login = query_result['user']['login']
		for r in query_result['user']['contributionsCollection']['commitContributionsByRepository']:
			try:
				nb_commits_repo = r['contributions']['totalCount']
				total_size = sum([e['size'] for e in r['repository']['languages']['edges']])
				nb_lang_repo = len(r['repository']['languages']['edges'])
				for e in r['repository']['languages']['edges']:
					lang = e['node']['name']
					size = e['size']
					if total_size != 0:
						factor = size*1./total_size
					else:
						factor = 1./nb_lang_repo
					if lang in ans_dict.keys():
						ans_dict[lang] += nb_commits_repo*factor
					else:
						ans_dict[lang] = nb_commits_repo*factor
			except (KeyError,TypeError) as err:
				self.logger.info('KeyError when parsing languages for {}: {}'.format(user_login,err))
		return [{'user_identity':identity_id,'user_login':user_login,'language_name':lang,'language_size':size,'identity_type_id':identity_type_id} for lang,size in ans_dict.items()]

	def insert_items(self,items_list,commit=True,db=None):
		'''
		In subclasses this has to be implemented
		inserts results in the DB
		'''
		if db is None:
			db = self.db
		if db.db_type == 'postgres':
			extras.execute_batch(db.cursor,'''
				INSERT INTO user_languages(user_identity,language,size)
				VALUES(%s,
						%s,
						%s
						)

				ON CONFLICT DO NOTHING
				;''',((s['user_identity'],s['language_name'],s['language_size']) for s in items_list))
		else:
			db.cursor.executemany('''
					INSERT OR IGNORE INTO user_languages(user_identity,language,size)
					VALUES(?,
							?,
							?
							)
				;''',((s['user_identity'],s['language_name'],s['language_size']) for s in items_list))


		if commit:
			db.connection.commit()

	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		ans = set()
		for r in query_result['user']['contributionsCollection']['commitContributionsByRepository']:
			ans |= set(
				[e['node']['name']
					for e in r['repository']['languages']['edges'] ])
		return len(ans)




class UserLanguagesGQLFiller(SingleQueryUserLanguagesGQLFiller):
	'''
	Querying languages of repositories of users through the GraphQL API
	'''
	def __init__(self,start=None,end=None,**kwargs):
		if isinstance(end,str):
			self.end = dateutil.parser.parse(end)
		else:
			self.end = end
		if isinstance(start,str):
			self.start = dateutil.parser.parse(start)
		else:
			self.start = start
		SingleQueryUserLanguagesGQLFiller.__init__(self,**kwargs)

	def get_startend_attr(self,end,start):
		'''
		returns a string to be put as argument of contributionsCollection in the GraphQL queries for time window selection
		'''

		if start is None and end is None:
			return ''
		else:
			if start is not None:
				start_str = 'from: "{}"'.format(start.strftime('%Y-%m-%dT%H:%M:%SZ'))
			else:
				start_str = ''
			if end is not None:
				end_str = 'to: "{}"'.format(end.strftime('%Y-%m-%dT%H:%M:%SZ'))
			else:
				end_str = ''
			return '({} {})'.format(start_str,end_str)


	def query_string(self,**kwargs):
		'''
		In subclasses this has to be implemented
		output: python-formatable string representing the graphql query
		'''
		if self.start is None or self.end is None:
			tw_info = [self.get_startend_attr(end=self.end,start=self.start)]
		else:
			tw_info = []
			delta = relativedelta(months=11)
			current_max = self.end
			current_min = max(self.end - delta,self.start)
			while current_min > self.start:
				tw_info.append(self.get_startend_attr(end=current_max,start=current_min))
				current_max = current_min
				current_min = max(current_max - delta,self.start)
			tw_info.append(self.get_startend_attr(end=current_max,start=current_min)) # case current_min = start

		query = 'query {{'
		for i,tw in enumerate(tw_info):
			query += self.query_string_element(query_name=('q{}'.format(i) if i>0 else 'user'),time_window_info=tw)
		query += ' }}'

		return query



	def query_string_element(self,query_name,time_window_info,**kwargs):
		return '''{query_name}:user(login: "{user_login}") {{
  							login
  							contributionsCollection{time_window_info} {{
      							totalCommitContributions
      							commitContributionsByRepository {{
        							contributions{{
        								totalCount
        								}}
       								repository{{
       									nameWithOwner
      									languages(first:{page_size}){{
      										totalSize
        									edges{{
        										size
        										node {{
        											name}}
        										}}
        									}}
        								}}
        							}}
        						}}
  							}}
'''.replace('{query_name}',query_name).replace('{time_window_info}',time_window_info) # Not using classic format for clarity: {{ }} would become {{{{ }}}}



	def parse_query_result(self,query_result,identity_id,identity_type_id,**kwargs):
		'''
		In subclasses this has to be implemented
		output: [ {'user_identity':identity_id,'language':languagename,'size':size,'identity_type_id':it_id,'user_login':user_login} , ...]
		'''
		dict_res = dict()

		for qname,q in query_result.items():
			if qname == 'rateLimit' or q is None:
				continue
			for elt in SingleQueryUserLanguagesGQLFiller.parse_query_result(self,
															query_result={'user':q},
															identity_id=identity_id,
															identity_type_id=identity_type_id,
															**kwargs):
				lang = elt['language_name']
				size = elt['language_size']
				if lang in dict_res.keys():
					dict_res[lang]['language_size'] += size
				else:
					dict_res[lang] = elt

		return list(dict_res.values())


	def get_nb_items(self,query_result):
		'''
		In subclasses this has to be implemented
		output: nb_items or None if not relevant
		'''
		ans = set()
		for qname,q in query_result.items():
			if qname == 'rateLimit' or q is None:
				continue
			for r in q['contributionsCollection']['commitContributionsByRepository']:
				ans |= set(
					[e['node']['name']
						for e in r['repository']['languages']['edges'] ])
		return len(ans)


