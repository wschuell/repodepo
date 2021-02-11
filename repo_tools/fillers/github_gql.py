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


from repo_tools import fillers
from repo_tools.fillers import generic
from repo_tools.fillers import github_rest
import repo_tools as rp

import gql
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport



class Requester(object):
	'''
	Class implementing the Request
	Caching rate limit information, updating at each query, or requerying after <refresh_time in sec> without update
	'''
	def __init__(self,api_key,refresh_time=120,schema=None,fetch_schema=False):
		self.api_key = api_key
		self.remaining = 0
		self.reset_at = datetime.datetime.now() # Like on the API, reset time is last reset time, not future reset time
		self.refresh_time = refresh_time
		self.refreshed_at = None
		self.transport = AIOHTTPTransport(url="https://api.github.com/graphql",headers={'Authorization':'token {}'.format(self.api_key)})
		if schema is not None:
			self.client = Client(transport=self.transport, schema=schema)
		elif fetch_schema:
			self.client = Client(transport=self.transport, fetch_schema_from_transport=True)
		else:
			self.client = Client(transport=self.transport, fetch_schema_from_transport=False)

	def copy(self):
		out_obj = self.__class__(api_key=self.api_key,refresh_time=self.refresh_time)
		out_obj.refreshed_at = self.refreshed_at
		out_obj.reset_at = self.reset_at
		out_obj.remaining = self.remaining


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

	def query(self,gql_query):
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
			else:
				raise
		self.remaining = result['rateLimit']['remaining']
		self.reset_at = datetime.datetime.strptime(result['rateLimit']['resetAt'], '%Y-%m-%dT%H:%M:%SZ')
		self.refreshed_at = datetime.datetime.now()

		return result

class GHGQLFiller(github_rest.GithubFiller):
	"""
	class to be inherited from, contains github credentials management
	"""

	def get_rate_limit(self,requester):
		return requester.get_rate_limit()

	def get_reset_at(self,requester):
		return requester.reset_at

	def set_github_requesters(self,in_thread=False):
		'''
		Setting github requesters
		api keys file syntax, per line: API#notes
		'''
		github_requesters = []
		schema = None
		for ak in self.api_keys:
			g = Requester(api_key=ak,schema=schema)
			try:
				g.get_rate_limit()
			except:
				self.logger.info('API key starting with "{}" and of length {} not valid'.format(ak[:5],len(ak)))
			else:
				github_requesters.append(g)
			schema = g.client.schema
		if in_thread:
			return github_requesters
		else:
			self.github_requesters = github_requesters
