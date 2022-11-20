
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import datetime
import os
import psycopg2
import json
import glob
import toml
import oyaml as yaml
import pygit2
import subprocess
import copy
import time
import csv
import semantic_version
import sqlite3
import pandas as pd

from .. import fillers
from ..fillers import generic,github_rest


class CMCRequester(fillers.Filler):
	def __init__(self,api_url='https://pro-api.coinmarketcap.com',env_apikey='COINMARKETCAP_API_KEY',api_keys_file='coinmarketcap_api_keys.txt',api_keys=None,**kwargs):
		if api_keys is None:
			self.api_keys = []
		else:
			self.api_keys = copy.deepcopy(api_keys)
		self.api_keys_file = api_keys_file
		self.env_apikey = env_apikey
		self.api_url = api_url
		self.session = Session()

		self.headers = {
			'Accepts': 'application/json',
			'X-CMC_PRO_API_KEY': '',
			}

		fillers.Filler.__init__(self,**kwargs)


	def set_api_keys(self):
		github_rest.GithubFiller.set_api_keys(self)
		self.headers['X-CMC_PRO_API_KEY'] = self.api_keys[0]

	def request(self,path,params):
		if path.startswith('/'):
			if self.api_url.endswith('/'):
				path = path[1:]
		elif not self.api_url.endswith('/'):
			path += '/'
		url = self.api_url + path
		self.session.headers.update(self.headers)
		response = self.session.get(url,params=params)
		return json.loads(response.text)

class CMCFiller(generic.PackageFiller):
	"""
	wrapper around generic.PackageFiller for data from CoinMarketCap
	"""

	def __init__(self,
			source='coinmarketcap',
			source_urlroot=None,
			package_limit=None,
			force=False,
			range_len=10**2,
			filename='coinmarketcap_currencies.csv',
			api_url='https://pro-api.coinmarketcap.com',
			# api_url='https://sandbox-api.coinmarketcap.com',
					**kwargs):
		self.source = source
		self.filename = filename
		self.source_urlroot = source_urlroot
		self.package_limit = package_limit
		self.force = force
		self.range_len = range_len
		self.requester = CMCRequester(api_url=api_url)
		fillers.Filler.__init__(self,**kwargs)

	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder
		data_folder = self.data_folder

		self.requester.data_folder = self.data_folder
		self.requester.set_api_keys()


		#create folder if needed
		if not os.path.exists(data_folder):
			os.makedirs(data_folder)

		self.db.register_source(source=self.source,source_urlroot=self.source_urlroot)
		if self.source_urlroot is None:
			self.source_url_root = self.db.get_source_info(source=self.source)[1]


		self.get_package_list()

	def load_file(self):
		ans = []
		if os.path.exists(os.path.join(self.data_folder,self.filename)):
			with open(os.path.join(self.data_folder,self.filename),'r') as f:
				reader = csv.reader(f)
				for r in reader:
					u,n,sy,sl,da,i = r
					ans.append({'url':u,'name':n,'symbol':sy,'slug':sl,'date_added':da,'id':int(i)})
		return ans

	def save_file(self,ans):
		with open(os.path.join(self.data_folder,self.filename),'w') as f:
			past_ids = set()
			ans = sorted(ans,key=lambda x: x['id'])
			for a in ans:
				if a['id'] not in past_ids:
					f.write(f'''"{a['url']}","{a['name']}","{a['symbol']}","{a['slug']}","{a['date_added']}","{a['id']}"\n''')
					past_ids.add(a['id'])

	def get_package_list(self):
		ans = self.load_file()
		if len(ans):
			init_range = int(max([a['id'] for a in ans]))+1
		else:
			init_range = 0

		while True:
			query_range = list(range(init_range,init_range+self.range_len))
			currencies = self.request_currencies(query_range)
			ans += [{'url':(v['urls']['source_code'][0] if len(v['urls']['source_code']) else None),'name':v['name'],'symbol':v['symbol'],'slug':v['slug'],'date_added':v['date_added'],'id':v['id']} for k,v in currencies['data'].items()]
			init_range += self.range_len
			self.save_file(ans)
			self.logger.info(f'''Retrieved currency info up to ID {ans[-1]['id']}''')
			if len(currencies['data']) == 0 or (self.package_limit is not None and len(ans)>= self.package_limit):
				break

		self.package_list = [(p['id'],p['slug'],p['date_added'],(None if p['url'] in ('None','') else p['url']),None) for p in ans]

	def request_currencies(self,query_range):
		query_range = copy.deepcopy(query_range)
		ans = self.requester.request(path='/v2/cryptocurrency/info',params={'aux':'urls,date_added','id':','.join([str(i) for i in query_range])})
		if ans['status']['error_code'] == 400 and ans['status']['error_message'].startswith('Invalid value'):
			invalid = ans['status']['error_message'].split('"')[-2].split(',')
			for i in invalid:
				query_range.remove(int(i))
			if len(query_range):
				return self.request_currencies(query_range=query_range)
			else:
				return {'data':dict()}
		elif ans['status']['error_code'] == 1008:
			time.sleep(60)
			return self.request_currencies(query_range=query_range)
		elif ans['status']['error_code'] != 0:
			raise IOError(ans['status'])
		else:
			return ans

	def apply(self):
		self.fill_packages()
		self.db.connection.commit()
