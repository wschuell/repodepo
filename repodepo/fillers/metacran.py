
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
import gzip
import requests

from .. import fillers
from ..fillers import generic,github_rest


class MetaCRANFiller(generic.PackageFiller):
	"""
	wrapper around generic.PackageFiller for data from MetaCRAN + RStudio download stats
	"""

	def __init__(self,
			source='cran',
			source_urlroot=None,
			force=False,
			start_date_dl=None,
			end_date_dl=None,
			batch_size=100,
			metacran_url='https://crandb.r-pkg.org/',
			metacran_filename='metacran.json.gz',
			dlstats_url='https://cranlogs.r-pkg.org/downloads/',
			dlstats_folder='metacran_dlstats',
			include_R=True,
			page_size=10**4,
					**kwargs):
		generic.PackageFiller.__init__(self,page_size=page_size,**kwargs)
		self.source = source
		self.source_urlroot = source_urlroot
		self.metacran_url = metacran_url
		self.metacran_filename = metacran_filename
		self.dlstats_url = dlstats_url
		self.dlstats_folder = dlstats_folder
		self.batch_size = batch_size
		self.force = force
		self.include_R = include_R

		if start_date_dl is None:
			self.start_date_dl = datetime.datetime(1993,8,1)
		else:
			self.start_date_dl = start_date_dl
		if end_date_dl is None:
			self.end_date_dl = datetime.datetime.now()
		else:
			self.end_date_dl = end_date_dl

	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder
		data_folder = self.data_folder

		#create folder if needed
		if not os.path.exists(data_folder):
			os.makedirs(data_folder)

		self.db.register_source(source=self.source,source_urlroot=self.source_urlroot)
		if self.source_urlroot is None:
			self.source_url_root = self.db.get_source_info(source=self.source)[1]

		self.get_metacran_info()
		self.parse_metacran_info()
		self.get_dlstats()
		self.parse_dlstats()

	def get_metacran_info(self):
		url = self.metacran_url + '-/allall'
		if self.package_limit is not None:
			url += f'?start_key="A3"&limit={self.package_limit}'
		metacran_file = os.path.join(self.data_folder,self.metacran_filename)
		self.download(url=url,destination=metacran_file,autogzip=True)

	def parse_metacran_info(self):
		metacran_file = os.path.join(self.data_folder,self.metacran_filename)
		with gzip.open(metacran_file,'rb') as f:
			metacran_info = json.load(f)
		self.package_names = sorted(metacran_info.keys())
		if self.include_R:
			offset_p_limit = 1
		else:
			offset_p_limit = 0
		if self.package_limit is not None:
			self.package_names = self.package_names[:self.package_limit-offset_p_limit]
			metacran_info = {k:metacran_info[k] for k in self.package_names}
		self.package_list = [self.parse_package(p) for p in metacran_info.values()]
		if self.include_R:
			self.package_names += ['R']
			self.package_list += [('R','R',datetime.datetime(1993,8,1),'https://github.com/wch/r-source',None)]
		self.package_version_list = []
		for p in metacran_info.values():
			self.package_version_list += self.parse_versions(p)
		if self.include_R:
			self.package_version_list += self.get_R_versions()
		self.package_deps_list = []
		for p in metacran_info.values():
			self.package_deps_list += self.parse_deps(p)
		del metacran_info

	def get_dlstats(self):
		start_date_txt = self.start_date_dl.strftime('%Y-%m-%d')
		end_date_txt = self.end_date_dl.strftime('%Y-%m-%d')
		current_offset = 0
		if os.path.exists(os.path.join(self.data_folder,self.dlstats_folder)):
			file_list = glob.glob(os.path.join(self.data_folder,self.dlstats_folder,'*_*.json.gz'))
			if file_list:
				current_offset = max([int(os.path.basename(r)[:-len('.json.gz')].split('_')[1]) for r in file_list])+1
		while current_offset < len(self.package_names):
			batch_size = self.batch_size
			while True:
				url = self.dlstats_url+f'daily/{start_date_txt}:{end_date_txt}/'+','.join(self.package_names[current_offset:current_offset+batch_size])
				filename = f'{current_offset}_{min(current_offset+batch_size-1,len(self.package_names))}.json.gz'
				dlstats_file = os.path.join(self.data_folder,self.dlstats_folder,filename)
				self.logger.info(f'''Current query for R download stats: {current_offset}-{current_offset+batch_size-1}/{len(self.package_names)}''')
				self.download(url=url,destination=dlstats_file,autogzip=True)
				try:
					with gzip.open(dlstats_file,'rb') as f:
						r = json.load(f)
						del r
					break
				except TypeError:
					batch_size = int(batch_size/2)
					if batch_size == 0:
						self.logger.error('Batch size 1 did not work')
						os.rename(dlstats_file,dlstats_file+'_error')
						raise
					time.sleep(10)
					os.remove(dlstats_file)
			current_offset += batch_size

	def parse_dlstats(self):
		def gen_dl_info():
			for filename in sorted(glob.glob(os.path.join(self.data_folder,self.dlstats_folder,'*.json.gz'))):
				self.logger.info(f'Parsing downloads from {os.path.basename(filename)}')
				with gzip.open(filename,'rb') as f:
					dl_data = json.load(f)
				for e in dl_data:
					if 'package' in e.keys():
						p = e['package']
						if e['downloads'] is not None:
							for d in e['downloads']:
								yield (p,None,d['downloads'],d['day'])
					elif self.include_R:
						R_dl = {}
						for d in e['downloads']:
							k = (d['version'],d['day'])
							if k in R_dl.keys():
								R_dl[k] += d['downloads']
							else:
								R_dl[k] = d['downloads']
						for k,v in R_dl.items():
							yield ('R',k[0],v,k[1])
						del R_dl
				del dl_data

		self.package_version_download_list = gen_dl_info()

	def pick_url(self,p):
		if p['latest'] not in p['versions'].keys():
			pv = p['versions'][max(p['versions'].keys())]
		else:
			pv = p['versions'][p['latest']]
		if 'BugReports' in pv.keys() and pv['BugReports'] != '':
			for ending in '/issues','/issues/':
				if pv['BugReports'].endswith(ending):
					return pv['BugReports'][:-len(ending)]

		if 'URL' in pv.keys() and len(pv['URL']):
			return pv['URL']
		else:
			return None

	def parse_package(self,p):
		pid = p['name']
		name = p['name']
		p_url = self.pick_url(p)
		c_at = min([v for v in p['timeline'].values() if v is not None])
		if not p['archived']:
			archived_at = None
		else:
			archived_at = p['timeline']['archived']
			if archived_at is None:
				archived_at = datetime.datetime.today()
		return (pid,name,c_at,p_url,archived_at)

	def parse_versions(self,p):
		pid = p['name']
		return [(pid,version,c_at) for version,c_at in p['timeline'].items() if version != 'archived' ]

	def get_R_versions(self):
		# inspired by https://github.com/r-hub/rversions
		s = requests.Session()
		req = s.request(method='PROPFIND', url='https://svn.r-project.org/R/tags/', headers={'Depth': '1'}).text
		del s
		versions_raw = req.split('<D:href>/R/tags/R-')
		versions_raw.pop(0)
		versions = []
		for vr in versions_raw:
			v = vr.split('/</D:href>')[0].replace('-','.')
			if v[0] in '0123456789':
				raw_date = vr.split('<lp1:creationdate>')[1].split('</lp1:creationdate>')[0]
				versions.append((v,raw_date))
		return [('R',version,c_at) for version,c_at in sorted(versions)]

	def parse_deps(self,p):
		pid = p['name']
		ans = []
		for version,v_info in p['versions'].items():
			if version != 'archived':
				if 'Depends' in v_info.keys():
					ans += [(pid,version,dep,dep_semver) for dep,dep_semver in v_info['Depends'].items()]
				if 'Imports' in v_info.keys():
					ans += [(pid,version,dep,dep_semver) for dep,dep_semver in v_info['Imports'].items()]
		return ans

	def apply(self):
		self.fill_packages()
		self.fill_package_versions()
		self.fill_package_dependencies()
		self.fill_package_version_downloads()
		self.db.connection.commit()
