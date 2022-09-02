import datetime
import os
import psycopg2
import json
import glob
import toml
import oyaml as yaml
import pygit2
import subprocess
import csv
import semantic_version
import sqlite3
import pandas as pd

from .. import fillers
from ..fillers import generic


def pseudosemver_check(version,pattern):
	version = version.replace('"','').replace('[','').replace(']','')
	pattern = pattern.replace('"','').replace('[','').replace(']','')

	if version == pattern:
		return True
	else:
		if '-' not in pattern:
			min_pattern = pattern
			max_pattern = pattern
		else:
			parts = pattern.split('-')
			if len(parts) != 2:
				raise SyntaxError(f'Range semver pattern not parseable: {pattern}')
			else:
				min_pattern = parts[0]
				max_pattern = parts[1]
		min_cond = semantic_version.Version(version) in semantic_version.SimpleSpec(f'>={min_pattern}')
		max_cond = semantic_version.Version(version) in semantic_version.SimpleSpec(f'<={max_pattern}')
		return (min_cond and max_cond)


class JuliaHubFiller(generic.PackageFiller):
	"""
	wrapper around generic.PackageFiller for data from https://juliahub.com/app/packages/info
	"""

	def __init__(self,
			source='juliahub',
			source_urlroot=None,
			package_limit=None,
			force=False,
			file_url='https://juliahub.com/app/packages/info',
					**kwargs):
		self.source = source
		self.source_urlroot = source_urlroot
		self.package_limit = package_limit
		self.force = force
		self.file_url = file_url
		fillers.Filler.__init__(self,**kwargs)

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

		self.download(url=self.file_url,destination=os.path.join(self.data_folder,'juliahub_packages.json'))
		with open(os.path.join(self.data_folder,'juliahub_packages.json'),'r') as f:
			packages_json = json.load(f)
		self.package_list = [(i,p['name'],None,p['metadata']['repo']) for i,p in enumerate(packages_json['packages'])]
		if not self.force:
			if self.db.db_type == 'postgres':
				self.db.cursor.execute('''SELECT COUNT(*) FROM packages p
								INNER JOIN sources s
								ON p.source_id=s.id
								AND s.name=%s
								; ''',(self.source,))
			else:
				self.db.cursor.execute('''SELECT COUNT(*) FROM packages p
								INNER JOIN sources s
								ON p.source_id=s.id
								AND s.name=?
								; ''',(self.source,))
			ans = self.db.cursor.fetchone()
			if ans is not None:
				pk_cnt = ans[0]
				if len(self.package_list) == pk_cnt:
					self.package_list = []

		self.db.connection.commit()


	def apply(self):
		self.fill_packages()
		self.db.connection.commit()


class JuliaGeneralFiller(generic.PackageFiller):

	def __init__(self,
			source='julia_general',
			source_urlroot=None,
			package_limit=None,
			force=False,
			repo_url='https://github.com/JuliaRegistries/General',
			repo_folder='Julia_General',
			dates_filename='julia_version_dates.csv',
			replace_repo=False,
			update_repo=False,
			genie_pkgs_url='https://www.dropbox.com/s/ogohqe5bo7qfzl2/dev.sqlite?dl=1',
			dlstats_url='https://julialang-logs.s3.amazonaws.com/public_outputs/current/package_requests_by_date.csv.gz',
			silent_discont_statsintervals=False,
					**kwargs):
		generic.PackageFiller.__init__(self,**kwargs)
		self.source = source
		self.source_urlroot = source_urlroot
		self.package_limit = package_limit
		self.force = force
		self.repo_url = repo_url
		self.repo_folder = repo_folder
		self.replace_repo = replace_repo
		self.update_repo = update_repo
		self.dates_filename = dates_filename
		self.silent_discont_statsintervals = silent_discont_statsintervals
		self.genie_pkgs_url = genie_pkgs_url
		self.dlstats_url = dlstats_url

	def get_downloadstats_overlap(self):
		statsdb_conn = sqlite3.connect(os.path.join(self.data_folder,'pkgs_genie.db'))
		statsdb_cur = statsdb_conn.cursor()
		statsdb_cur.execute('SELECT MAX(date),MIN(date) FROM stats;')
		max_db,min_db = statsdb_cur.fetchone()
		statsdb_conn.close()
		intervals = [[datetime.datetime.strptime(min_db,'%Y-%m-%d'),datetime.datetime.strptime(max_db,'%Y-%m-%d')]]
		for statsfile in glob.glob(os.path.join(self.data_folder,'julia_pkg_dlstats','*.csv')):
			df = pd.read_csv(statsfile)
			intervals.append([datetime.datetime.strptime(df['date'].min(),'%Y-%m-%d'),datetime.datetime.strptime(df['date'].max(),'%Y-%m-%d')])

		intervals = sorted(intervals)
		concat_intervals = [intervals[0]]
		for i_min,i_max in intervals:
			if i_min <= concat_intervals[-1][1]:
				concat_intervals[-1][1] = i_max
			else:
				concat_intervals.append([i_min,i_max])
		return concat_intervals



	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder
		data_folder = self.data_folder

		self.db.cursor.execute('''SELECT 1 FROM full_updates WHERE update_type='julia_packages_general' LIMIT 1;''')
		ans = list(self.db.cursor.fetchall())
		if len(ans)>0 and not self.force:
			self.done = True
			self.logger.info('Julia packages info from General Registry already present in DB, skipping')
			return


		#create folder if needed
		if not os.path.exists(data_folder):
			os.makedirs(data_folder)

		self.db.register_source(source=self.source,source_urlroot=self.source_urlroot)
		if self.source_urlroot is None:
			self.source_url_root = self.db.get_source_info(source=self.source)[1]

		self.clone_repo(repo_url=self.repo_url,repo_folder=self.repo_folder,update=self.update_repo,replace=self.replace_repo)


		self.download(url=self.genie_pkgs_url,destination=os.path.join(self.data_folder,'pkgs_genie.db'))
		
		self.download(url=self.dlstats_url,destination=os.path.join(self.data_folder,'julia_pkg_dlstats',f'''package_requests_by_date_{datetime.datetime.now().strftime('%Y_%m_%d')}.csv.gz'''))
		for gzfile in glob.glob(os.path.join(self.data_folder,'julia_pkg_dlstats','*.gz')):
			self.ungzip(orig_file=gzfile,destination=gzfile[:-3])

		if not len(self.get_downloadstats_overlap()) == 1:
			self.download(force=True,url=self.genie_pkgs_url,destination=os.path.join(self.data_folder,'pkgs_genie.db'))
			new_overlap = self.get_downloadstats_overlap()
			if not len(new_overlap) == 1:
				message = f' Download statistics not spanning a continuous interval: {new_overlap}'
				if not self.silent_discont_statsintervals:
					raise ValueError(message)
				else:
					self.logger.info(message)

		self.package_version_download_list = []
		dlstats_conn = sqlite3.connect(os.path.join(self.data_folder,'pkgs_genie.db'))
		dlstats_cur = dlstats_conn.cursor()
		dlstats_cur.execute('''
			SELECT package_uuid,NULL,SUM(request_count),date
			FROM stats
			GROUP BY package_uuid,date
			;''')
		self.package_version_download_list += list(dlstats_cur.fetchall())
		dlstats_conn.close()
		for statsfile in glob.glob(os.path.join(self.data_folder,'julia_pkg_dlstats','*.csv')):
			df = pd.read_csv(statsfile)
			df['version_str'] = None
			self.package_version_download_list += df[['package_uuid','version_str','request_count','date']].values.tolist()

		self.get_dates()


		# packages		
		self.package_list = []
		package_uuids = set()
		removed_packages = []
		for p in glob.glob(os.path.join(data_folder,self.repo_folder,'?','*')):
			content = toml.load(os.path.join(p,'Package.toml')) 
			elt = (content['uuid'],content['name'],self.package_dates[content['name']],content['repo'])
			self.package_list.append(elt)
			package_uuids.add(content['uuid'])
		for p in glob.glob(os.path.join(data_folder,self.repo_folder,'?','*')):
			content = toml.load(os.path.join(p,'Package.toml')) 
			if content['name'] != 'julia':
				content_deps = toml.load(os.path.join(p,'Deps.toml')) 
				
				for v,v_deps in content_deps.items():
					for d,d_uuid in v_deps.items():
						if d_uuid not in package_uuids:
							package_uuids.add(d_uuid)
							elt = (d_uuid,d,None,None)
							removed_packages.append(elt)
		self.package_list += removed_packages
		
		# package versions		
		self.package_version_list = []
		for p in glob.glob(os.path.join(data_folder,self.repo_folder,'?','*')):
			content_p = toml.load(os.path.join(p,'Package.toml')) 
			content_v = toml.load(os.path.join(p,'Versions.toml'))
			for version,version_info in content_v.items():
				elt = (content_p['uuid'],version,self.version_dates[content_p['name']][version])
				self.package_version_list.append(elt)

		if not self.force:
			if self.db.db_type == 'postgres':
				self.db.cursor.execute('''SELECT COUNT(*) FROM packages p
								INNER JOIN sources s
								ON p.source_id=s.id
								AND s.name=%s
								; ''',(self.source,))
			else:
				self.db.cursor.execute('''SELECT COUNT(*) FROM packages p
								INNER JOIN sources s
								ON p.source_id=s.id
								AND s.name=?
								; ''',(self.source,))
			ans = self.db.cursor.fetchone()
			if ans is not None:
				pk_cnt = ans[0]
				if len(self.package_list) == pk_cnt:
					self.package_list = []
					self.package_version_list = []
					self.package_version_download_list = []

		# package version deps
		self.parse_deps()

		self.package_deps_list = []
		for p,v_deps in self.deps_content.items():
			for v,deps in v_deps.items():
				for d,dv in deps.items():
					self.package_deps_list.append((p,v,d,dv)) ## p and v are uuid not package name



		self.db.connection.commit()

	def get_versions_list(self):
		'''
		Need to build such a list before self.package_version_list because of get_dates() needing it; and self.package_version_list needs the dates
		'''
		if not hasattr(self,'versions_list'):
			self.versions_list = []
			for p in glob.glob(os.path.join(self.data_folder,self.repo_folder,'?','*')):
				content_p = toml.load(os.path.join(p,'Package.toml')) 
				content_v = toml.load(os.path.join(p,'Versions.toml'))
				for version,version_info in content_v.items():
					self.versions_list.append((p,content_p['name'],version))

	def save_dates(self):
		dates_filepath = os.path.join(self.data_folder,self.dates_filename)
		loaded_dates = self.load_dates()
		to_write = {}
		for p,v,d in loaded_dates:
			to_write[(p,v)] = d
		for p,v_l in self.version_dates.items():
			for v,d in v_l.items():
				to_write[(p,v)] = d
		to_write_lines = [f'''"{k[0]}","{k[1]}","{d.strftime('%Y-%m-%d %H:%M:%S')}"''' for k,d in to_write.items()]
		with open(dates_filepath,'w') as f:
			f.write('\n'.join(sorted(to_write_lines)))


	def load_dates(self):
		dates_filepath = os.path.join(self.data_folder,self.dates_filename)
		if os.path.exists(dates_filepath):
			with open(dates_filepath,'r') as f:
				reader = csv.reader(f)
				return [(p,v,datetime.datetime.strptime(d,'%Y-%m-%d %H:%M:%S')) for p,v,d in reader]
		else:
			return []

	def get_dates(self):
		self.get_versions_list()

		total_versions = len(self.versions_list)
		
		self.package_dates = {}
		self.version_dates = {}
		
		'''
		Parsing from file
		'''

		loaded_versions = self.load_dates()
		versions_dict = {}
		for p,pname,v in self.versions_list:
			if pname not in versions_dict.keys():
				versions_dict[pname] = []
			versions_dict[pname].append(v)

		for p,v,d in loaded_versions:
			if p in versions_dict.keys() and v in versions_dict[p]:
				if p not in self.version_dates.keys():
					self.version_dates[p] = {}
				self.version_dates[p][v] = d
	
		count_versions = sum([len(v.keys()) for v in self.version_dates.values()])

		self.logger.info(f'{count_versions}/{total_versions} version date info after parsing from file')
		if count_versions != total_versions:
	
			'''
			Parsing from commit messages
			'''
			count_parseable = 0
			count_not_parseable = 0
			
			repo = pygit2.Repository(os.path.join(self.data_folder,self.repo_folder)) 
			for c in repo.walk(repo.head.target):
				if c.message.startswith('New package: ') or c.message.startswith('New version: '):
					package_name = c.message.split(' ')[2]
					version = c.message.split(' ')[3][1:]
					version_time = datetime.datetime.fromtimestamp(c.commit_time)
					if package_name in versions_dict.keys() and version in versions_dict[package_name]:
						try:
							self.version_dates[package_name][version] = version_time
						except KeyError:
							self.version_dates[package_name] = {version:version_time}
						count_parseable += 1
					else:
						count_not_parseable += 1
				else:
					self.logger.debug(f'Commit message non parseable: {c.message}')
					count_not_parseable += 1
		
			self.logger.info(f'{count_not_parseable} non parseable commits for getting version date from commit message')
	
			count_versions = sum([len(v.keys()) for v in self.version_dates.values()])
			self.logger.info(f'{count_versions}/{total_versions} version date info after parsing from commit messages')
			'''
			Parsing remaining from commit contents specific to the file of each version
			'''
	
			repo = pygit2.Repository(os.path.join(self.data_folder,self.repo_folder)) 
			
			missing_versions = total_versions - count_versions
			count = 0
			try:
				for p,pname,v in sorted(self.versions_list):
					if pname in self.version_dates.keys() and v in self.version_dates[pname].keys():
						continue
					else:
						count += 1
						rel_path = os.path.join(pname[0].upper(),pname)
						# p_folder = os.path.join(self.data_folder,self.repo_folder,rel_path)
						self.logger.info(f'Getting version date for {rel_path} for version {v} ({count}/{missing_versions})')
						cmd = ['git','-C',os.path.join(self.data_folder,self.repo_folder),'log','--pretty="%H"','-S',f"""["{v}"]""",'--',os.path.join(rel_path,'Versions.toml')]#,'|','cat']
						commit_ids = subprocess.check_output(cmd)
						# commit_ids = subprocess.check_output(cmd,cwd=os.path.join(self.data_folder,self.repo_folder))
						commit_ids = commit_ids.decode('utf-8').split('\n')
	
						if commit_ids[-1] == '':
							commit_ids = commit_ids[:-1]
					
						oid = commit_ids[0].replace('"','')
						if pname not in self.version_dates.keys():
							self.version_dates[pname] = {}
						self.version_dates[pname][v] = datetime.datetime.fromtimestamp(repo.get(oid).commit_time)
			finally:
				self.save_dates()

		for p,versions in self.version_dates.items():
			self.package_dates[p] = min([t for t in versions.values()])


	def parse_deps(self):
		'''
		parsing dependencies from toml files.
		Output format: {package1:{version1:{dep1:depv1}}}
		'''
		if not hasattr(self,'deps_content'):
			self.deps_content = {}
			p_list = sorted(glob.glob(os.path.join(self.data_folder,self.repo_folder,'?','*')))
			for i,p in enumerate(p_list):
				content_p = toml.load(os.path.join(p,'Package.toml')) 
				if content_p['name'] == 'julia':
					continue
				else:
					self.logger.info(f'''Parsing dependencies for package {content_p['name']} ({i+1}/{len(p_list)})''')
				content_v = toml.load(os.path.join(p,'Versions.toml'))
				content_d = toml.load(os.path.join(p,'Deps.toml')) 
				content_c = toml.load(os.path.join(p,'Compat.toml'))
				
				versions = [v.replace('"','').replace('[','').replace(']','') for v in content_v.keys()]
				p_name = content_p['name']
				p_uuid = content_p['uuid']

				self.deps_content[p_uuid] = {}
				
				uuid_dict = {}
				for categ_d,deps in content_d.items():
					for dep,d_uuid in deps.items():
						uuid_dict[dep] = d_uuid
				uuid_dict['julia'] = "1222c4b2-2114-5bfd-aeef-88e4692bbb3e"

				for v in versions:
					deps_v = {}
					for categ_d,deps in content_d.items():
						if pseudosemver_check(version=v,pattern=categ_d):
							for dep,d_uuid in deps.items():
								deps_v[d_uuid] = '*'
					
					for categ_c,deps in content_c.items():
						if pseudosemver_check(version=v,pattern=categ_d):
							for dep,sv in deps.items():
								deps_v[uuid_dict[dep]] = str(sv)

					self.deps_content[p_uuid][v] = deps_v

	def apply(self):
		generic.PackageFiller.apply(self)
		self.db.cursor.execute('''INSERT INTO full_updates SELECT 'julia_packages_general';''')
		self.db.connection.commit()


'''
https://julialang-logs.s3.amazonaws.com/public_outputs/current/package_requests_by_date.csv.gz
Downloads: only the last month.
syntax (header): "package_uuid","status","client_type","date","request_addrs","request_count","cache_misses","body_bytes_sent","request_time"
to be used: package_uuid, date,request_count

!! to be aggregated over several possible lines.
!! no versions provided; temporary solution: associate DL to latest version

!! in_source_id should be uuid, and field as text
!! versions info missing?

For deps and versions info: https://github.com/JuliaRegistries/General

versions : working number but not date. Necessary to walk the general repo for additions of the entries in all Versions.toml



!!! some deps are broken! Package in Deps.toml does not exist (anymore?) in the registry
for the moment silently ignoring
'''
