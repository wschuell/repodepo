import os
import hashlib
import csv
import copy
import pygit2
import json
import shutil
import datetime
import subprocess

from psycopg2 import extras

from .. import fillers


class RepoSyntaxError(ValueError):
	'''
	raised when syntax error is encountered in repository url
	'''
	pass

class PackageFiller(fillers.Filler):
	"""
	Fills in packages from a given list, stored in self.package_list during the prepare phase
	This wrapper takes a list as input or a filename, but can be inherited for more complicated package_list construction

	CSV file syntax is expected to be, with header:
	external_id,name,created_at,repository
	or
	name,created_at,repository
	"""
	def __init__(self,package_list=None,package_list_file=None,package_version_list=None,package_deps_list=None,package_version_download_list=None,force=False,deps_to_delete=None,package_limit=None,page_size=10**4,**kwargs):
		self.package_list = package_list
		self.package_list_file = package_list_file
		self.package_limit = package_limit
		if package_version_download_list is None:
			self.package_version_download_list = []
		else:
			self.package_version_download_list = package_version_download_list
		if package_version_list is None:
			self.package_version_list = []
		else:
			self.package_version_list = package_version_list
		if package_deps_list is None:
			self.package_deps_list = []
		else:
			self.package_deps_list = package_deps_list
		if deps_to_delete is None:
			self.deps_to_delete = []
		else:
			self.deps_to_delete = deps_to_delete
		self.force = force
		self.page_size = page_size
		fillers.Filler.__init__(self,**kwargs)

	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder

		if self.package_list is None:
			with open(os.path.join(self.data_folder,self.package_list_file),"rb") as f:
				filehash = hashlib.sha256(f.read()).hexdigest()
			self.source = '{}_{}'.format(self.package_list_file,filehash)
			self.db.register_source(source=self.source)
			with open(os.path.join(self.data_folder,self.package_list_file),'r') as f:
				reader = csv.reader(f)
				headers = next(reader) #remove header
				if len(headers) == 4:
					self.package_list = [r for r in reader]
				elif len(headers) == 3:
					self.package_list = [(i,r[0],r[1],r[2]) for i,r in enumerate(reader)]
				else:
					raise ValueError('''Expected syntax:
external_id,name,created_at,repository
or
name,created_at,repository

got: {}'''.format(headers))

		if self.package_limit is not None:
			self.package_list = self.package_list[:self.package_limit]


	def apply(self):
		self.fill_packages(force=self.force)
		self.fill_package_versions(force=self.force)
		self.fill_package_version_downloads(force=self.force)
		self.fill_package_dependencies(force=self.force)
		self.db.connection.commit()

	def fill_packages(self,package_list=None,source=None,force=False,clean_urls=True):
		'''
		adds repositories from a package repository database (eg crates)
		syntax of package list:
		package id (in source), package name, created_at (datetime.datetime),repo_url

		see .misc for wrappers
		'''

		if package_list is None:
			package_list = self.package_list
		if source is None:
			source = self.source
		if not force:
			if self.db.db_type == 'postgres':
				self.db.cursor.execute('SELECT * FROM packages WHERE source_id=(SELECT id FROM sources WHERE name=%s) LIMIT 1;',(source,))
			else:
				self.db.cursor.execute('SELECT * FROM packages WHERE source_id=(SELECT id FROM sources WHERE name=?) LIMIT 1;',(source,))
			sample_package = self.db.cursor.fetchone()
			if sample_package is not None:
				self.logger.info('Skipping packages from {}'.format(source))
			else:
				self.fill_packages(package_list=package_list,source=source,force=True,clean_urls=clean_urls)
		else:
			self.logger.info('Filling packages from {}'.format(source))
			self.db.register_source(source)
			self.db.register_urls(source=source,url_list=[p[3] for p in package_list if p[3] is not None])

			self.logger.info('Filled URLs')


			# self.db.register_repositories(repo_info_list=[(self.clean_url(p[3])[1],self.clean_url(p[3])[0].split('/')[-2],self.clean_url(p[3])[0].split('/')[-1],self.clean_url(p[3])[0]) for p in package_list if p[3] is not None and self.clean_url(p[3])[0] is not None])
			# self.logger.info('Filled repositories')

			self.db.register_packages(source=source,package_list=package_list)
			self.logger.info('Filled packages')


	def fill_package_versions(self,package_version_list=None,source=None,force=False,clean_urls=True):
		'''
		syntax of package_version list:
		package id (in source), version_name, created_at (datetime.datetime)

		'''

		if package_version_list is None:
			package_version_list = self.package_version_list
		if source is None:
			source = self.source
		if not force:
			if self.db.db_type == 'postgres':
				self.db.cursor.execute('''SELECT 1 FROM package_versions pv
					INNER JOIN packages p
					ON pv.package_id=p.id
					INNER JOIN sources s
					ON p.source_id=s.id AND s.name=%s
					LIMIT 1;''',(source,))
			else:
				self.db.cursor.execute('''SELECT 1 FROM package_versions pv
					INNER JOIN packages p
					ON pv.package_id=p.id
					INNER JOIN sources s
					ON p.source_id=s.id AND s.name=?
					LIMIT 1;''',(source,))
			sample_package = self.db.cursor.fetchone()
			if sample_package is not None:
				self.logger.info('Skipping package versions from {}'.format(source))
			else:
				self.fill_package_versions(package_version_list=package_version_list,source=source,force=True)
		else:
			self.logger.info('Filling package versions from {}'.format(source))
			self.db.register_source(source)
			self.set_source_id()
			if self.db.db_type == 'postgres':
				extras.execute_batch(self.db.cursor,'''
					INSERT INTO package_versions(package_id,version_str,created_at)
					-- SELECT p.id,%(version_str)s,%(created_at)s
					-- FROM packages p
					-- WHERE p.source_id=%(package_source_id)s
					VALUES((SELECT id FROM packages WHERE source_id=%(package_source_id)s AND insource_id=%(package_insource_id)s),%(version_str)s,%(created_at)s)
					ON CONFLICT DO NOTHING
					;''',({'version_str':v_str,'package_insource_id':p_id,'package_source_id':self.source_id,'created_at':created_at} for (p_id,v_str,created_at) in package_version_list),
					page_size=self.page_size)
			else:
				self.db.cursor.executemany('''
					INSERT OR IGNORE INTO package_versions(package_id,version_str,created_at)
					VALUES((SELECT id FROM packages WHERE source_id=:package_source_id
						AND insource_id=:package_insource_id)
					,:version_str,
					:created_at)
					;''',({'version_str':v_str,'package_insource_id':p_id,'package_source_id':self.source_id,'created_at':created_at} for (p_id,v_str,created_at) in package_version_list))

			self.logger.info('Filled package versions')


	def set_source_id(self):
		if not hasattr(self,'source_id'):
			if self.db.db_type == 'postgres':
				self.db.cursor.execute('''
					SELECT id FROM sources WHERE name=%s
						;''',(self.source,))
			else:
				self.db.cursor.execute('''
					SELECT id FROM sources WHERE name=?
						;''',(self.source,))
			self.source_id = self.db.cursor.fetchone()[0]

	def fill_package_version_downloads(self,package_version_download_list=None,source=None,force=False,clean_urls=True):
		'''
		syntax of package_version_download list:
		package id (in source), version_name, nb_downloads ,downloaded_at (datetime.datetime)

		'''

		if package_version_download_list is None:
			package_version_download_list = self.package_version_download_list
		if source is None:
			source = self.source
		if not force:
			if self.db.db_type == 'postgres':
				self.db.cursor.execute('''SELECT 1 FROM package_version_downloads pvd
					INNER JOIN package_versions pv
					ON pv.id=pvd.package_version
					INNER JOIN packages p
					ON pv.package_id=p.id
					INNER JOIN sources s
					ON p.source_id=s.id AND s.name=%s
					LIMIT 1;''',(source,))
			else:
				self.db.cursor.execute('''SELECT 1 FROM package_version_downloads pvd
					INNER JOIN package_versions pv
					ON pv.id=pvd.package_version
					INNER JOIN packages p
					ON pv.package_id=p.id
					INNER JOIN sources s
					ON p.source_id=s.id AND s.name=?
					LIMIT 1;''',(source,))
			sample_package = self.db.cursor.fetchone()
			if sample_package is not None:
				self.logger.info('Skipping package version downloads from {}'.format(source))
			else:
				self.fill_package_version_downloads(package_version_download_list=package_version_download_list,source=source,force=True)
		else:
			self.logger.info('Filling package version downloads from {}'.format(source))
			self.db.register_source(source)
			self.set_source_id()
			if self.db.db_type == 'postgres':
				extras.execute_batch(self.db.cursor,'''
					INSERT INTO package_version_downloads(package_version,downloaded_at,downloads)
					VALUES(
						(SELECT v.id FROM package_versions v
							INNER JOIN packages p
							ON p.source_id=%(package_source_id)s AND p.insource_id=%(package_insource_id)s
							AND p.id=v.package_id AND v.version_str=%(version_str)s)
						,%(downloaded_at)s,%(nb_downloads)s)
					ON CONFLICT DO NOTHING
					;''',({'version_str':v_str,'package_insource_id':p_id,'package_source_id':self.source_id,'downloaded_at':dl_at,'nb_downloads':nb_dl} for (p_id,v_str,nb_dl,dl_at) in package_version_download_list),
					page_size=self.page_size)
			else:
				self.db.cursor.executemany('''
					INSERT OR IGNORE INTO package_version_downloads(package_version,downloaded_at,downloads)
					VALUES(
						(SELECT v.id FROM package_versions v
							INNER JOIN packages p
							ON p.source_id=:package_source_id AND p.insource_id=:package_insource_id
							AND p.id=v.package_id AND v.version_str=:version_str)
						,:downloaded_at,:nb_downloads)
					;''',({'version_str':v_str,'package_insource_id':p_id,'package_source_id':self.source_id,'downloaded_at':dl_at,'nb_downloads':nb_dl} for (p_id,v_str,nb_dl,dl_at) in package_version_download_list))

			self.logger.info('Filled package version downloads')



	def fill_package_dependencies(self,package_deps_list=None,source=None,force=False,clean_urls=True):
		'''
		syntax of package_deps list:
		depending package id (in source), depending version_name, depending_on_package source_id, semver

		'''

		if package_deps_list is None:
			package_deps_list = self.package_deps_list
		if source is None:
			source = self.source
		if not force:
			if self.db.db_type == 'postgres':
				self.db.cursor.execute('''SELECT 1 FROM package_dependencies pd
					INNER JOIN packages p
					ON pd.depending_on_package=p.id
					INNER JOIN sources s
					ON p.source_id=s.id AND s.name=%s
					LIMIT 1;''',(source,))
			else:
				self.db.cursor.execute('''SELECT 1 FROM package_dependencies pd
					INNER JOIN packages p
					ON pd.depending_on_package=p.id
					INNER JOIN sources s
					ON p.source_id=s.id AND s.name=?
					LIMIT 1;''',(source,))
			sample_package = self.db.cursor.fetchone()
			if sample_package is not None:
				self.logger.info('Skipping package dependencies from {}'.format(source))
			else:
				self.fill_package_dependencies(package_deps_list=package_deps_list,source=source,force=True)
		else:
			self.logger.info('Filling package dependencies from {}'.format(source))
			self.db.register_source(source)
			self.set_source_id()
			if self.db.db_type == 'postgres':
				extras.execute_batch(self.db.cursor,'''
					INSERT INTO package_dependencies(depending_version,depending_on_package,semver_str)
					VALUES(
						(SELECT v.id FROM package_versions v
							INNER JOIN packages p
							ON p.source_id=%(package_source_id)s AND p.insource_id=%(version_package_id)s
							AND p.id=v.package_id AND v.version_str=%(version_str)s),
						(SELECT id FROM packages WHERE source_id=%(package_source_id)s AND insource_id=%(depending_on_package)s),
						%(semver_str)s)
					ON CONFLICT DO NOTHING
					;''',({'version_package_id':vp_id,'version_str':v_str,'depending_on_package':dop_id,'package_source_id':self.source_id,'semver_str':semver_str} for (vp_id,v_str,dop_id,semver_str) in package_deps_list),
					page_size=self.page_size)

				for (dep_p,dep_on_p) in self.deps_to_delete:
					self.db.cursor.execute('''
						DELETE FROM package_dependencies WHERE
							depending_version IN
								(SELECT pv.id FROM packages p
									INNER JOIN package_versions pv
									ON pv.package_id=p.id AND p.name=%(dep_p)s
									AND p.source_id=%(package_source_id)s)
							AND depending_on_package IN
								(SELECT p.id FROM packages p
									WHERE p.name=%(dep_on_p)s
									AND p.source_id=%(package_source_id)s)
					;''',{'dep_p':dep_p,'dep_on_p':dep_on_p,'package_source_id':self.source_id})
			else:
				self.db.cursor.executemany('''
					INSERT OR IGNORE INTO package_dependencies(depending_version,depending_on_package,semver_str)
					VALUES(
						(SELECT v.id FROM package_versions v
							INNER JOIN packages p
							ON p.source_id=:package_source_id AND p.insource_id=:version_package_id
							AND p.id=v.package_id AND v.version_str=:version_str),
						(SELECT id FROM packages WHERE source_id=:package_source_id AND insource_id=:depending_on_package),
						:semver_str)
					;''',({'version_package_id':vp_id,'version_str':v_str,'depending_on_package':dop_id,'package_source_id':self.source_id,'semver_str':semver_str} for (vp_id,v_str,dop_id,semver_str) in package_deps_list))


				for (dep_p,dep_on_p) in self.deps_to_delete:
					self.db.cursor.execute('''
						DELETE FROM package_dependencies WHERE
							depending_version IN
								(SELECT pv.id FROM packages p
									INNER JOIN package_versions pv
									ON pv.package_id=p.id AND p.name=:dep_p
									AND p.source_id=:package_source_id)
							AND depending_on_package IN
								(SELECT p.id FROM packages p
									WHERE p.name=:dep_on_p
									AND p.source_id=:package_source_id)
					;''',{'dep_p':dep_p,'dep_on_p':dep_on_p,'package_source_id':self.source_id})

			self.db.connection.commit()
			self.logger.info('Filled package dependencies')


class URLFiller(fillers.Filler):
	'''
	Similar to PackageFiller but only fills in URLs
	'''

	def __init__(self,url_list=None,url_list_file=None,force=False,**kwargs):
		self.url_list = url_list
		self.url_list_file = url_list_file
		self.force = force
		fillers.Filler.__init__(self,**kwargs)

	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder

		if self.url_list is None:
			with open(os.path.join(self.data_folder,self.url_list_file),"rb") as f:
				filehash = hashlib.sha256(f.read()).hexdigest()
			self.source = '{}_{}'.format(self.url_list_file,filehash)
			self.db.register_source(source=self.source)
			self.set_url_list()

	def set_url_list(self):
		with open(os.path.join(self.data_folder,self.url_list_file),'r') as f:
			reader = csv.reader(f)
			self.url_list = [r[0] for r in reader]

	def apply(self):
		self.fill_urls(force=self.force)
		self.db.connection.commit()

	def fill_urls(self,url_list=None,source=None,force=False,clean_urls=True):
		'''
		'''
		if url_list is None:
			url_list = self.url_list
		self.db.register_urls(source=source,url_list=url_list)
		self.logger.info('Filled URLs')

class GithubURLFiller(URLFiller):

	def set_url_list(self):
		with open(os.path.join(self.data_folder,self.url_list_file),'r') as f:
			reader = csv.reader(f)
			header = next(reader)
			self.url_list = ['github.com/'+r[-1] for r in reader]


class SourcesFiller(fillers.Filler):
	'''
	Register given sources in the database
	'''
	def __init__(self,source,source_urlroot=None,**kwargs):
		'''
		source and source_urlroot can be strings or lists.
		If lists they have to be of the same size
		'''
		self.source = source
		self.source_urlroot = source_urlroot
		fillers.Filler.__init__(self,**kwargs)

	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder
		if isinstance(self.source,str) and isinstance(self.source_urlroot,str):
			self.source_list = [(self.source,self.source_urlroot)]
		elif self.source_urlroot is None:
			if isinstance(self.source,str):
				self.source_list = [(self.source,None)]
			else:
				self.source_list = [(s,None) for s in self.source]
		elif isinstance(self.source_urlroot,str):
			self.source_list = [(s,self.source_urlroot) for s in self.source]
		elif len(self.source) == len(self.source_urlroot):
			self.source_list = list(zip(self.source,self.source_urlroot))
		else:
			raise ValueError('Args source and source_urlroot do not match, they should either be both strings or both lists of the same length. source: {}, source_urlroot: {}'.format(self.source,self.source_urlroot))

	def apply(self):
		for s,su in self.source_list:
			self.db.register_source(source=s,source_urlroot=su)

class RepositoriesFiller(fillers.Filler):
	'''
	From currently set sources, fills repositories with recognized URL
	Also cleans URLs in url table
	Goes through packages to associate them back with the created repos
	Uses sources already in the database, dont forget to register them beforehand
	'''
	def __init__(self,source='autofill_repos_from_urls',force=False,**kwargs):
		'''

		'''
		self.source = source
		self.force = force
		fillers.Filler.__init__(self,**kwargs)

	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder

		self.db.cursor.execute('SELECT id,url_root FROM sources WHERE url_root IS NOT NULL;')
		self.url_roots = list(self.db.cursor.fetchall())

		if self.force:

			self.db.cursor.execute('SELECT url FROM urls;')
			# self.urls = [(raw_url,cleaned_url,source_id)]
		else:
			self.db.cursor.execute('''
					SELECT u.url FROM urls u
					LEFT OUTER JOIN repositories r
					ON r.url_id=u.id
					WHERE r.url_id IS NULL AND (u.id=u.cleaned_url OR u.cleaned_url IS NULL)
					;''')
		self.urls = list(set([(u[0],*self.clean_url(u[0])) for u in self.db.cursor.fetchall()]))
		self.cleaned_urls = list(set([(cleaned_url,source_id) for (raw_url,cleaned_url,source_id) in self.urls if cleaned_url is not None]))

			# source_id,owner,name,cleaned_url
		self.repo_info_list = [(source_id,cleaned_url.split('/')[-2],cleaned_url.split('/')[-1],cleaned_url) for (cleaned_url,source_id) in self.cleaned_urls ]


	def apply(self):
		self.fill_source()
		self.fill_cleaned_urls()
		self.fill_repositories()
		self.db.connection.commit()
		self.logger.info('Filled repositories')

	def fill_source(self):
		'''
		Registers source if not existing
		'''
		self.db.register_source(source=self.source)

	def fill_cleaned_urls(self):
		'''
		Lists URLS that can be cleaned with available url roots, and fills in the urls table accordingly
		'''
		self.db.register_urls(source=self.source,url_list=self.urls)

	def fill_repositories(self):
		'''
		Registers repositories
		'''
		# self.db.register_repositories(repo_info_list=[
		#(self.clean_url(p[3])[1],
		#self.clean_url(p[3])[0].split('/')[-2],
		#self.clean_url(p[3])[0].split('/')[-1],
		#self.clean_url(p[3])[0])

		#for p in self.package_list if p[3] is not None and self.clean_url(p[3])[0] is not None])
		self.db.register_repositories(repo_info_list=self.repo_info_list)

		if len(self.repo_info_list):
			self.db.cursor.execute('''
				UPDATE packages SET repo_id=(SELECT r.id FROM repositories r
									INNER JOIN urls u
									ON r.url_id=u.id
									INNER JOIN urls u2
									ON u2.cleaned_url=u.id AND u2.id=packages.url_id)
				;''')
		self.db.connection.commit()

	def clean_url(self,url):
		'''
		getting a clean url based on what is available as sources, using source_urlroot values
		returns clean_url,source_id
		'''
		if url is None:
			return None,None
		for ur_id,ur in self.url_roots:
			try:
				return self.repo_formatting(repo=url,source_urlroot=ur,output_cleaned_url=True),ur_id
			except RepoSyntaxError:
				continue
		return None,None

	def repo_formatting(self,repo,source_urlroot,output_cleaned_url=False,raise_error=False):
		'''
		Formatting repositories so that they match the expected syntax 'user/project'
		'''

		r = copy.copy(repo)

		# checking
		if source_urlroot not in r:
			raise RepoSyntaxError('Repo {} has not expected source {}.'.format(repo,source_urlroot))

		# Removing front elements
		for start_str in ['http://','https://','http:/','https:/','www.','/',' ','\n','\t','\r','"',"'"]:
			if repo.startswith(start_str):
				return self.repo_formatting(repo=repo[len(start_str):],source_urlroot=source_urlroot,output_cleaned_url=output_cleaned_url,raise_error=raise_error)

		# Remove end of url modifiers
		for flagged_char in ['"',"'",'?',' ',' ','!',',',';']:
			if flagged_char in repo:
				return self.repo_formatting(repo=repo.split(flagged_char)[0],source_urlroot=source_urlroot,output_cleaned_url=output_cleaned_url,raise_error=raise_error)

		# Removing back elements
		for end_str in ['.git',' ','/','\n','\t','\r']:
			if repo.endswith(end_str):
				return self.repo_formatting(repo=repo[:-len(end_str)],source_urlroot=source_urlroot,output_cleaned_url=output_cleaned_url,raise_error=raise_error)

		# Removing double extension url_root
		if '.' in source_urlroot:
			ending = source_urlroot.split('.')[-1]
			double_ending = '{}.{}'.format(source_urlroot,ending)
			if repo.startswith(double_ending):
				return self.repo_formatting(repo=source_urlroot+repo[len(double_ending):],source_urlroot=source_urlroot,output_cleaned_url=output_cleaned_url,raise_error=raise_error)

		# Removing double url_root
		if repo.startswith('{0}{0}'.format(source_urlroot)):
			return self.repo_formatting(repo=repo[len(source_urlroot):],source_urlroot=source_urlroot,output_cleaned_url=output_cleaned_url,raise_error=raise_error)

		if repo.startswith('{0}/{0}'.format(source_urlroot)):
			return self.repo_formatting(repo=repo[len(source_urlroot)+1:],source_urlroot=source_urlroot,output_cleaned_url=output_cleaned_url,raise_error=raise_error)

		# Typos replacement
		repo = repo.replace('//','/')

		# checks
		# minimum 3 fields
		# begins with source

		if not repo.startswith(source_urlroot):
			raise RepoSyntaxError('Repo {} has not expected source {}.'.format(repo,source_urlroot))
		else:
			r = repo[len(source_urlroot):]
			if r.startswith('/'):
				r = r[1:]

		if source_urlroot in r:
			msg = 'Repo {} has not expected syntax for source {}.'.format(repo,source_urlroot)
			self.logger.info(msg)
			raise RepoSyntaxError(msg)

		if (raise_error and len(r.split('/')) != 2) or len(r.split('/')) < 2:
			msg = 'Repo has not expected syntax "user/project" or prefixed with {}:{}. Please fix input or update the repo_formatting method.'.format(source_urlroot,repo)
			self.logger.info(msg)
			raise RepoSyntaxError(msg)
		r = '/'.join(r.split('/')[:2])
		if '' in r.split('/'):
			msg = 'Critical syntax error for repository url: {}, parsed {}'.format(repo,r)
			self.logger.info(msg)
			raise RepoSyntaxError(msg)

		if output_cleaned_url:
			return 'https://{}/{}'.format(source_urlroot,r)
		else:
			return r

class ClonesFiller(fillers.Filler):
	'''
	Tries to clone all repositories present in the DB
	'''
	def __init__(self,precheck_cloned=False,force=False,update=True,failed=False,ssh_sources=None,ssh_key=os.path.join(os.environ['HOME'],'.ssh','id_rsa'),sources=None,rm_first=False,**kwargs):
		'''
		if sources is None, repositories of all sources are cloned. Otherwise, considered as a whitelist of sources to batch-clone.

		sources listed in ssh_sources will be retrieved through SSH protocol, others with HTTPS
		syntax: {source_name:source_ssh_key_path}
		if the value source_ssh_key_path is None, it uses the main ssh_key arg
		'''
		self.force = force
		self.update = update
		self.failed = failed
		self.rm_first = rm_first
		self.precheck_cloned = precheck_cloned

		self.ssh_key = ssh_key
		if ssh_sources is None:
			self.ssh_sources = {}
		else:
			self.ssh_sources = copy.deepcopy(ssh_sources)
		self.callbacks = {}
		for k,v in list(self.ssh_sources.items()):
			if v is None:
				self.ssh_sources[k] = self.ssh_key
				ssh_key = self.ssh_key
			else:
				ssh_key = v
			keypair = pygit2.Keypair('git',ssh_key+'.pub',ssh_key,'')
			self.callbacks[k] = pygit2.RemoteCallbacks(credentials=keypair)
		fillers.Filler.__init__(self,**kwargs)

	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder
		elif self.data_folder != self.db.data_folder:
			raise ValueError('Data folder for ClonesFiller should not be different than db.data_folder. This ensures safe clones renaming when detecting change in repo name.')
		if self.rm_first and os.path.exists(os.path.join(self.data_folder,'cloned_repos')):
			shutil.rmtree(os.path.join(self.data_folder,'cloned_repos'))
		self.make_folder() # creating folder if not existing

		if self.precheck_cloned:
			self.db.cursor.execute('''
				SELECT s.name,r.owner,r.name,r.id
				FROM repositories r
				INNER JOIN sources s
				ON s.id=r.source
				AND r.cloned
				ORDER BY s.name,r.owner,r.name
				;''')
			repo_ids_to_update = []
			for source_name,repo_owner,repo_name,repo_id in self.db.cursor.fetchall():
				if not os.path.exists(os.path.join(self.data_folder,'cloned_repos',source_name,repo_owner,repo_name,'.git')):
					repo_ids_to_update.append((repo_id,))

			if self.db.db_type == 'postgres':
				extras.execute_batch(self.db.cursor,'UPDATE repositories SET cloned=false WHERE id=%s;',repo_ids_to_update)
				extras.execute_batch(self.db.cursor,'''DELETE FROM table_updates WHERE repo_id=%s AND table_name='clones';''',repo_ids_to_update)
			else:
				self.db.cursor.executemany('UPDATE repositories SET cloned=false WHERE id=?;',repo_ids_to_update)
				self.db.cursor.executemany('''DELETE FROM table_updates WHERE repo_id=? AND table_name='clones';''',repo_ids_to_update)
			if len(repo_ids_to_update):
				self.logger.info('{} repositories set as cloned but not found in cloned_repos folder, setting to not cloned'.format(len(repo_ids_to_update)))
			else:
				self.logger.info('All repositories set as cloned found in cloned_repos folder')
			self.db.connection.commit()

	def make_folder(self):
		'''
		creating folder if not existing
		'''
		if not os.path.exists(self.data_folder):
			os.makedirs(self.data_folder)
		if not os.path.exists(os.path.join(self.data_folder,'cloned_repos')):
			os.makedirs(os.path.join(self.data_folder,'cloned_repos'))

	def apply(self):
		self.clone_all()

	def clone_all(self):
		repo_list = self.get_repo_list()
		for i,r in enumerate(repo_list):
			source,source_urlroot,owner,name = r
			self.logger.info('Repo {}/{}'.format(i+1,len(repo_list)))
			self.clone(source=source,name=name,owner=owner,source_urlroot=source_urlroot,update=self.update)

	def get_repo_list(self):
		'''
		force: all repos
		failed: all failed repos
		not force and not failed: all repos not having an entry in table_updates

		update does not matter here, it is only the policy when encountering a folder already existing for one element of the list.
		update can be combined with force; or alternatively table_updates can be emptied of all 'clones' entries so that all repos are updated once.
		'''

		if self.force: # or self.update:
			self.db.cursor.execute('''
				SELECT s.name,s.url_root,r.owner,r.name
				FROM repositories r
				INNER JOIN sources s
				ON s.id=r.source
				ORDER BY s.name,r.owner,r.name
				;''')
			return list(self.db.cursor.fetchall())
		elif self.failed:
			self.db.cursor.execute('''
				SELECT s.name,s.url_root,r.owner,r.name
				FROM repositories r
				INNER JOIN sources s
				ON s.id=r.source AND NOT r.cloned
				ORDER BY s.name,r.owner,r.name
				;''')
			return list(self.db.cursor.fetchall())
		else:
			self.db.cursor.execute('''
				SELECT s.name,s.url_root,r.owner,r.name
				FROM repositories r
				INNER JOIN sources s
				ON s.id=r.source
				LEFT JOIN table_updates tu
				ON tu.repo_id=r.id AND tu.table_name='clones'
				GROUP BY s.name,s.url_root,r.owner,r.name
				HAVING COUNT(tu.repo_id)=0
				ORDER BY s.name,r.owner,r.name

				;''')
			return list(self.db.cursor.fetchall())


	def build_url(self,name,owner,source_urlroot,ssh_mode):
		'''
		building url, depending on mode (ssh or https)
		'''
		if ssh_mode:
			return 'git@{}:{}/{}'.format(source_urlroot,owner,name)
		else:
			return 'https://{}/{}/{}.git'.format(source_urlroot,owner,name)

	def set_init_dl(self,repo_id,source,owner,repo):
		'''
		Sets a download attempt in the database, with update time being the time of the last commit
		This is used when for a newly created database cloned repos are already present in the folder
		'''
		if self.db.get_last_dl(repo_id=repo_id,success=True) is None:
			repo_obj = self.get_repo(source=source,owner=owner,name=repo)
			try:
				last_commit_time = datetime.datetime.fromtimestamp(repo_obj.revparse_single('HEAD').commit_time)
			except KeyError:
				self.logger.info('HEAD reference unavailable for repo {}/{}/{}'.format(source,owner,repo))
				last_commit_time = None
			self.db.submit_download_attempt(source=source,owner=owner,repo=repo,success=True,dl_time=last_commit_time)

	def clone(self,source,name,owner,source_urlroot,replace=False,update=False,db=None,clean_symlinks=False):
		'''
		Cloning one repo.
		Skipping if folder exists by default; not if replace=True in this case delete folder and restart
		Executing update_repo if repo already exists and update is True

		'''
		if db is None:
			db = self.db
		repo_folder = os.path.join(self.data_folder,'cloned_repos',source,owner,name)
		if os.path.exists(repo_folder):
			if replace:
				self.logger.info('Removing folder {}/{}/{}'.format(source,owner,name))
				shutil.rmtree(repo_folder)
				self.clone(source=source,name=name,owner=owner,source_urlroot=source_urlroot)
			elif update:
				self.update_repo(source=source,name=name,owner=owner,source_urlroot=source_urlroot)
			else:
				self.logger.info('Repo {}/{}/{} already exists'.format(source,owner,name))
				repo_id = self.db.get_repo_id(source=source,name=name,owner=owner)
				self.set_init_dl(repo_id=repo_id,source=source,repo=name,owner=owner)
				self.db.set_cloned(repo_id=repo_id)
		else:
			if os.path.islink(repo_folder): # is symbolic link but broken
				if clean_symlinks:
					shutil.rmtree(repo_folder)
				else:
					err_txt = 'Symlink broken: {} -> {}'.format(repo_folder,os.readlink(repo_folder))
					self.db.log_error(err_txt)
					raise OSError(err_txt)
			repo_id = self.db.get_repo_id(source=source,name=name,owner=owner)
			# if self.db.db_type == 'postgres':
			# 	self.db.cursor.execute('SELECT * FROM download_attempts WHERE repo_id=%s LIMIT 1;',(repo_id,))
			# else:
			# 	self.db.cursor.execute('SELECT * FROM download_attempts WHERE repo_id=? LIMIT 1;',(repo_id,))

			# if (self.db.cursor.fetchone() is None) or force:
			self.logger.info('Cloning repo {}/{}/{}'.format(source,owner,name))
			try:
				try:
					callbacks = self.callbacks[source]
					ssh_mode = True
				except KeyError:
					callbacks = None
					ssh_mode = False
				pygit2.clone_repository(url=self.build_url(source_urlroot=source_urlroot,name=name,owner=owner,ssh_mode=ssh_mode),path=repo_folder,callbacks=callbacks)
				success = True
			except pygit2.GitError as e:
				err_txt = 'Git Error for repo {}/{}/{}'.format(source,owner,name)
				self.logger.info(err_txt)
				self.db.log_error(err_txt)
				success = False
			except ValueError as e:
				if str(e).startswith('malformed URL'):
					err_txt = 'Error for repo {}/{}/{}: {}'.format(source,owner,name,e)
					self.logger.info(err_txt)
					self.db.log_error(err_txt)
					success = False
				else:
					raise
			self.db.submit_download_attempt(success=success,source=source,repo=name,owner=owner)
			# else:
			# 	self.logger.info('Skipping repo {}/{}/{}, already failed to download'.format(source,owner,name))

	def update_repo(self,name,source,source_urlroot,owner):
		'''
		git fetch on repo
		cloning if folder not existing
		'''
		self.logger.info('Updating repo {}/{}/{}'.format(source,owner,name))
		repo_folder = os.path.join(self.data_folder,'cloned_repos',source,owner,name)

		repo_obj = pygit2.Repository(os.path.join(repo_folder,'.git'))
		try:
			try:
				callbacks = self.callbacks[source]
			except KeyError:
				callbacks = None
			cmd = 'git fetch --force --all'
			cmd_output = subprocess.check_output(cmd.split(' '),cwd=repo_folder, env=os.environ.update(dict(GIT_TERMINAL_PROMPT='0')))
			try:
				cmd2 = 'git pull --force --all'
				cmd_output2 = subprocess.check_output(cmd2.split(' '),cwd=repo_folder, env=os.environ.update(dict(GIT_TERMINAL_PROMPT='0')))
			except subprocess.CalledProcessError as e:
				err_txt = 'Git pull Error (fetch worked) for repo {}/{}/{}: {}, {}'.format(source,owner,name,e,e.output)
				self.logger.info(err_txt)
				self.db.log_error(err_txt)

			### NB: pygit2 is complex for a simple 'git pull', a solution would be to test such an implementation: https://github.com/MichaelBoselowitz/pygit2-examples/blob/master/examples.py
			# repo_obj.remotes["origin"].fetch(callbacks=callbacks)
			## NB: GIT_TERMINAL_PROMPT=0 forces failure when credentials asked instead of prompt
			success = True
		# except pygit2.GitError as e:
		except subprocess.CalledProcessError as e:
			err_txt = 'Git Error (fetch) for repo {}/{}/{}: {}, {}'.format(source,owner,name,e,e.output)
			self.logger.info(err_txt)
			self.db.log_error(err_txt)
			success = False

		self.db.submit_download_attempt(success=success,source=source,repo=name,owner=owner)

	def get_repo(self,name,source,owner):
		'''
		Returns the pygit2 repository object
		'''
		repo_folder = os.path.join(self.data_folder,'cloned_repos',source,owner,name)
		if not os.path.exists(repo_folder):
			raise ValueError('Repository {}/{}/{} not found in cloned_repos folder'.format(source,owner,name))
		else:
			return pygit2.Repository(os.path.join(repo_folder,'.git'))

class RepoCommitOwnershipFiller(fillers.Filler):
	'''
	Based on repo creation date (or package creation date if NULL), attributing commit to oldest repo
	As commit timestamps can be forged, this is not used.
	USING ONLY PACKAGE DATE SO FAR -- repo creation date is CURRENT_TIMESTAMP  at row creation
	'''

	def __init__(self,force=False,**kwargs):
		self.force = force
		fillers.Filler.__init__(self)

	def apply(self):
		self.db.cursor.execute('''SELECT MAX(updated_at) FROM full_updates WHERE update_type='commits orig repos repo/package creation date';''')
		last_fu = self.db.cursor.fetchone()[0]
		self.db.cursor.execute('''SELECT MAX(updated_at) FROM full_updates WHERE update_type='commits';''')
		last_fu_commits = self.db.cursor.fetchone()[0]
		if not self.force and last_fu is not None and last_fu_commits<=last_fu:
			self.logger.info('Skipping commit origin repository attribution using repo/package creation date')
		else:
			self.logger.info('Filling commit origin repository attribution using repo/package creation date')
			self.db.cursor.execute('''
						UPDATE commit_repos SET is_orig_repo=true
							WHERE is_orig_repo IS NULL
								AND repo_id = (SELECT ccp.repo_id
									FROM commit_repos ccp
									INNER JOIN repositories r
									ON ccp.commit_id=commit_repos.commit_id AND r.id=ccp.repo_id
									LEFT OUTER JOIN packages p
									ON p.repo_id=r.id
									AND p.created_at IS NOT NULL
									ORDER BY p.created_at ASC
									LIMIT 1
									)
						;''')

			self.logger.info('Filled {} commit origin repository attributions'.format(self.db.cursor.rowcount))

			# set to null where twice true for is_orig_repo
			self.logger.info('Filling commit origin repository attribution: set to null where twice true for is_orig_repo')
			self.db.cursor.execute('''
						UPDATE commit_repos SET is_orig_repo=NULL
							WHERE commit_id IN (
								SELECT cr.commit_id FROM commit_repos cr
								WHERE cr.is_orig_repo
								GROUP BY cr.commit_id
								HAVING COUNT(*)>=2
									)
						;''')
			self.logger.info('Detected {} commit origin conflicts'.format(self.db.cursor.rowcount))

			if self.db.cursor.rowcount > 0:
				self.logger.info('Filling commit origin repository attribution using repo/package creation date')
				self.db.cursor.execute('''
						UPDATE commit_repos SET is_orig_repo=true
							WHERE is_orig_repo IS NULL
								AND repo_id = (SELECT ccp.repo_id
									FROM commit_repos ccp
									INNER JOIN repositories r
									ON ccp.commit_id=commit_repos.commit_id AND r.id=ccp.repo_id
									LEFT OUTER JOIN packages p
									ON p.repo_id=r.id
									AND p.created_at IS NOT NULL
									ORDER BY p.created_at ASC
									LIMIT 1
									)
						;''')

				self.logger.info('Filled {} commit origin repository attributions'.format(self.db.cursor.rowcount))

			self.logger.info('Filling commit origin repository attribution: updating commits table')
			self.db.cursor.execute('''
					UPDATE commits SET repo_id=(
							SELECT cp.repo_id FROM commit_repos cp
								WHERE cp.commit_id=commits.id
								AND cp.is_orig_repo)
					;
					''')

			self.db.cursor.execute('''INSERT INTO full_updates(update_type) VALUES('commits orig repos repo/package creation date');''')

			self.db.connection.commit()


class IdentitiesFiller(fillers.Filler):
	"""
	Fills in identities from a given list, stored in self.identities_list during the prepare phase
	This wrapper takes a list as input or a filename, but can be inherited for more complicated identities_list construction

	all identities in the list belong to the same identity type, provided as argument.

	CSV file syntax is expected to be, with header:
	identity(e.g. email or login);additional_info (json)
	or
	identity(e.g. email or login)
	"""
	def __init__(self,identity_type,identities_list=None,identities_list_file=None,clean_users=False,**kwargs):
		self.identities_list = identities_list
		self.identities_list_file = identities_list_file
		self.clean_users = clean_users
		self.identity_type = identity_type
		fillers.Filler.__init__(self,**kwargs)

	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder

		if self.identities_list is None:
			with open(os.path.join(self.data_folder,self.identities_list_file),"rb") as f:
				filehash = hashlib.sha256(f.read()).hexdigest()
			self.source = '{}_{}'.format(self.identities_list_file,filehash)
			self.db.register_source(source=self.source)
			with open(os.path.join(self.data_folder,self.identities_list_file),'r') as f:
				reader = csv.reader(f,delimiter=';')
				headers = next(reader) #remove header
				if len(headers) == 2:
					self.identities_list = [r for r in reader]
				elif len(headers) == 1:
					self.identities_list = [(r[0],None) for r in reader]
				else:
					raise ValueError('''Expected syntax:

	identity(e.g. email or login),additional_info (json)
	or
	identity(e.g. email or login)

got: {}'''.format(headers))


	def apply(self):
		self.fill_identities(clean_users=self.clean_users)
		self.db.connection.commit()


	def clean_id_list(self,identities_list):
		ans = []
		for elt in identities_list:
			if isinstance(elt,str):
				identity,info = elt,None
			else:
				identity,info = elt
			if info is not None and not isinstance(info,str):
				ans.append((identity,json.dumps(info)))
			else:
				ans.append((identity,info))
		return ans

	def fill_identities(self,identities_list=None,identity_type=None,clean_users=True):

		if identities_list is None:
			identities_list = self.identities_list
		if identity_type is None:
			identity_type = self.identity_type

		identities_list = self.clean_id_list(identities_list)

		if self.db.db_type == 'postgres':

			self.db.cursor.execute('''
				INSERT INTO identity_types(name) VALUES(%s)
				ON CONFLICT DO NOTHING
				;''',(identity_type,))
			self.db.connection.commit()

			extras.execute_batch(self.db.cursor,'''
				INSERT INTO users(
						creation_identity,
						creation_identity_type_id)
							SELECT %s,id FROM identity_types WHERE name=%s
					AND NOT EXISTS (SELECT 1 FROM identities i
						INNER JOIN identity_types it
						ON i.identity=%s AND i.identity_type_id=it.id AND it.name=%s)
				ON CONFLICT DO NOTHING;
				''',((c[0],identity_type,c[0],identity_type) for c in identities_list))
			self.db.connection.commit()
			extras.execute_batch(self.db.cursor,'''
				INSERT INTO identities(
						attributes,
						identity,
						user_id,
						identity_type_id) SELECT %s,%s,u.id,it.id
						FROM users u
						INNER JOIN identity_types it
						ON it.name=%s AND u.creation_identity=%s AND u.creation_identity_type_id=it.id
				ON CONFLICT DO NOTHING;
				''',((c[1],c[0],identity_type,c[0]) for c in identities_list))
			self.db.connection.commit()



		else:
			self.db.cursor.execute('''
				INSERT OR IGNORE INTO identity_types(name) VALUES(?)
				;''',(identity_type,))
			self.db.connection.commit()

			self.db.cursor.executemany('''
				INSERT OR IGNORE INTO users(
						creation_identity,
						creation_identity_type_id)
							SELECT ?,id FROM identity_types WHERE name=?
					AND NOT EXISTS  (SELECT 1 FROM identities i
						INNER JOIN identity_types it
						ON i.identity=? AND i.identity_type_id=it.id AND it.name=?)
				;
				''',((c[0],identity_type,c[0],identity_type) for c in identities_list))
			self.db.connection.commit()

			self.db.cursor.executemany('''
				INSERT OR IGNORE INTO identities(
						attributes,
						identity,
						user_id,
						identity_type_id) SELECT ?,?,u.id,it.id
						FROM users u
						INNER JOIN identity_types it
						ON it.name=? AND u.creation_identity=? AND u.creation_identity_type_id=it.id
				;
				''',((c[1],c[0],identity_type,c[0]) for c in identities_list))
			self.db.connection.commit()

		if clean_users:
			self.db.clean_users()



class SimilarIdentitiesMerger(fillers.Filler):
	"""
	Merges identities with same value from two given identity_types
	"""
	def __init__(self,identity_type1,identity_type2,**kwargs):
		self.identity_type1 = identity_type1
		self.identity_type2 = identity_type2
		fillers.Filler.__init__(self,**kwargs)


	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder

		if self.db.db_type == 'postgres':
			self.db.cursor.execute('''
				SELECT i1.id,i2.id
				FROM identities i1
				INNER JOIN identity_types it1
				ON i1.identity_type_id =it1.id and it1.name = %(identity_type1)s
				INNER JOIN identities i2
				ON i1.identity = i2.identity AND i1.user_id != i2.user_id
				INNER JOIN identity_types it2
				ON it2.id=i2.identity_type_id AND it2.name = %(identity_type2)s
				;''',{'identity_type1':self.identity_type1,'identity_type2':self.identity_type2})
		else:
			self.db.cursor.execute('''
				SELECT i1.id,i2.id
				FROM identities i1
				INNER JOIN identity_types it1
				ON i1.identity_type_id =it1.id and it1.name = :identity_type1
				INNER JOIN identities i2
				ON i1.identity = i2.identity AND i1.user_id != i2.user_id
				INNER JOIN identity_types it2
				ON it2.id=i2.identity_type_id AND it2.name = :identity_type2
				;''',{'identity_type1':self.identity_type1,'identity_type2':self.identity_type2})

		self.to_merge_list = list(self.db.cursor.fetchall())

	def apply(self):
		self.logger.info('Merging {} couples of similar identities from identity types {} and {}'.format(len(self.to_merge_list),self.identity_type1,self.identity_type2))
		for i1,i2 in self.to_merge_list:
			self.db.merge_identities(identity1=i1,identity2=i2,autocommit=False,reason='Same identity string for both identity types: {} and {}'.format(self.identity_type1,self.identity_type2))
		self.db.connection.commit()

class GithubNoreplyEmailMerger(IdentitiesFiller):
	"""
	Merges identities NUMBER+LOGIN@users.noreply.github.com with LOGIN
	"""
	def __init__(self,**kwargs):
		fillers.Filler.__init__(self,**kwargs)

	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder

		self.db.cursor.execute('''
				SELECT i.id,i.identity FROM identities i
				INNER JOIN identity_types it
				ON it.name='email' AND it.id=i.identity_type_id
				LEFT OUTER JOIN identities i2
				ON i2.user_id=i.user_id AND i.id!=i2.id
				WHERE i.identity LIKE '%@users.noreply.github.com'
				AND i2.id IS NULL
				;''')

		self.to_merge_list = [(i,email,self.parse_email(email))for i,email in self.db.cursor.fetchall()]

	def parse_email(self,email):
		assert email.endswith('@users.noreply.github.com'),email
		ans = email.split('@users.noreply.github.com')[0].split('+')[-1]
		assert len(ans) > 0
		return ans

	def apply(self):
		self.logger.info('Merging {} emails finishing in @users.noreply.github.com with their github_login'.format(len(self.to_merge_list)))
		self.fill_identities(identities_list=list(set([login for i,email,login in self.to_merge_list])),identity_type='github_login',clean_users=False)

		self.db.cursor.execute('''
			SELECT i.id,i.identity FROM identities i
			INNER JOIN identity_types it
				ON it.name='github_login' AND it.id=i.identity_type_id
			;''')
		ghlogin_ids = {login:i for i,login in self.db.cursor.fetchall()}

		for i,email,login in self.to_merge_list:
			self.db.merge_identities(identity1=i,identity2=ghlogin_ids[login],autocommit=False,reason='Parsed email {} as github_login {}'.format(email,login))
		self.db.connection.commit()

