import os
import hashlib
import csv
import copy
import pygit2
import shutil
import datetime
import subprocess

from psycopg2 import extras

from repo_tools import fillers
import repo_tools as rp


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
	def __init__(self,package_list=None,package_list_file=None,**kwargs):
		self.package_list = package_list
		self.package_list_file = package_list_file
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


	def apply(self):
		self.fill_packages()
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
	def __init__(self,source='autofill_repos_from_urls',**kwargs):
		'''

		'''
		self.source = source
		fillers.Filler.__init__(self,**kwargs)

	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder

		self.db.cursor.execute('SELECT id,url_root FROM sources WHERE url_root IS NOT NULL;')
		self.url_roots = list(self.db.cursor.fetchall())

		self.db.cursor.execute('SELECT url FROM urls;')
		# self.urls = [(raw_url,cleaned_url,source_id)]
		self.urls = list(set([(u[0],*self.clean_url(u[0])) for u in self.db.cursor.fetchall()]))
		self.cleaned_urls = list(set([(cleaned_url,source_id) for (raw_url,cleaned_url,source_id) in self.urls if cleaned_url is not None]))

		# source_id,owner,name,cleaned_url
		self.repo_info_list = [(source_id,cleaned_url.split('/')[-2],cleaned_url.split('/')[-1],cleaned_url) for (cleaned_url,source_id) in self.cleaned_urls ]


	def apply(self):
		self.fill_source()
		self.fill_cleaned_urls()
		self.fill_repositories()
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
		for start_str in ['http://','https://','http:/','https:/','www.','/',' ','\n','\t','\r']:
			if repo.startswith(start_str):
				return self.repo_formatting(repo=repo[len(start_str):],source_urlroot=source_urlroot,output_cleaned_url=output_cleaned_url,raise_error=raise_error)

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
	def __init__(self,precheck_cloned=False,force=False,update=False,failed=False,ssh_sources=None,ssh_key=os.path.join(os.environ['HOME'],'.ssh','id_rsa'),sources=None,rm_first=False,**kwargs):
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

		if self.force or self.update:
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
			last_commit_time = datetime.datetime.fromtimestamp(repo_obj.revparse_single('HEAD').commit_time)
			self.db.submit_download_attempt(source=source,owner=owner,repo=repo,success=True,dl_time=last_commit_time)

	def clone(self,source,name,owner,source_urlroot,replace=False,update=False,db=None):
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
				self.logger.info('Git Error for repo {}/{}/{}'.format(source,owner,name))
				success = False
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
			cmd = 'git pull'
			cmd_output = subprocess.check_output(cmd.split(' '),cwd=repo_folder)
			### NB: pygit2 is complex for a simple 'git pull', a solution would be to test such an implementation: https://github.com/MichaelBoselowitz/pygit2-examples/blob/master/examples.py
			# repo_obj.remotes["origin"].fetch(callbacks=callbacks)
			success = True
		# except pygit2.GitError as e:
		except subprocess.CalledProcessError as e:
			self.logger.info('Git Error for repo {}/{}/{}: {}'.format(source,owner,name,e))
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
