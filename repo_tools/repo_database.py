import os
import datetime
import logging
import sqlite3
import glob
import shutil
import uuid
import time

import csv
import copy
import json
import numpy as np
import io

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.INFO)

try:
	import psycopg2
	from psycopg2 import extras
	from psycopg2.extensions import register_adapter, AsIs
	register_adapter(np.float64, AsIs)
	register_adapter(np.int64, AsIs)
except ImportError:
	logger.info('Psycopg2 not installed, pip install psycopg2 (or binary-psycopg2) if you want to use a PostgreSQL DB')


def adapt_array(arr):
    """
    http://stackoverflow.com/a/31312102/190597 (SoulNibbler)
    """
    out = io.BytesIO()
    np.save(out, arr)
    out.seek(0)
    return sqlite3.Binary(out.read())

def convert_array(text):
    out = io.BytesIO(text)
    out.seek(0)
    return np.load(out)

# Converts np.array to TEXT when inserting
sqlite3.register_adapter(np.ndarray, adapt_array)

# Converts TEXT to np.array when selecting
sqlite3.register_converter("array", convert_array)

sqlite3.register_adapter(np.int64, int)

def check_sqlname_safe(s):
    assert s == ''.join( c for c in s if c.isalnum() or c in ('_',) )

class Database(object):
	'''

	This class creates a database object with the main structure, with a few methods  to manipulate it.
	By default SQLite is used, but PostgreSQL is also an option

	To fill it, fillers are used (see Filler class).
	The object uses a specific data folder and a list of files used for the fillers, with name, keyword, and potential download link. (move to filler class?)

	A 'computation_db' can be associated to it (always SQLite, can be in memory) to store temporary measures on repositories and users.
	'''

	def __init__(self,
					db_type='sqlite',
					db_name='repo_tools',
					db_folder='.',
					db_schema=None,
					db_user='postgres',
					port='5432',
					host='localhost',
					data_folder='./datafolder',
					password=None,
					clean_first=False,
					do_init=False,
					timeout=5,
					computation_db_name='repo_tools_computation.db',
					reconnect_on_pickling=False):
		self.reconnect_on_pickling = reconnect_on_pickling
		self.db_type = db_type
		self.logger = logger
		self.db_name = db_name
		if db_type == 'sqlite':
			if db_name.startswith(':memory:'):
				self.connection = sqlite3.connect(db_name, detect_types=sqlite3.PARSE_DECLTYPES)
				self.in_ram = True
			else:
				self.in_ram = False
				self.db_path = os.path.join(db_folder,'{}.db'.format(db_name))
				if not os.path.exists(db_folder):
					os.makedirs(db_folder)
				self.timeout = timeout
				self.connection = sqlite3.connect(self.db_path,timeout=timeout, detect_types=sqlite3.PARSE_DECLTYPES)
			self.cursor = self.connection.cursor()
		elif db_type == 'postgres':
			if db_schema is not None:
				self.db_schema = db_schema
				options = '-c search_path="{}"'.format(db_schema)
			else:
				self.db_schema = None
				options = None
			if password is not None:
				logger.warning('You are providing your password directly, this could be a security concern, consider using solutions like .pgpass file.')
			try:
				self.connection = psycopg2.connect(user=db_user,port=port,host=host,database=db_name,password=password,options=options)
			except psycopg2.OperationalError:
				pgpass_env = 'PGPASSFILE'
				default_pgpass = os.path.join(os.environ['HOME'],'.pgpass')
				if pgpass_env not in os.environ.keys():
					os.environ[pgpass_env] = default_pgpass
					self.logger.info('Password authentication failed,trying to set .pgpass env variable')
					self.connection = psycopg2.connect(user=db_user,port=port,host=host,database=db_name,password=password,options=options)
				else:
					raise
			self.cursor = self.connection.cursor()
		else:
			raise ValueError('Unknown DB type: {}'.format(db_type))

		if clean_first:
			self.clean_db()
		if do_init:
			self.init_db()
		self.fillers = []
		self.data_folder = data_folder
		if not os.path.exists(self.data_folder):
			os.makedirs(self.data_folder)

		if computation_db_name.startswith(':') or os.path.isabs(computation_db_name):
			self.computation_db_name = computation_db_name
		else:
			self.computation_db_name = os.path.join(self.data_folder,computation_db_name)
		#storing info to be able to copy the db and have independent cursor/connection
		self.db_conninfo = {
				'db_type':db_type,
				'db_name':db_name,
				'db_folder':db_folder,
				'db_user':db_user,
				'port':port,
				'host':host,
				'password':password,
				'db_schema':db_schema,
		}

	def __getstate__(self):
		d = self.__dict__.copy()
		del d['connection']
		del d['cursor']

	def __setstate__(self, d):
		if d['reconnect_on_pickling']:
			new = d['__class__'](do_init=False,**d['db_conninfo'])
			self.__dict__ = new.__dict__
		else:
			d['connection'] = None
			d['cursor'] = None
			self.__dict__ = d


	def get_computation_db(self):
		if not hasattr(self,'computation_db'):
			self.computation_db = ComputationDB(db=self)
		return self.computation_db


	def copy(self,timeout=30):
		'''
		Returns a copy, without init, with independent connection and cursor
		'''
		return self.__class__(do_init=False,timeout=timeout,**self.db_conninfo)

	def dump_pg_to_sqlite(self,other_db):
		'''
		Wrapper to move the db (postgres) to another with sqlite.
		Table names are used as variables, and the (self-made) check against injection expects alphanumeric chars or _
		DB structure has to be the same, and especially attribute order.
		Maybe attributes could be retrieved and specified also as variables to be safer in terms of data integrity.
		'''
		if not (self.db_type == 'postgres' and other_db.db_type == 'sqlite'):
			raise NotImplementedError('Dumping is only available from PostgreSQL to SQLite. Trying to dump from {} to {}.'.format(self.db_type,other_db.db_type))
		else:
			self.logger.info('Dumping PostgreSQL {} into SQLite {}'.format(self.db_conninfo['db_name'],other_db.db_conninfo['db_name']))
			self.cursor.execute('''SELECT table_name FROM information_schema.tables
							WHERE table_schema = (SELECT current_schema());''')
			tab_list = [r[0] for r in self.cursor.fetchall()]
			for i,tab in enumerate(tab_list):
				check_sqlname_safe(tab)
				other_db.cursor.execute(''' SELECT * FROM {} LIMIT 1;'''.format(tab))
				res = list(other_db.cursor.fetchall())
				if len(res) == 0:
					self.logger.info('Dumping table {} ({}/{})'.format(tab,i+1,len(tab_list)))
					self.cursor.execute(''' SELECT * from {} LIMIT 1;'''.format(tab))
					resmain_sample = list(self.cursor.fetchall())
					if len(resmain_sample) > 0:
						nb_attr = len(resmain_sample[0])
						self.cursor.execute(''' SELECT * from {};'''.format(tab))
						other_db.cursor.executemany('''INSERT OR IGNORE INTO {} VALUES({});'''.format(tab,','.join(['?' for _ in range(nb_attr)])),self.cursor.fetchall())
						other_db.connection.commit()
				else:
					self.logger.info('Skipping table {} ({}/{})'.format(tab,i+1,len(tab_list)))

	def init_db(self):
		'''
		Initializing the database, with correct tables, constraints and indexes.
		'''
		logger.info('Creating database ({}) table and indexes'.format(self.db_type))
		if self.db_type == 'sqlite':
			with open(os.path.join(os.path.dirname(__file__),'initscript_sqlite.sql'),'r') as f:
				self.DB_INIT = f.read()
			for q in self.DB_INIT.split(';')[:-1]:
				self.cursor.execute(q)

			self.cursor.execute('''INSERT OR IGNORE INTO _dbinfo(info_type,info_content) VALUES('uuid',?) ;''',(str(uuid.uuid1()),))
			self.cursor.execute('''INSERT OR IGNORE INTO _dbinfo(info_type,info_content) VALUES('DB_INIT',?) ;''',(self.DB_INIT,))

		elif self.db_type == 'postgres':

			with open(os.path.join(os.path.dirname(__file__),'initscript_postgres.sql'),'r') as f:
				self.DB_INIT = f.read()
			self.cursor.execute(self.DB_INIT)
			self.cursor.execute('''INSERT INTO _dbinfo(info_type,info_content) VALUES('uuid',%s) ON CONFLICT (info_type) DO NOTHING ;''',(str(uuid.uuid1()),))
			# self.cursor.execute('''INSERT INTO _dbinfo(info_type,info_content) VALUES('DB_INIT',%s) ON CONFLICT (info_type) DO UPDATE SET info_content=EXCLUDED.info_content;''',(self.DB_INIT,))
			self.cursor.execute('''INSERT INTO _dbinfo(info_type,info_content) VALUES('DB_INIT',%s) ON CONFLICT DO NOTHING;''',(self.DB_INIT,))

		self.connection.commit()

	def clean_db(self,sqlite_del=True):
		'''
		Dropping tables
		If there is a change in structure in the init script, this method should be called to 'reset' the state of the database
		'''
		logger.info('Cleaning database')
		if self.db_type == 'sqlite' and not self.in_ram and sqlite_del:
			del self.cursor
			del self.connection
			os.remove(self.db_path)
			self.connection = sqlite3.connect(self.db_path,timeout=self.timeout, detect_types=sqlite3.PARSE_DECLTYPES)
			self.cursor = self.connection.cursor()
		else:
			self.cursor.execute('DROP TABLE IF EXISTS _dbinfo;')
			self.cursor.execute('DROP TABLE IF EXISTS sponsors_listings;')
			self.cursor.execute('DROP TABLE IF EXISTS releases;')
			self.cursor.execute('DROP TABLE IF EXISTS issues;')
			self.cursor.execute('DROP TABLE IF EXISTS sponsors_user;')
			self.cursor.execute('DROP TABLE IF EXISTS sponsors_repo;')
			self.cursor.execute('DROP TABLE IF EXISTS package_version_downloads;')
			self.cursor.execute('DROP TABLE IF EXISTS package_dependencies;')
			self.cursor.execute('DROP TABLE IF EXISTS package_versions;')
			self.cursor.execute('DROP TABLE IF EXISTS filtered_deps_package;')
			self.cursor.execute('DROP TABLE IF EXISTS filtered_deps_repo;')
			self.cursor.execute('DROP TABLE IF EXISTS filtered_deps_repoedges;')
			self.cursor.execute('DROP TABLE IF EXISTS packages;')
			self.cursor.execute('DROP TABLE IF EXISTS followers;')
			self.cursor.execute('DROP TABLE IF EXISTS stars;')
			self.cursor.execute('DROP TABLE IF EXISTS forks;')
			self.cursor.execute('DROP TABLE IF EXISTS commit_repos;')
			self.cursor.execute('DROP TABLE IF EXISTS commit_parents;')
			self.cursor.execute('DROP TABLE IF EXISTS commits;')
			self.cursor.execute('DROP TABLE IF EXISTS table_updates;')
			self.cursor.execute('DROP TABLE IF EXISTS merged_identities;')
			self.cursor.execute('DROP TABLE IF EXISTS merged_repositories;')
			self.cursor.execute('DROP TABLE IF EXISTS identities;')
			self.cursor.execute('DROP TABLE IF EXISTS users;')
			self.cursor.execute('DROP TABLE IF EXISTS identity_types;')
			self.cursor.execute('DROP TABLE IF EXISTS full_updates;')
			self.cursor.execute('DROP TABLE IF EXISTS download_attempts;')
			self.cursor.execute('DROP TABLE IF EXISTS repo_languages;')
			self.cursor.execute('DROP TABLE IF EXISTS repositories;')
			self.cursor.execute('DROP TABLE IF EXISTS urls;')
			self.cursor.execute('DROP TABLE IF EXISTS sources;')
			self.connection.commit()


	def fill_db(self):
		self.connection.commit()
		for f in self.fillers:
			if not f.done:
				f.prepare()
				self.logger.info('Prepared filler {}'.format(f.name))
				if not f.done:
					f.apply()
					f.done = True
					self.logger.info('Filled with filler {}'.format(f.name))
			else:
				self.logger.info('Already filled with filler {}, skipping'.format(f.name))
		self.connection.commit()

	def add_filler(self,f):
		if f.name in [ff.name for ff in self.fillers if ff.unique_name]:
			self.logger.warning('Filler {} already present'.format(f.name))
			return
		f.db = self
		self.fillers.append(f)
		f.logger = self.logger
		self.logger.info('Added filler {}'.format(f.name))

	def register_repo(self,source,owner,repo,cloned=False):
		'''
		Putting a repo in the database
		'''
		if self.db_type == 'postgres':
			self.cursor.execute(''' INSERT INTO repositories(source,owner,name,cloned)
				 VALUES((SELECT id FROM sources WHERE name=%s),
								%s,
								%s,
								%s) ON CONFLICT DO NOTHING; ''',(source,owner,repo,cloned))
		else:
			self.cursor.execute(''' INSERT OR IGNORE INTO repositories(source,owner,name,cloned)
				 VALUES((SELECT id FROM sources WHERE name=?),
								?,
								?,
								?);''',(source,owner,repo,cloned))
		self.connection.commit()

	def register_source(self,source,source_urlroot=None):
		'''
		Putting a source in the database
		'''
		if self.db_type == 'postgres':
			self.cursor.execute(''' INSERT INTO sources(name,url_root)
				 VALUES(%s,%s) ON CONFLICT DO NOTHING;''',(source,source_urlroot))
		else:
			self.cursor.execute(''' INSERT OR IGNORE INTO sources(name,url_root)
				 VALUES(?,?);''',(source,source_urlroot))
		self.connection.commit()

	def register_urls(self,source,url_list):
		'''
		Registering URLs and potentially their cleaned version in the database
		url_list should be [(url,cleaned_url,source_root_id)] # source is the source of the url (eg crates), source_root is the repository system source (eg github)
		but if [url], completed by [(url,None,None)]
		'''
		if len(url_list)>0 and isinstance(url_list[0],str):
			url_list = [(url,None,None) for url in url_list]

		if self.db_type == 'postgres':
			extras.execute_batch(self.cursor,''' INSERT INTO urls(source,source_root,url)
				 VALUES((SELECT id FROM sources WHERE name=%s),
				 				%s,
								%s) ON CONFLICT(url) DO NOTHING;''',((source,source_root_id,url_cleaned) for url,url_cleaned,source_root_id  in url_list if url_cleaned is not None))
			extras.execute_batch(self.cursor,''' UPDATE urls SET cleaned_url=id WHERE url=%s ;''',((url_cleaned,) for url,url_cleaned,source_root_id  in url_list if url_cleaned is not None))
			extras.execute_batch(self.cursor,''' UPDATE urls SET source=(SELECT id FROM sources WHERE name=%s),source_root=%s WHERE url=%s ;''',((source,source_root_id,url,) for url,url_cleaned,source_root_id  in url_list if url_cleaned is not None))
			extras.execute_batch(self.cursor,''' UPDATE urls SET source=(SELECT id FROM sources WHERE name=%s),source_root=%s WHERE url=%s ;''',((source,source_root_id,url_cleaned,) for url,url_cleaned,source_root_id  in url_list if url_cleaned is not None))
			extras.execute_batch(self.cursor,''' INSERT INTO urls(source,source_root,url,cleaned_url)
				 VALUES((SELECT id FROM sources WHERE name=%s),
				 				%s,
								%s,(SELECT id FROM urls WHERE url=%s)) ON CONFLICT(url) DO UPDATE
								SET cleaned_url=excluded.cleaned_url;''',((source,source_root_id,url,url_cleaned) for url,url_cleaned,source_root_id  in url_list))
		else:
			self.cursor.executemany(''' INSERT OR IGNORE INTO urls(source,source_root,url)
				 VALUES((SELECT id FROM sources WHERE name=?),
				 				?,
								?);''',((source,source_root_id,url_cleaned) for url,url_cleaned,source_root_id in url_list if url_cleaned is not None))
			self.cursor.executemany(''' UPDATE urls SET cleaned_url=id WHERE url=?;''',((url_cleaned,) for url,url_cleaned,source_root_id in url_list if url_cleaned is not None))
			self.cursor.executemany(''' UPDATE urls SET source=(SELECT id FROM sources WHERE name=?),source_root=? WHERE url=?;''',((source,source_root_id,url,) for url,url_cleaned,source_root_id in url_list if url_cleaned is not None))
			self.cursor.executemany(''' UPDATE urls SET source=(SELECT id FROM sources WHERE name=?),source_root=? WHERE url=?;''',((source,source_root_id,url_cleaned,) for url,url_cleaned,source_root_id in url_list if url_cleaned is not None))
			self.cursor.executemany(''' INSERT OR REPLACE INTO urls(source,source_root,url,cleaned_url)
				 VALUES((SELECT id FROM sources WHERE name=?),
				 				?,
								?,(SELECT id FROM urls WHERE url=?));''',((source,source_root_id,url,url_cleaned) for url,url_cleaned,source_root_id in url_list))

		self.connection.commit()

	def register_url(self,source,repo_url,repo_id=None,clean_info=None): # DEPRECATED
		'''
		Putting URLs in the database
		'''
		if clean_info is None:
			self.register_urls(source=source,url_list=[repo_url])
		else:
			self.register_urls(source=source,url_list=[(repo_url,*clean_info)])
	# 	if self.db_type == 'postgres':
	# 		self.cursor.execute(''' INSERT INTO urls(source,repo_url,repo_id)
	# 			 VALUES((SELECT id FROM sources WHERE name=%s),
	# 							%s,%s) ON CONFLICT DO NOTHING;''',(source,repo_url,repo_id))
	# 	else:
	# 		self.cursor.execute(''' INSERT OR IGNORE INTO urls(source,repo_url,repo_id)
	# 			 VALUES((SELECT id FROM sources WHERE name=?),
	# 							?,?);''',(source,repo_url,repo_id))
	# 	self.connection.commit()

	# def update_url(self,source,repo_url,repo_id):
	# 	'''
	# 	Updating a URL in the database
	# 	'''
	# 	if self.db_type == 'postgres':
	# 		self.cursor.execute(''' UPDATE urls SET repo_id=%s WHERE
	# 			 source=(SELECT id FROM sources WHERE name=%s
	# 							AND repo_url=%s);''',(repo_id,source,repo_url))
	# 	else:
	# 		self.cursor.execute(''' UPDATE urls SET repo_id=? WHERE
	# 			 source=(SELECT id FROM sources WHERE name=?
	# 							AND repo_url=?);''',(repo_id,source,repo_url))
	# 	self.connection.commit()


	def register_repositories(self,repo_info_list):
		'''
		repo_info_list syntax:
		source_id, owner, name, url
		'''
		if self.db_type == 'postgres':
			extras.execute_batch(self.cursor,'''
				INSERT INTO repositories(source,owner,name,url_id) VALUES(
				%s,%s,%s,(SELECT id FROM urls WHERE url=%s)
				) ON CONFLICT DO NOTHING
				;''',repo_info_list)
		else:
			self.cursor.executemany('''
				INSERT OR IGNORE INTO repositories(source,owner,name,url_id) VALUES(
				?,?,?,(SELECT id FROM urls WHERE url=?)
				)
				;''',repo_info_list)
		self.connection.commit()


	def register_packages(self,source,package_list,autocommit=True,update_urls=False):
		'''
		Registering packages from package list
		URLs are supposed to be already filled

		syntax of package list:
		package id (in source), package name, created_at (datetime.datetime),repo_url

		'''
		source_id = self.get_source_info(source=source)[0]
		if self.db_type == 'postgres':
			extras.execute_batch(self.cursor,'''
				INSERT INTO packages(repo_id,source_id,insource_id,name,created_at,url_id)
				VALUES(
					(SELECT r.id FROM urls u
						INNER JOIN repositories r ON r.url_id=u.cleaned_url
						AND u.url=%s),
				%s,%s,%s,%s,(SELECT id FROM urls WHERE url=%s))
				ON CONFLICT DO NOTHING
				;''',((p[-1],source_id,*p) for p in package_list))
			if update_urls:
				extras.execute_batch(self.cursor,'''
					UPDATE packages SET repo_id=
						(SELECT r.id FROM urls u
							INNER JOIN repositories r ON r.url_id=u.cleaned_url
							AND u.url=%s),
						url_id=(SELECT id FROM urls WHERE url=%s)
					WHERE (url_id IS NULL OR repo_id IS NULL) AND insource_id=%s
					;''',((p[-1],p[-1],p[0]) for p in package_list))

		else:
			self.cursor.executemany('''
				INSERT OR IGNORE INTO packages(repo_id,source_id,insource_id,name,created_at,url_id)
				VALUES(
					(SELECT r.id FROM urls u
						INNER JOIN repositories r ON r.url_id=u.cleaned_url
						AND u.url=?),?,?,?,?,(SELECT id FROM urls WHERE url=?))
				;''',((p[-1],source_id,*p) for p in package_list))
			if update_urls:
				self.cursor.executemany('''
					UPDATE packages SET repo_id=
						(SELECT r.id FROM urls u
							INNER JOIN repositories r ON r.url_id=u.cleaned_url
							AND u.url=?),
						url_id=(SELECT id FROM urls WHERE url=?)
					WHERE (url_id IS NULL or repo_id IS NULL) AND insource_id=?
					;''',((p[-1],p[-1],p[0]) for p in package_list))

		if autocommit:
			self.connection.commit()

	def check_repo_id(self,repo_id):
		'''
		Checks if a repo id is still in the DB through query
		'''
		if self.db_type == 'postgres':
			self.cursor.execute('''
				SELECT id FROM repositories
				WHERE id=%s
				;''',(repo_id,))
		else:
			self.cursor.execute('''
				SELECT id FROM repositories
				WHERE id=?
				;''',(repo_id,))

		ans = self.cursor.fetchone()
		if ans is None:
			return None
		else:
			return ans[0]


	def get_repo_id(self,owner,name,source):
		'''
		Getting repo id, None if not in DB
		Dealing with source is str or int.
		If source is None, checking if only one match
		'''
		if isinstance(source,str):
			if self.db_type == 'postgres':
				self.cursor.execute(''' SELECT r.id FROM repositories r
										INNER JOIN sources s
										ON r.source=s.id
										AND s.name=%s
										AND r.owner=%s
										AND r.name=%s;''',(source,owner,name))
			else:
				self.cursor.execute(''' SELECT r.id FROM repositories r
										INNER JOIN sources s
										ON r.source=s.id
										AND s.name=?
										AND r.owner=?
										AND r.name=?;''',(source,owner,name))
			repo_id = self.cursor.fetchone()
			if repo_id is None:
				return None
			else:
				return repo_id[0]
		elif isinstance(source,int):
			if self.db_type == 'postgres':
				self.cursor.execute(''' SELECT id FROM repositories WHERE
										source=%s
										AND owner=%s
										AND name=%s;''',(source,owner,name))
			else:
				self.cursor.execute(''' SELECT id FROM repositories WHERE
										source=?
										AND owner=?
										AND name=?;''',(source,owner,name))
			repo_id = self.cursor.fetchone()
			if repo_id is None:
				return None
			else:
				return repo_id[0]
		elif source is None:
			if self.db_type == 'postgres':
				self.cursor.execute(''' SELECT id FROM repositories WHERE
										owner=%s
										AND name=%s;''',(owner,name))
			else:
				self.cursor.execute(''' SELECT id FROM repositories WHERE
										owner=?
										AND name=?;''',(owner,name))
			ans = list(self.cursor.fetchall())
			if len(ans) == 0:
				return None
			elif len(ans) == 1:
				return ans[0][0]
			else:
				raise ValueError('Project {}/{} could not be identified without specifying the source, {} projects found'.format(owner,name,len(ans)))


	def submit_download_attempt(self,source,owner,repo,success,dl_time=None):
		'''
		Registers a repository if not already done, plus the download attempt
		'''
		#Getting repo id
		repo_id = self.get_repo_id(name=repo,source=source,owner=owner)

		#creating if not existing
		if repo_id is None:
			if self.db_type == 'postgres':
				self.cursor.execute(''' INSERT INTO repositories(source,owner,name)
					 VALUES((SELECT id FROM sources WHERE name=%s),
									%s,
									%s);''',(source,owner,repo))
			else:
				self.cursor.execute(''' INSERT INTO repositories(source,owner,name)
					 VALUES((SELECT id FROM sources WHERE name=?),
									?,
									?);''',(source,owner,repo))
			repo_id = self.get_repo_id(name=repo,source=source,owner=owner)

		#inserting download attempt
		if dl_time is None:
			if self.db_type == 'postgres':
				self.cursor.execute(''' INSERT INTO table_updates(repo_id,table_name,success)
				 VALUES(%s,'clones',%s);''',(repo_id,success))
				if success:
					self.cursor.execute(''' UPDATE repositories SET updated_at=(SELECT CURRENT_TIMESTAMP), cloned=true
				WHERE id=%s;''',(repo_id,))

			else:
				self.cursor.execute(''' INSERT INTO table_updates(repo_id,table_name,success)
				 VALUES(?,'clones',?);''',(repo_id,success))
				if success:
					self.cursor.execute(''' UPDATE repositories SET updated_at=(SELECT CURRENT_TIMESTAMP), cloned=1
				WHERE id=?;''',(repo_id,))
		else:
			if self.db_type == 'postgres':
				self.cursor.execute(''' INSERT INTO table_updates(repo_id,table_name,success,updated_at)
				 VALUES(%s,'clones',%s,%s);''',(repo_id,success,dl_time))
				if success:
					self.cursor.execute(''' UPDATE repositories SET updated_at=%s, cloned=true
				WHERE id=%s;''',(dl_time,repo_id,))

			else:
				self.cursor.execute(''' INSERT INTO table_updates(repo_id,table_name,success,updated_at)
				 VALUES(?,'clones',?,?);''',(repo_id,success,dl_time))
				if success:
					self.cursor.execute(''' UPDATE repositories SET updated_at=?, cloned=1
				WHERE id=?;''',(dl_time,repo_id,))
		self.connection.commit()

	def get_repo_list(self,option='all'):
		'''
		Getting a list of source,source_urlroot,owner,name
		'''
		if option == 'all':
			self.cursor.execute('''
				SELECT s.name,s.url_root,r.owner,r.name
				FROM repositories r
				INNER JOIN sources s
				ON s.id=r.source
				ORDER BY s.name,r.owner,r.name
				;''')
			return list(self.cursor.fetchall())
		elif option == 'only_cloned':
			self.cursor.execute('''
				SELECT s.name,s.url_root,r.owner,r.name
				FROM repositories r
				INNER JOIN sources s
				ON s.id=r.source AND r.cloned
				ORDER BY s.name,r.owner,r.name
				;''')
			return list(self.cursor.fetchall())
		elif option == 'only_not_cloned':
			self.cursor.execute('''
				SELECT s.name,s.url_root,r.owner,r.name
				FROM repositories r
				INNER JOIN sources s
				ON s.id=r.source AND NOT r.cloned
				ORDER BY s.name,r.owner,r.name
				;''')
			return list(self.cursor.fetchall())
		elif option == 'basicinfo_dict':
			self.cursor.execute('''
				SELECT s.name,r.owner,r.name,r.id
				FROM repositories r
				INNER JOIN sources s
				ON s.id=r.source
				ORDER BY s.name,r.owner,r.name
				;''')
			return [{'source':r[0],'owner':r[1],'name':r[2],'repo_id':r[3]} for r in self.cursor.fetchall()]

		elif option == 'basicinfo_dict_time':
			if self.db_type == 'postgres':
				self.cursor.execute('''
					SELECT s.name,r.owner,r.name,r.id,extract(epoch from r.latest_commit_time)
					FROM repositories r
					INNER JOIN sources s
					ON s.id=r.source
					ORDER BY s.name,r.owner,r.name
					;''')
			else:
				self.cursor.execute('''
					SELECT s.name,r.owner,r.name,r.id,CAST(strftime('%s', r.latest_commit_time) AS INTEGER)
					FROM repositories r
					INNER JOIN sources s
					ON s.id=r.source
					ORDER BY s.name,r.owner,r.name
					;''')

			return [{'source':r[0],'owner':r[1],'name':r[2],'repo_id':r[3],'after_time':r[4]} for r in self.cursor.fetchall()]
		elif option == 'basicinfo_dict_cloned':
			self.cursor.execute('''
				SELECT s.name,r.owner,r.name,r.id
				FROM repositories r
				INNER JOIN sources s
				ON s.id=r.source AND r.cloned
				ORDER BY s.name,r.owner,r.name
				;''')
			return [{'source':r[0],'owner':r[1],'name':r[2],'repo_id':r[3]} for r in self.cursor.fetchall()]

		elif option == 'basicinfo_dict_time_cloned':
			if self.db_type == 'postgres':
				self.cursor.execute('''
					SELECT s.name,r.owner,r.name,r.id,extract(epoch from r.latest_commit_time)
					FROM repositories r
					INNER JOIN sources s
					ON s.id=r.source AND r.cloned
					ORDER BY s.name,r.owner,r.name
					;''')
			else:
				self.cursor.execute('''
					SELECT s.name,r.owner,r.name,r.id,CAST(strftime('%s', r.latest_commit_time) AS INTEGER)
					FROM repositories r
					INNER JOIN sources s
					ON s.id=r.source AND r.cloned
					ORDER BY s.name,r.owner,r.name
					;''')

			return [{'source':r[0],'owner':r[1],'name':r[2],'repo_id':r[3],'after_time':r[4]} for r in self.cursor.fetchall()]

		elif option == 'starinfo_dict':
			self.cursor.execute('''
				SELECT s.name,r.owner,r.name,r.id,MAX(tu.updated_at)
				FROM repositories r
				INNER JOIN sources s
				ON s.id=r.source
				LEFT OUTER JOIN table_updates tu
				ON tu.repo_id=r.id AND tu.table_name='stars'
				GROUP BY s.name,r.owner,r.name
				ORDER BY s.name,r.owner,r.name
				;''')
			return [{'source':r[0],'owner':r[1],'name':r[2],'repo_id':r[3],'last_star_update':r[4]} for r in self.cursor.fetchall()]

		elif option == 'starinfo':
			self.cursor.execute('''
				SELECT t1.sname,t1.rowner,t1.rname,t1.rid,t1.updated,t1.succ FROM
					(SELECT s.name AS sname,r.owner AS rowner,r.name AS rname,r.id AS rid,tu.updated_at AS updated,tu.success AS succ
						FROM repositories r
						INNER JOIN sources s
						ON s.id=r.source
						LEFT OUTER JOIN table_updates tu
						ON tu.repo_id=r.id AND tu.table_name='stars'
						ORDER BY r.owner,r.name,tu.updated_at ) as t1
					INNER JOIN
						(SELECT s.name AS sname,r.owner AS rowner,r.name AS rname,r.id AS rid,max(tu.updated_at) AS updated
						FROM repositories r
						INNER JOIN sources s
						ON s.id=r.source
						LEFT OUTER JOIN table_updates tu
						ON tu.repo_id=r.id AND tu.table_name='stars'
						GROUP BY r.owner,r.name,r.id,s."name" ) AS t2
				ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.rid=t2.rid
				ORDER BY t1.sname,t1.rowner,t1.rname
				;''')
			return list(self.cursor.fetchall())

		elif option == 'forkinfo':
			self.cursor.execute('''
				SELECT t1.sname,t1.rowner,t1.rname,t1.rid,t1.updated,t1.succ FROM
					(SELECT s.name AS sname,r.owner AS rowner,r.name AS rname,r.id AS rid,tu.updated_at AS updated,tu.success AS succ
						FROM repositories r
						INNER JOIN sources s
						ON s.id=r.source
						LEFT OUTER JOIN table_updates tu
						ON tu.repo_id=r.id AND tu.table_name='forks'
						ORDER BY r.owner,r.name,tu.updated_at ) as t1
					INNER JOIN
						(SELECT s.name AS sname,r.owner AS rowner,r.name AS rname,r.id AS rid,max(tu.updated_at) AS updated
						FROM repositories r
						INNER JOIN sources s
						ON s.id=r.source
						LEFT OUTER JOIN table_updates tu
						ON tu.repo_id=r.id AND tu.table_name='forks'
						GROUP BY r.owner,r.name,r.id,s."name" ) AS t2
				ON (t1.updated=t2.updated or t2.updated IS NULL) AND t1.rid=t2.rid
				ORDER BY t1.sname,t1.rowner,t1.rname
				;''')
			return list(self.cursor.fetchall())

		elif option == 'no_dl':

			self.cursor.execute('''
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
			return list(self.cursor.fetchall())

		else:
			raise ValueError('Unknown option for repo_list: {}'.format(option))

	def get_user_id(self,user_id=None,identity_id=None):
		'''
		Gets an id for the user with provided info. Raises an error if not found.
		Accepted syntaxes:
		(user_id and identity_id cannot be both None)
		user_id:
		  int -> return
		  other type: sent to identity_id
		identity_id:
		  int -> get user id
		  str -> check that only one user id corresponds, otherwise throw specific error
		  (int,<any>)-> return int
		  (str,None) -> call same for str
		  (str,int)
		  (str,str)
		'''
		if user_id is None and identity_id is None:
			raise ValueError('user_id and identity_id cannot both be None')
		elif isinstance(user_id,int):
			return user_id
		elif user_id is not None:
			return self.get_user_id(user_id=None,identity_id=user_id)
		elif isinstance(identity_id,int):
			if self.db_type == 'postgres':
				self.cursor.execute('''
					SELECT i.user_id FROM identities i
					WHERE i.id=%s
					;''',(identity_id,))
			else:
				self.cursor.execute('''
					SELECT i.user_id FROM identities i
					WHERE i.id=?
					;''',(identity_id,))
			ans = self.cursor.fetchone()
			if ans is not None:
				return ans[0]
		elif isinstance(identity_id,str):
			if self.db_type == 'postgres':
				self.cursor.execute('''
					SELECT DISTINCT i.user_id FROM identities i
					WHERE i.identity=%s
					;''',(identity_id,))
			else:
				self.cursor.execute('''
					SELECT DISTINCT i.user_id FROM identities i
					WHERE i.identity=?
					;''',(identity_id,))
			ans = list(self.cursor.fetchall())
			if len(ans) == 1:
				return ans[0][0]
			elif len(ans) > 1:
				raise ValueError('Several identities corresponding to several users ({}) fit this identity:{}'.format(len(ans),identity_id))
		elif isinstance(identity_id,tuple) or isinstance(identity_id,list):
			identity,identity_type = tuple(identity_id)
			if isinstance(identity,str) and isinstance(identity_type,int):
				if self.db_type == 'postgres':
					self.cursor.execute('''
						SELECT i.user_id FROM identities i
						WHERE i.identity=%s
						AND i.identity_type_id=%s
						;''',(identity,identity_type))
				else:
					self.cursor.execute('''
						SELECT i.user_id FROM identities i
						WHERE i.identity=?
						AND i.identity_type_id=?
						;''',(identity,identity_type))
				ans = self.cursor.fetchone()
				if ans is not None:
					return ans[0]
			elif isinstance(identity,str) and isinstance(identity_type,str):
				if self.db_type == 'postgres':
					self.cursor.execute('''
						SELECT i.user_id FROM identities i
						INNER JOIN identity_types it
						ON i.identity=%s
						AND i.identity_type_id=it.id
						AND it.name=%s
						;''',(identity,identity_type))
				else:
					self.cursor.execute('''
						SELECT i.user_id FROM identities i
						INNER JOIN identity_types it
						ON i.identity=?
						AND i.identity_type_id=it.id
						AND it.name=?
						;''',(identity,identity_type))
				ans = self.cursor.fetchone()
				if ans is not None:
					return ans[0]
			else:
				return self.get_user_id(identity_id=identity)
		else:
			raise ValueError('User not found or not parsed, user_id: {},identity_id: {}'.format(user_id,identity_id))

	def get_user_list(self,option='all',time_delay=24*3600):
		'''
		Getting a list of users depending on different conditions and patterns
		time delay is used only for getting the followers, is in seconds, and returns logins that dont have a value for followers from less then 'time_delay' seconds ago
		'''
		if option == 'all':
			self.cursor.execute('''
				SELECT u.id,u.email,u.github_login
				FROM users u
				;''')
			return list(self.cursor.fetchall())
		elif option == 'id_sha_all':
			if self.db_type == 'postgres':
				self.cursor.execute('''
					SELECT u.id,c.repo_id,c.sha
					FROM users u
					JOIN LATERAL (SELECT cc.sha,cc.repo_id FROM commits cc
						WHERE cc.author_id=u.id ORDER BY cc.created_at DESC LIMIT 1) AS c
					ON u.github_login IS NULL
					;''')
			else:
				self.cursor.execute('''
					SELECT u.id,c.repo_id,c.sha
					FROM users u
					JOIN commits c
						ON u.github_login IS NULL AND
						c.id IN (SELECT cc.id FROM commits cc
							WHERE cc.author_id=u.id ORDER BY cc.created_at DESC LIMIT 1)
					;''')

			return list(self.cursor.fetchall())

		elif option == 'id_sha':
			if self.db_type == 'postgres':
				self.cursor.execute('''
					SELECT u.id,c.repo_id,c.sha
					FROM (
						SELECT uu.id FROM
					 		(SELECT uuu.id FROM users uuu
							WHERE uuu.github_login IS NULL) AS uu
							LEFT JOIN table_updates tu
							ON tu.user_id=uu.id AND tu.table_name='login'
							GROUP BY uu.id,tu.user_id
							HAVING tu.user_id IS NULL
						) AS u
					JOIN LATERAL (SELECT cc.sha,cc.repo_id FROM commits cc
						WHERE cc.author_id=u.id ORDER BY cc.created_at DESC LIMIT 1) AS c
					ON true
					;''')
			else:
				self.cursor.execute('''
					SELECT u.id,c.repo_id,c.sha
					FROM (
						SELECT uu.id FROM
					 		(SELECT uuu.id FROM users uuu
							WHERE uuu.github_login IS NULL) AS uu
							LEFT JOIN table_updates tu
							ON tu.user_id=uu.id AND tu.table_name='login'
							GROUP BY uu.id,tu.user_id
							HAVING tu.user_id IS NULL
						) AS u
					JOIN commits c
						ON
						c.id IN (SELECT cc.id FROM commits cc
							WHERE cc.author_id=u.id ORDER BY cc.created_at DESC LIMIT 1)
					;''')
			return list(self.cursor.fetchall())


		elif option == 'id_sha_repoinfo_all':
			if self.db_type == 'postgres':
				self.cursor.execute('''
					SELECT u.id,c.repo_id,r.owner,r.name,c.sha
					FROM users u
					JOIN LATERAL (SELECT cc.sha,cc.repo_id FROM commits cc
						WHERE cc.author_id=u.id ORDER BY cc.created_at DESC LIMIT 1) AS c
					ON u.github_login IS NULL
					INNER JOIN repositories r
					ON r.id=c.repo_id
					;''')
			else:
				self.cursor.execute('''
					SELECT u.id,c.repo_id,r.owner,r.name,c.sha
					FROM users u
					JOIN commits c
						ON u.github_login IS NULL AND
						c.id IN (SELECT cc.id FROM commits cc
							WHERE cc.author_id=u.id ORDER BY cc.created_at DESC LIMIT 1)
					INNER JOIN repositories r
					ON r.id=c.repo_id
					;''')

			return list(self.cursor.fetchall())

		elif option == 'id_sha_repoinfo':
			if self.db_type == 'postgres':
				self.cursor.execute('''
					SELECT u.id,c.repo_id,r.owner,r.name,c.sha
					FROM (
						SELECT uu.id FROM
					 		(SELECT uuu.id FROM users uuu
							WHERE uuu.github_login IS NULL) AS uu
							LEFT JOIN table_updates tu
							ON tu.user_id=uu.id AND tu.table_name='login'
							GROUP BY uu.id,tu.user_id
							HAVING tu.user_id IS NULL
						) AS u
					JOIN LATERAL (SELECT cc.sha,cc.repo_id FROM commits cc
						WHERE cc.author_id=u.id ORDER BY cc.created_at DESC LIMIT 1) AS c
					ON true
					INNER JOIN repositories r
					ON r.id=c.repo_id
					;''')
			else:
				self.cursor.execute('''
					SELECT u.id,c.repo_id,r.owner,r.name,c.sha
					FROM (
						SELECT uu.id FROM
					 		(SELECT uuu.id FROM users uuu
							WHERE uuu.github_login IS NULL) AS uu
							LEFT JOIN table_updates tu
							ON tu.user_id=uu.id AND tu.table_name='login'
							GROUP BY uu.id,tu.user_id
							HAVING tu.user_id IS NULL
						) AS u
					JOIN commits c
						ON
						c.id IN (SELECT cc.id FROM commits cc
							WHERE cc.author_id=u.id ORDER BY cc.created_at DESC LIMIT 1)
					INNER JOIN repositories r
					ON r.id=c.repo_id
					;''')
			return list(self.cursor.fetchall())

		elif option == 'logins':
			if self.db_type == 'postgres':
				self.cursor.execute('''
					SELECT u.github_login FROM
						(SELECT DISTINCT uu.github_login FROM users uu
						WHERE uu.github_login IS NOT NULL) AS u
					LEFT JOIN followers f
					ON f.github_login=u.github_login
					AND now() - f.created_at < %s*'1 second'::interval
					GROUP BY u.github_login,f.github_login
					HAVING f.github_login IS NULL
					;''',(time_delay,))
			else:
				self.cursor.execute('''
					SELECT u.github_login FROM
						(SELECT DISTINCT uu.github_login FROM users uu
						WHERE uu.github_login IS NOT NULL) AS u
					LEFT JOIN followers f
					ON f.github_login=u.github_login
					AND (julianday('now') - julianday(f.created_at))*24*3600 < ?
					GROUP BY u.github_login,f.github_login
					HAVING f.github_login IS NULL
					;''',(time_delay,))

			return [r[0] for r in self.cursor.fetchall()]

		else:
			raise ValueError('Unknown option for user_list: {}'.format(option))



	def fill_authors(self,commit_info_list,autocommit=True):
		'''
		Creating table if necessary.
		Filling authors in table.

		Defining a wrapper around the commit list generator to keep track of data
		Using generator and not lists to be able to deal with high volumes, and lets choice to caller to provide a list or generator.
		'''


		tracked_data = {'latest_commit_time':0,'empty':True}
		def tracked_gen(orig_gen):
			for c in orig_gen:
				tracked_data['empty'] = False
				tracked_data['last_commit'] = c
				tracked_data['latest_commit_time'] = max(tracked_data['latest_commit_time'],c['time'])
				yield c

		if self.db_type == 'postgres':
			extras.execute_batch(self.cursor,'''
				INSERT INTO users(name,email) VALUES(%s,%s)
				ON CONFLICT DO NOTHING;
				''',((c['author_name'],c['author_email']) for c in tracked_gen(commit_info_list)))



		else:
			self.cursor.executemany('''
				INSERT OR IGNORE INTO users(name,email) VALUES(?,?)
				;
				''',((c['author_name'],c['author_email']) for c in tracked_gen(commit_info_list)))


		if not tracked_data['empty']:
			repo_id = tracked_data['last_commit']['repo_id']
			latest_commit_time = datetime.datetime.fromtimestamp(tracked_data['latest_commit_time'])
			if self.db_type == 'postgres':
				self.cursor.execute('''INSERT INTO table_updates(repo_id,table_name,latest_commit_time) VALUES(%s,'users',%s) ;''',(repo_id,latest_commit_time))
			else:
				self.cursor.execute('''INSERT INTO table_updates(repo_id,table_name,latest_commit_time) VALUES(?,'users',?) ;''',(repo_id,latest_commit_time))


		if autocommit:
			self.connection.commit()

	def fill_commits(self,commit_info_list,autocommit=True):
		'''
		Creating table if necessary.
		Filling commits in table.
		'''

		tracked_data = {'latest_commit_time':0,'empty':True}
		def tracked_gen(orig_gen):
			for c in orig_gen:
				tracked_data['last_commit'] = c
				tracked_data['empty'] = False
				tracked_data['latest_commit_time'] = max(tracked_data['latest_commit_time'],c['time'])
				yield c

		if self.db_type == 'postgres':
			extras.execute_batch(self.cursor,'''
				INSERT INTO commits(sha,author_id,repo_id,created_at,insertions,deletions)
					VALUES(%s,
							(SELECT id FROM users WHERE email=%s),
							%s,
							%s,
							%s,
							%s
							)
				ON CONFLICT DO NOTHING;
				''',((c['sha'],c['author_email'],c['repo_id'],datetime.datetime.fromtimestamp(c['time']),c['insertions'],c['deletions'],) for c in tracked_gen(commit_info_list)))

		else:
			self.cursor.executemany('''
				INSERT OR IGNORE INTO commits(sha,author_id,repo_id,created_at,insertions,deletions)
					VALUES(?,
							(SELECT id FROM users WHERE email=?),
							?,
							?,
							?,
							?
							);
				''',((c['sha'],c['author_email'],c['repo_id'],datetime.datetime.fromtimestamp(c['time']),c['insertions'],c['deletions'],) for c in tracked_gen(commit_info_list)))

		if not tracked_data['empty']:
			repo_id = tracked_data['last_commit']['repo_id']
			latest_commit_time = datetime.datetime.fromtimestamp(tracked_data['latest_commit_time'])
			if self.db_type == 'postgres':
				self.cursor.execute('''INSERT INTO table_updates(repo_id,table_name,latest_commit_time) VALUES(%s,'commits',%s) ;''',(repo_id,latest_commit_time))
			else:
				self.cursor.execute('''INSERT INTO table_updates(repo_id,table_name,latest_commit_time) VALUES(?,'commits',?) ;''',(repo_id,latest_commit_time))



		if autocommit:
			self.connection.commit()

	def fill_commit_parents(self,commit_info_list,autocommit=True):
		'''
		Creating table if necessary.
		Filling commit parenthood in table.
		'''

		tracked_data = {'latest_commit_time':0,'empty':True}
		def transformed_list(orig_gen):
			for c in orig_gen:
				tracked_data['last_commit'] = c
				tracked_data['empty'] = False
				tracked_data['latest_commit_time'] = max(tracked_data['latest_commit_time'],c['time'])
				c_id = c['sha']
				for r,p_id in enumerate(c['parents']):
					yield (c_id,p_id,r)

		if self.db_type == 'postgres':
			extras.execute_batch(self.cursor,'''
				INSERT INTO commit_parents(child_id,parent_id,rank)
					VALUES(
							(SELECT id FROM commits WHERE sha=%s),
							(SELECT id FROM commits WHERE sha=%s),
							%s)
				ON CONFLICT DO NOTHING;
				''',transformed_list(commit_info_list))

		else:
			self.cursor.executemany('''
				INSERT OR IGNORE INTO commit_parents(child_id,parent_id,rank)
					VALUES(
							(SELECT id FROM commits WHERE sha=?),
							(SELECT id FROM commits WHERE sha=?),
							?);
				''',transformed_list(commit_info_list))

		if not tracked_data['empty']:
			repo_id = tracked_data['last_commit']['repo_id']
			latest_commit_time = datetime.datetime.fromtimestamp(tracked_data['latest_commit_time'])
			if self.db_type == 'postgres':
				self.cursor.execute('''INSERT INTO table_updates(repo_id,table_name,latest_commit_time) VALUES(%s,'commit_parents',%s) ;''',(repo_id,latest_commit_time))
				self.cursor.execute('''UPDATE repositories SET latest_commit_time=%s WHERE id=%s;''',(latest_commit_time,repo_id))
			else:
				self.cursor.execute('''INSERT INTO table_updates(repo_id,table_name,latest_commit_time) VALUES(?,'commit_parents',?) ;''',(repo_id,latest_commit_time))
				self.cursor.execute('''UPDATE repositories SET latest_commit_time=? WHERE id=?;''',(latest_commit_time,repo_id))


		if autocommit:
			self.connection.commit()


	def fill_followers(self,followers_info_list,autocommit=True):
		'''
		Filling in followers.
		No table_updates entry, because the info is self-contained already in the followers table
		'''
		if self.db_type == 'postgres':
			extras.execute_batch(self.cursor,'''
				INSERT INTO followers(github_login,followers)
				VALUES(%s,%s)
				;''',followers_info_list)
		else:
			self.cursor.executemany('''
				INSERT INTO followers(github_login,followers)
				VALUES(?,?)
				;''',followers_info_list)
		if autocommit:
			self.connection.commit()

	# def create_indexes(self,table=None):
	# 	'''
	# 	Creating indexes for the various tables that are not specified at table creation, where insertion time could be impacted by their presence
	# 	'''
	# 	if table == 'user' or table is None:
	# 		self.logger.info('Creating indexes for table user')
	# 		self.cursor.execute('''
	# 			CREATE INDEX IF NOT EXISTS user_names_idx ON users(name)
	# 			;''')
	# 	elif table == 'commits' or table is None:
	# 		self.logger.info('Creating indexes for table commits')
	# 		self.cursor.execute('''
	# 			CREATE INDEX IF NOT EXISTS commits_ac_idx ON commits(author_id,created_at)
	# 			;''')
	# 		self.cursor.execute('''
	# 			CREATE INDEX IF NOT EXISTS commits_rc_idx ON commits(repo_id,created_at)
	# 			;''')
	# 		self.cursor.execute('''
	# 			CREATE INDEX IF NOT EXISTS commits_cra_idx ON commits(created_at,repo_id,author_id)
	# 			;''')
	# 	elif table == 'commit_parents' or table is None:
	# 		self.logger.info('Creating indexes for table commit_parents')
	# 		pass
	# 	self.connection.commit()

	def get_last_dl(self,repo_id,success=None):
		'''
		gets last download time as datetime object
		success None: no selection on success
		succes bool: selection on success
		'''
		if self.db_type == 'postgres':
			self.cursor.execute('''
				SELECT MAX(updated_at)
					FROM table_updates
					WHERE repo_id=%s AND table_name='clones' AND (%s IS NULL OR success=%s)
				;''',(repo_id,success,success))
		else:

			self.cursor.execute('''
				SELECT MAX(updated_at)
					FROM table_updates
					WHERE repo_id=? AND table_name='clones' AND (? IS NULL OR success=?)
				;''',(repo_id,success,success))
		ans = self.cursor.fetchone()
		if ans is not None:
			return ans[0]

	def get_source_info(self,source=None,repo_id=None):
		'''
		Returns source_urlroot if source exists, otherwise throws and error
		'''
		if isinstance(source,str):
			if self.db_type == 'postgres':
				self.cursor.execute('SELECT id,url_root FROM sources WHERE name=%s;',(source,))
			else:
				self.cursor.execute('SELECT id,url_root FROM sources WHERE name=?;',(source,))
			ans = self.cursor.fetchone()
		elif isinstance(source,int):
			if self.db_type == 'postgres':
				self.cursor.execute('SELECT id,url_root FROM sources WHERE id=%s;',(source,))
			else:
				self.cursor.execute('SELECT id,url_root FROM sources WHERE id=?;',(source,))
			ans = self.cursor.fetchone()
		elif source is None and repo_id is not None:
			if self.db_type == 'postgres':
				self.cursor.execute('''SELECT s.id,s.url_root FROM sources s
								INNER JOIN repositories r
								ON r.id=%s AND r.source=s.id;''',(repo_id,))
			else:
				self.cursor.execute('''SELECT s.id,s.url_root FROM sources s
								INNER JOIN repositories r
								ON r.id=? AND r.source=s.id;''',(repo_id,))
			ans = self.cursor.fetchone()
		else:
			ans = None
		if ans is None:
			raise ValueError('Unregistered or unparsed source {} or repo_id {}'.format(source,repo_id))
		else:
			return ans

	def get_last_star(self,source=None,repo=None,owner=None,repo_id=None):
		'''
		returns a dict with created_at, starred_at and login for the last registered star. All to None if does not exist
		'''

		if repo_id is None:
			if owner is None or source is None or repo is None:
				raise SyntaxError('Invalid information for repo, source:{}, owner:{}, name:{}'.format(source,owner,repo))
			else:
				repo_id = self.get_repo_id(name=repo,owner=owner,source=source)
		if self.db_type == 'postgres':
			self.cursor.execute('''SELECT created_at,starred_at,login FROM stars WHERE repo_id=%s
									ORDER BY created_at DESC LIMIT 1;''',(repo_id,))
		else:
			self.cursor.execute('''SELECT created_at,starred_at,login FROM stars WHERE repo_id=?
									ORDER BY created_at DESC LIMIT 1;''',(repo_id,))
		ans = self.cursor.fetchone()
		if ans is None:
			return {'created_at':None,'starred_at':None,'login':None}
		else:
			return {'created_at':ans[0],'starred_at':ans[1],'login':ans[2]}

	def count_stars(self,source=None,repo=None,owner=None,repo_id=None):
		'''
		Counts registered starring events of a repo
		'''
		if repo_id is None:
			if owner is None or source is None or repo is None:
				raise SyntaxError('Invalid information for repo, source:{}, owner:{}, name:{}'.format(source,owner,repo))
			else:
				repo_id = self.get_repo_id(name=repo,owner=owner,source=source)
		if self.db_type == 'postgres':
			self.cursor.execute('''SELECT COUNT(*) FROM stars WHERE repo_id=%s;''',(repo_id,))
		else:
			self.cursor.execute('''SELECT COUNT(*) FROM stars WHERE repo_id=?;''',(repo_id,))
		ans = self.cursor.fetchone()[0] # When no count, result is (None,)
		if ans is None:
			return 0
		else:
			return ans

	def count_forks(self,source=None,repo=None,owner=None,repo_id=None):
		'''
		Counts registered forks of a repo
		'''
		if repo_id is None:
			if owner is None or source is None or repo is None:
				raise SyntaxError('Invalid information for repo, source:{}, owner:{}, name:{}'.format(source,owner,repo))
			else:
				repo_id = self.get_repo_id(name=repo,owner=owner,source=source)
		if self.db_type == 'postgres':
			self.cursor.execute('''SELECT COUNT(*) FROM forks WHERE forked_repo_id=%s;''',(repo_id,))
		else:
			self.cursor.execute('''SELECT COUNT(*) FROM forks WHERE forked_repo_id=?;''',(repo_id,))
		ans = self.cursor.fetchone()[0] # When no count, result is (None,)
		if ans is None:
			return 0
		else:
			return ans

	def count_followers(self,login_id):
		if self.db_type == 'postgres':
			self.cursor.execute('''
				SELECT COUNT(*) FROM followers
				WHERE followee_id=%s
				;

				''',(login_id,))
		else:
			self.cursor.execute('''
				SELECT COUNT(*) FROM followers
				WHERE followee_id=?
				;
				''',(login_id,))
		ans = self.cursor.fetchall()
		if ans is None:
			return 0
		else:
			return ans[0][0]

	def count_users(self):
		self.cursor.execute('''
				SELECT COUNT(*) FROM users
				;''')
		ans = self.cursor.fetchall()
		if ans is None:
			return 0
		else:
			return ans[0][0]

	def count_identities(self,user_id=None):
		if user_id is None:
			self.cursor.execute('''
				SELECT COUNT(*) FROM identities
				;''')
		else:
			if self.db_type == 'postgres':
				self.cursor.execute('''
					SELECT COUNT(*) FROM identities
					WHERE user_id=%s
					;''',(user_id,))
			else:
				self.cursor.execute('''
					SELECT COUNT(*) FROM identities
					WHERE user_id=?
					;''',(user_id,))
		ans = self.cursor.fetchall()
		if ans is None:
			return 0
		else:
			return ans[0][0]

	def insert_stars(self,stars_list,commit=True):
		'''
		Inserts starring events.
		commit defines the behavior at the end, commit of the transaction or not. Committing externally allows to do it only when all stars for a repo have been added
		'''
		if self.db_type == 'postgres':
			extras.execute_batch(self.cursor,'''
				INSERT INTO stars(starred_at,login,repo_id)
				VALUES(%s,%s,%s)
				ON CONFLICT DO NOTHING
				;''',((s['starred_at'],s['login'],s['repo_id']) for s in stars_list))
		else:
			self.cursor.executemany('''
				INSERT OR IGNORE INTO stars(starred_at,login,repo_id)
				VALUES(?,?,?)
				;''',((s['starred_at'],s['login'],s['repo_id']) for s in stars_list))

		if commit:
			self.connection.commit()


	def insert_update(self,table,repo_id=None,identity_id=None,success=True,info=None,autocommit=True):
		'''
		Inserting an update in table_updates
		'''
		if isinstance(info,dict):
			info = json.dumps(info)
		if self.db_type == 'postgres':
			self.cursor.execute('''INSERT INTO table_updates(repo_id,identity_id,table_name,success,info)
				VALUES(%s,%s,%s,%s,%s)
				;''', (repo_id,identity_id,table,success,info))
		else:
			self.cursor.execute('''INSERT INTO table_updates(repo_id,identity_id,table_name,success,info)
				VALUES(?,?,?,?,?)
				;''', (repo_id,identity_id,table,success,info))
		if autocommit:
			self.connection.commit()

	def clean_null_updates(self,table,repo_id=None,identity_id=None,autocommit=True):
		'''
		Removing rows in table_updates with success=NULL, potentially corresponding to identity and / or repo
		'''
		if self.db_type == 'postgres':
			self.cursor.execute('''DELETE FROM table_updates
				WHERE success IS NULL
				AND repo_id=%s AND identity_id=%s AND table_name=%s
				;''', (repo_id,identity_id,table))
		else:
			self.cursor.execute('''DELETE FROM table_updates
				WHERE success IS NULL
				AND repo_id=? AND identity_id=? AND table_name=?
				;''', (repo_id,identity_id,table))
		if autocommit:
			self.connection.commit()

	def set_cloned(self,repo_id,autocommit=True):
		'''
		Setting cloned to true for a given repository
		'''
		if self.db_type == 'postgres':
			self.cursor.execute('''UPDATE repositories SET cloned=true WHERE id=%s;''',(repo_id,))
		else:
			self.cursor.execute('''UPDATE repositories SET cloned=1 WHERE id=?;''',(repo_id,))
		if autocommit:
			self.connection.commit()

	# def set_gh_login(self,user_id,login,autocommit=True):
	# 	'''
	# 	Sets a login for a given user (id refers to a unique email, which can refer to several logins)
	# 	'''
	# 	if self.db_type == 'postgres':
	# 		self.cursor.execute('''UPDATE users SET github_login=%s WHERE id=%s;''',(login,user_id))
	# 		self.cursor.execute('''INSERT INTO table_updates(user_id,table_name,success) VALUES(%s,'login',%s);''',(user_id,(login is not None)))
	# 	else:
	# 		self.cursor.execute('''UPDATE users SET github_login=? WHERE id=?;''',(login,user_id))
	# 		self.cursor.execute('''INSERT INTO table_updates(user_id,table_name,success) VALUES(?,'login',?);''',(user_id,(login is not None)))

	# 	if autocommit:
	# 		self.connection.commit()

	def merge_identities(self,identity1,identity2,autocommit=True,record=True,reason=None):
		'''
		Merges the user corresponding to both identities.
		user of identity1 gets precedence
		'''

		# Getting user_id that may disappear
		if self.db_type == 'postgres':
			self.cursor.execute('''
				SELECT user_id FROM identities WHERE id=%s LIMIT 1
				;''',(identity2,))
		else:
			self.cursor.execute('''
				SELECT user_id FROM identities WHERE id=? LIMIT 1
				;''',(identity2,))
		old_user_id2 = self.cursor.fetchall()[0][0]

		#Getting common user_id that will be used at the end
		if self.db_type == 'postgres':
			self.cursor.execute('''
				SELECT user_id FROM identities WHERE id=%s LIMIT 1
				;''',(identity1,))
		else:
			self.cursor.execute('''
				SELECT user_id FROM identities WHERE id=? LIMIT 1
				;''',(identity1,))
		user_id = self.cursor.fetchall()[0][0]

		#If same do nothing
		if user_id != old_user_id2:
			#Update user_id for identity2 and all that have old_user_id2
			if self.db_type == 'postgres':
				self.cursor.execute('''UPDATE identities SET user_id=%s WHERE user_id=%s;''',(user_id,old_user_id2))
			else:
				self.cursor.execute('''UPDATE identities SET user_id=? WHERE user_id=?;''',(user_id,old_user_id2))

			if record and self.cursor.rowcount:
				if self.db_type == 'postgres':
					self.cursor.execute('''INSERT INTO merged_identities(main_identity_id,secondary_identity_id,main_user_id,secondary_user_id,affected_identities,reason)
							VALUES(%s,%s,%s,%s,%s,%s);''',(identity1,identity2,user_id,old_user_id2,self.cursor.rowcount,reason))
				else:
					self.cursor.execute('''INSERT INTO merged_identities(main_identity_id,secondary_identity_id,main_user_id,secondary_user_id,affected_identities,reason)
							VALUES(?,?,?,?,?,?);''',(identity1,identity2,user_id,old_user_id2,self.cursor.rowcount,reason))

			#Delete old_user2
			if self.db_type == 'postgres':
				self.cursor.execute(''' DELETE FROM users WHERE id=%s;''',(old_user_id2,))
			else:
				self.cursor.execute(''' DELETE FROM users WHERE id=?;''',(old_user_id2,))


		if autocommit:
			self.connection.commit()

	def reset_merged_identities(self):
		'''
		Recreates a situatuion where all identities are referring to their own individual user
		'''
		# Recreating a user per identity
		if self.db_type == 'postgres':
			self.cursor.execute('''
				INSERT INTO users(creation_identity_type_id,creation_identity)
					SELECT i.identity_type_id,i.identity FROM identities i
				ON CONFLICT DO NOTHING;''')
		else:
			self.cursor.execute('''
				INSERT OR IGNORE INTO users(creation_identity_type_id,creation_identity)
					SELECT i.identity_type_id,i.identity FROM identities i
				;''')
		# Associating each identity to its user
		# if self.db_type == 'postgres':
		# 	self.cursor.execute('''
		# 		UPDATE identities SET user_id=u.id
		# 			FROM identities i
		# 			INNER JOIN users u
		# 			ON i.identity=u.creation_identity
		# 			AND i.identity_type_id=u.creation_identity_type_id
		# 		;''')
		# else:
		self.cursor.execute('''
				UPDATE identities SET user_id=(SELECT u.id FROM users u
					WHERE identities.identity=u.creation_identity
					AND identities.identity_type_id=u.creation_identity_type_id)
				;''')

		self.connection.commit()

	def plan_repo_merge(self,
		merged_repo_table_id=None,
		obsolete_id=None,
		obsolete_source=None,
		obsolete_owner=None,
		obsolete_name=None,
		new_id=None,
		new_source=None,
		new_owner=None,
		new_name=None,
		merging_reason_source='repository merge process',
		autocommit=True,
		retry=4
		):
		'''
		Setting the repo merging process in the table merged_repositories for later execution, returning the id of the row
		'''

		# checks
		if (new_id is None and (new_owner is None or new_name is None)) or (obsolete_id is None and (obsolete_owner is None or obsolete_name is None)):
			raise SyntaxError('Insufficent info provided for merging repositories (id,source,owner,name): \n new ({},{},{},{}) \n obsolete ({},{},{},{})'.format(new_id,new_source,new_owner,new_name,obsolete_id,obsolete_source,obsolete_owner,obsolete_name))

		self.register_source(source=merging_reason_source)
		if autocommit:
			self.connection.commit()
		if self.db_type == 'postgres':
			self.cursor.execute('''
				INSERT INTO merged_repositories(
				new_id,
				new_source,
				new_owner,
				new_name,
				obsolete_id,
				obsolete_source,
				obsolete_owner,
				obsolete_name,
				merging_reason_source)
					VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
					RETURNING id
				;''',(new_id,
				new_source,
				new_owner,
				new_name,
				obsolete_id,
				obsolete_source,
				obsolete_owner,
				obsolete_name,
				merging_reason_source,))
			try:
				row_id = self.cursor.fetchone()[0]
			except (TypeError,psycopg2.ProgrammingError) as e:
				message = '{},{},{},{},{},{},{},{},{}: {} ({} retries left)'.format(new_id,
								new_source,
								new_owner,
								new_name,
								obsolete_id,
								obsolete_source,
								obsolete_owner,
								obsolete_name,
								merging_reason_source,e,retry-1)
				if retry > 0:
					self.log_error(message=message)
					time.sleep(0.1)
					return self.plan_repo_merge(merged_repo_table_id=merged_repo_table_id,
							obsolete_id=obsolete_id,
							obsolete_source=obsolete_source,
							obsolete_owner=obsolete_owner,
							obsolete_name=obsolete_name,
							new_id=new_id,
							new_source=new_source,
							new_owner=new_owner,
							new_name=new_name,
							merging_reason_source=merging_reason_source,
							autocommit=autocommit,
							retry=retry-1
							)
				else:
					raise TypeError(message) from e

		else:
			self.cursor.execute('''
				INSERT INTO merged_repositories(
				new_id,
				new_source,
				new_owner,
				new_name,
				obsolete_id,
				obsolete_source,
				obsolete_owner,
				obsolete_name,
				merging_reason_source)
					VALUES(?,?,?,?,?,?,?,?,?)
				;''',(new_id,
				new_source,
				new_owner,
				new_name,
				obsolete_id,
				obsolete_source,
				obsolete_owner,
				obsolete_name,
				merging_reason_source,))
			row_id = self.cursor.lastrowid

		assert isinstance(row_id,int), 'expected int for row_id, got {}'.format(row_id)

		if autocommit:
			self.connection.commit()
		return row_id

	def validate_merge_repos(self,merged_repo_table_id):
		'''
		Sets the relevant merging process to done
		'''
		if self.db_type == 'postgres':
			self.cursor.execute('''
				UPDATE merged_repositories SET merged_at=CURRENT_TIMESTAMP
					WHERE id=%s
					;''',(merged_repo_table_id,))
		else:
			self.cursor.execute('''
				UPDATE merged_repositories SET merged_at=CURRENT_TIMESTAMP
					WHERE id=?
					;''',(merged_repo_table_id,))
		self.connection.commit()

	def batch_merge_repos(self):
		'''
		Checks for repo merging processes planned but not done yet (=merged_at is NULL in merged_repositories table)
		and executes the merges
		'''
		self.cursor.execute('''
			SELECT id,
				new_id,
				new_source,
				new_owner,
				new_name,
				obsolete_id,
				obsolete_source,
				obsolete_owner,
				obsolete_name,
				merging_reason_source FROM merged_repositories
			WHERE merged_at is NULL
			;''')

		merge_list = [ {
							'merged_repo_table_id': merged_repo_table_id,
							'new_id':new_id,
							'new_source':new_source,
							'new_owner':new_owner,
							'new_name':new_name,
							'obsolete_id':obsolete_id,
							'obsolete_source':obsolete_source,
							'obsolete_owner':obsolete_owner,
							'obsolete_name':obsolete_name,
							'merging_reason_source':merging_reason_source,
						} for  (merged_repo_table_id,
				new_id,
				new_source,
				new_owner,
				new_name,
				obsolete_id,
				obsolete_source,
				obsolete_owner,
				obsolete_name,
				merging_reason_source) in self.cursor.fetchall() ]

		self.logger.info('Batch merging {} repos ({} unique)'.format(len(merge_list),len(set([tuple(d.items()) for d in merge_list]))))

		for merge_dict in merge_list:
			self.merge_repos(**merge_dict)



	def merge_repos(self,
		merged_repo_table_id=None,
		obsolete_id=None,
		obsolete_source=None,
		obsolete_owner=None,
		obsolete_name=None,
		new_id=None,
		new_source=None,
		new_owner=None,
		new_name=None,
		merging_reason_source='repository merge process',
		fail_on_no_repo_id=False
		):
		'''
		Merge process of two repositories, e.g. when a URL redirect is detected. All information attributed to the new repo.
		Table_updates of obsolete are deleted (through cascade), just in case one forgets to update one table in the repo_id update process.
		'''

		if merging_reason_source is None:
			merging_reason_source = 'repository merge process'

		if merged_repo_table_id is None:
			merged_repo_table_id = self.plan_repo_merge(
							new_id=new_id,
							new_source=new_source,
							new_owner=new_owner,
							new_name=new_name,
							obsolete_id=obsolete_id,
							obsolete_source=obsolete_source,
							obsolete_owner=obsolete_owner,
							obsolete_name=obsolete_name,
							merging_reason_source=merging_reason_source
							)

		# checks
		if (new_id is None and (new_owner is None or new_name is None)) or (obsolete_id is None and (obsolete_owner is None or obsolete_name is None)):
			raise SyntaxError('Insufficent info provided for merging repositories (id,source,owner,name): \n new ({},{},{},{}) \n obsolete ({},{},{},{})'.format(new_id,new_source,new_owner,new_name,obsolete_id,obsolete_source,obsolete_owner,obsolete_name))

		if new_id is None:
			new_id = self.get_repo_id(source=new_source,owner=new_owner,name=new_name)
		if obsolete_id is not None:
			obsolete_id = self.check_repo_id(repo_id=obsolete_id)
		if obsolete_id is None:
			obsolete_id = self.get_repo_id(source=obsolete_source,owner=obsolete_owner,name=obsolete_name)
			if obsolete_id is None:
				if fail_on_no_repo_id:
					raise ValueError('Repository to be merged {}/{}/{} not found in DB. Destination repo: {}/{}/{} ({})'.format(obsolete_source,obsolete_owner,obsolete_name,new_source,new_owner,new_name,new_id))
				else:
					self.logger.info('Repository to be merged {}/{}/{} not found in DB. Destination repo: {}/{}/{} ({})'.format(obsolete_source,obsolete_owner,obsolete_name,new_source,new_owner,new_name,new_id))
					self.validate_merge_repos(merged_repo_table_id=merged_repo_table_id)
					return

		if obsolete_owner is None:
			if self.db_type == 'postgres':
				self.cursor.execute('SELECT owner,name FROM repositories WHERE id=%s;',(obsolete_id,))
			else:
				self.cursor.execute('SELECT owner,name FROM repositories WHERE id=?;',(obsolete_id,))
			ans = self.cursor.fetchone()
			if ans[0] is None:
				raise ValueError('Repository id not found: {}'.format(obsolete_id))
			else:
				obsolete_owner,obsolete_name = ans
		if obsolete_source is None:
			obsolete_source = self.get_source_info(repo_id=obsolete_id)[0]

		if new_id == obsolete_id:
			self.logger.info('Repositories to be merged already match(id,source,owner,name): \n new ({},{},{},{}) \n obsolete ({},{},{},{})'.format(new_id,new_source,new_owner,new_name,obsolete_id,obsolete_source,obsolete_owner,obsolete_name))
		else:
			self.logger.info('Repositories to be merged (id,source,owner,name): \n new ({},{},{},{}) \n obsolete ({},{},{},{})'.format(new_id,new_source,new_owner,new_name,obsolete_id,obsolete_source,obsolete_owner,obsolete_name))
			if new_id is None: # in this case we know new_owner and new_name were provided

				self.register_source(source=merging_reason_source)

				# build url
				if new_source is None:
					new_source = obsolete_source
				new_source_id,url_root = self.get_source_info(source=new_source)
				obsolete_source_id,obsolete_url_root = self.get_source_info(source=obsolete_source)
				url = 'https://{}/{}/{}'.format(url_root,new_owner,new_name)

				if self.db_type == 'postgres':
					self.cursor.execute('SELECT name FROM sources WHERE id=%s;',(new_source_id,))
				else:
					self.cursor.execute('SELECT name FROM sources WHERE id=?;',(new_source_id,))
				new_source_name = self.cursor.fetchone()[0]

				if self.db_type == 'postgres':
					self.cursor.execute('SELECT name FROM sources WHERE id=%s;',(obsolete_source_id,))
				else:
					self.cursor.execute('SELECT name FROM sources WHERE id=?;',(obsolete_source_id,))
				obsolete_source_name = self.cursor.fetchone()[0]

				# add url
				self.register_urls(url_list=[(url,url,new_source_id)],source=merging_reason_source)

				# change url,source,owner,name
				if self.db_type == 'postgres':
					self.cursor.execute('''SELECT cu.url
							FROM urls cu
							INNER JOIN urls u
								ON cu.id=u.cleaned_url
							INNER JOIN repositories r
								ON r.url_id=u.id
								AND r.id=%s
						;''',(obsolete_id,))

					ans = self.cursor.fetchone()
					if ans is None:
						old_url = '{},{},{}'.format(obsolete_source,obsolete_owner,obsolete_name)
					else:
						old_url = ans[0]

					self.cursor.execute('''SELECT u.id
							FROM urls u
							WHERE u.url=%s
						;''',(url,))

					url_id = self.cursor.fetchone()[0]

					self.cursor.execute('''SELECT r.url_id
							FROM repositories r
							WHERE r.id=%s
						;''',(obsolete_id,))

					old_url_id = self.cursor.fetchone()[0]
					self.connection.commit()

					self.cursor.execute('''
						UPDATE repositories SET
							url_id=%s,
							source=%s,
							owner=%s,
							name=%s
						WHERE id=%s
						;''',(url_id,new_source_id,new_owner,new_name,obsolete_id))

					self.cursor.execute('''DELETE FROM table_updates WHERE repo_id=%s AND table_name='clones';''',(obsolete_id,))

					if old_url_id is not None:
						self.cursor.execute('''UPDATE urls SET cleaned_url=%s
							WHERE urls.cleaned_url=(SELECT u.cleaned_url FROM urls u WHERE u.id=%s)
						;''',(url_id,old_url_id,))

				else:
					self.cursor.execute('''SELECT cu.url
							FROM urls cu
							INNER JOIN urls u
								ON cu.id=u.cleaned_url
							INNER JOIN repositories r
								ON r.url_id=u.id
								AND r.id=?
						;''',(obsolete_id,))

					ans = self.cursor.fetchone()
					if ans is None:
						old_url = '{},{},{}'.format(obsolete_source,obsolete_owner,obsolete_name)
					else:
						old_url = ans[0]

					self.cursor.execute('''SELECT u.id
							FROM urls u
							WHERE u.url=?
						;''',(url,))

					url_id = self.cursor.fetchone()[0]

					self.cursor.execute('''SELECT r.url_id
							FROM repositories r
							WHERE r.id=?
						;''',(obsolete_id,))

					old_url_id = self.cursor.fetchone()[0]
					self.connection.commit()

					self.cursor.execute('''
						UPDATE repositories SET
							url_id=?,
							source=?,
							owner=?,
							name=?
						WHERE id=?
						;''',(url_id,new_source_id,new_owner,new_name,obsolete_id))

					self.cursor.execute('''DELETE FROM table_updates WHERE repo_id=? AND table_name='clones';''',(obsolete_id,))

					if old_url_id is not None:
						self.cursor.execute('''UPDATE urls SET cleaned_url=?
							WHERE urls.cleaned_url=(SELECT u.cleaned_url FROM urls u WHERE u.id=?)
						;''',(url_id,old_url_id,))

				# Moving cloned repo if exists. But clones should be filled after forks or stars, to avoid failed cloning.
				if os.path.exists(os.path.join(self.data_folder,'cloned_repos',obsolete_source_name,obsolete_owner,obsolete_name)):
					if not os.path.exists(os.path.join(self.data_folder,'cloned_repos',new_source_name,new_owner,new_name)):
						if not os.path.exists(os.path.join(self.data_folder,'cloned_repos',new_source_name,new_owner)):
							os.makedirs(os.path.join(self.data_folder,'cloned_repos',new_source_name,new_owner))
						shutil.move(os.path.join(self.data_folder,'cloned_repos',obsolete_source_name,obsolete_owner,obsolete_name),os.path.join(self.data_folder,'cloned_repos',new_source_name,new_owner,new_name))
						# if not os.listdir(os.path.join(self.data_folder,'cloned_repos',new_source_name,new_owner)):
						# 	shutil.rmtree(os.path.join(self.data_folder,'cloned_repos',new_source_name,new_owner))
						os.symlink(os.path.abspath(os.path.join(self.data_folder,'cloned_repos',new_source_name,new_owner,new_name)),os.path.abspath(os.path.join(self.data_folder,'cloned_repos',obsolete_source_name,obsolete_owner,obsolete_name)))
			else:
				if self.db_type == 'postgres':

					self.cursor.execute('''SELECT cu.url
							FROM urls cu
							INNER JOIN urls u
								ON cu.id=u.cleaned_url
							INNER JOIN repositories r
								ON r.url_id=u.id
								AND r.id=%s
						;''',(obsolete_id,))


					self.cursor.execute('''SELECT r.url_id
							FROM repositories r
							WHERE r.id=%s
						;''',(new_id,))

					url_id = self.cursor.fetchone()[0]


					self.cursor.execute('''SELECT r.url_id
							FROM repositories r
							WHERE r.id=%s
						;''',(obsolete_id,))

					old_url_id = self.cursor.fetchone()[0]

					self.connection.commit()

					ans = self.cursor.fetchone()
					if ans is None:
						old_url = '{},{},{}'.format(obsolete_source,obsolete_owner,obsolete_name)
					else:
						old_url = ans[0]

					self.cursor.execute('''SELECT cu.url
							FROM urls cu
							INNER JOIN urls u
								ON cu.id=u.cleaned_url
							INNER JOIN repositories r
								ON r.url_id=u.id
								AND r.id=%s
						;''',(new_id,))


					ans = self.cursor.fetchone()
					if ans is None:
						url = '{},{},{}'.format(new_source,new_owner,new_name)
					else:
						url = ans[0]

					self.cursor.execute('''
						UPDATE packages SET repo_id=%s
						WHERE repo_id=%s
						;''',(new_id,obsolete_id))
					self.cursor.execute('''
						UPDATE commits SET repo_id=%s
						WHERE repo_id=%s
						;''',(new_id,obsolete_id))
					self.cursor.execute('''
						UPDATE stars SET repo_id=%s
						WHERE repo_id=%s
						AND NOT EXISTS (SELECT repo_id FROM stars s WHERE s.repo_id=%s AND stars.login=s.login AND stars.identity_type_id=s.identity_type_id)
						;''',(new_id,obsolete_id,new_id))
					self.cursor.execute('''
						UPDATE commit_repos SET repo_id=%s
						WHERE repo_id=%s
						AND NOT EXISTS (SELECT repo_id FROM commit_repos cr WHERE cr.repo_id=%s AND commit_repos.commit_id=cr.commit_id)
						;''',(new_id,obsolete_id,new_id))
					self.cursor.execute('''
						UPDATE forks SET forked_repo_id=%s
						WHERE forked_repo_id=%s
						AND NOT EXISTS (SELECT forked_repo_id FROM forks f WHERE f.forked_repo_id=%s AND forks.forking_repo_url=f.forking_repo_url)
						;''',(new_id,obsolete_id,new_id))
					self.cursor.execute('''
						UPDATE forks SET forking_repo_id=%s,forking_repo_url=%s
						WHERE forking_repo_id=%s
						AND NOT EXISTS (SELECT forked_repo_id FROM forks f WHERE f.forking_repo_id=%s AND forks.forked_repo_id=f.forked_repo_id)
						;''',(new_id,url[8:],obsolete_id,new_id))


					self.cursor.execute('DELETE FROM repositories WHERE id=%s;',(obsolete_id,))

					if old_url_id is not None:
						self.cursor.execute('''UPDATE urls SET cleaned_url=%s
							WHERE urls.cleaned_url=(SELECT u.cleaned_url FROM urls u WHERE u.id=%s)
						;''',(url_id,old_url_id,))


				else:
					self.cursor.execute('''SELECT cu.url
							FROM urls cu
							INNER JOIN urls u
								ON cu.id=u.cleaned_url
							INNER JOIN repositories r
								ON r.url_id=u.id
								AND r.id=?
						;''',(obsolete_id,))

					ans = self.cursor.fetchone()
					if ans is None:
						old_url = '{},{},{}'.format(obsolete_source,obsolete_owner,obsolete_name)
					else:
						old_url = ans[0]

					self.cursor.execute('''SELECT cu.url
							FROM urls cu
							INNER JOIN urls u
								ON cu.id=u.cleaned_url
							INNER JOIN repositories r
								ON r.url_id=u.id
								AND r.id=?
						;''',(new_id,))


					ans = self.cursor.fetchone()
					if ans is None:
						url = '{},{},{}'.format(new_source,new_owner,new_name)
					else:
						url = ans[0]


					self.cursor.execute('''SELECT r.url_id
							FROM repositories r
							WHERE r.id=?
						;''',(new_id,))

					url_id = self.cursor.fetchone()[0]


					self.cursor.execute('''SELECT r.url_id
							FROM repositories r
							WHERE r.id=?
						;''',(obsolete_id,))

					old_url_id = self.cursor.fetchone()[0]

					self.connection.commit()

					self.cursor.execute('''
						UPDATE OR IGNORE packages SET repo_id=?
						WHERE repo_id=?
						;''',(new_id,obsolete_id))
					self.cursor.execute('''
						UPDATE OR IGNORE commits SET repo_id=?
						WHERE repo_id=?
						;''',(new_id,obsolete_id))
					self.cursor.execute('''
						UPDATE OR IGNORE stars SET repo_id=?
						WHERE repo_id=?
						AND NOT EXISTS (SELECT repo_id FROM stars s WHERE s.repo_id=? AND stars.login=s.login AND stars.identity_type_id=s.identity_type_id)
						;''',(new_id,obsolete_id,new_id))
					self.cursor.execute('''
						UPDATE OR IGNORE commit_repos SET repo_id=?
						WHERE repo_id=?
						AND NOT EXISTS (SELECT repo_id FROM commit_repos cr WHERE cr.repo_id=? AND commit_repos.commit_id=cr.commit_id)
						;''',(new_id,obsolete_id,new_id))
					self.cursor.execute('''
						UPDATE OR IGNORE forks SET forked_repo_id=?
						WHERE forked_repo_id=?
						AND NOT EXISTS (SELECT forked_repo_id FROM forks f WHERE f.forked_repo_id=? AND forks.forking_repo_id=f.forking_repo_id AND forks.forking_repo_url=f.forking_repo_url)
						;''',(new_id,obsolete_id,new_id))
					self.cursor.execute('''
						UPDATE OR IGNORE forks SET forking_repo_id=?,forking_repo_url=?
						WHERE forking_repo_id=?
						AND NOT EXISTS (SELECT forked_repo_id FROM forks f WHERE f.forking_repo_id=? AND forks.forked_repo_id=f.forked_repo_id)
						;''',(new_id,url[8:],obsolete_id,new_id))


					self.cursor.execute('DELETE FROM repositories WHERE id=?;',(obsolete_id,))

					if old_url_id is not None:
						self.cursor.execute('''UPDATE urls SET cleaned_url=?
							WHERE urls.cleaned_url=(SELECT u.cleaned_url FROM urls u WHERE u.id=?)
						;''',(url_id,old_url_id,))


		self.validate_merge_repos(merged_repo_table_id=merged_repo_table_id)

	def log_error(self,message):
		'''
		Logs errors in the DB, to keep track of errors directly while executing the multiple threads; and not having to wait for all concurrent.futures to yield results.
		Alternative to use native python logging would be to add a logger with a uuid name (to not have the module level logger reused by all instances and report everything everywhere)
		and a file handler in self.data_folder
		WARNING: This commits the error, but also all previous elements of the transaction potentially waiting for commit. Solution would be to clone the connection temporarily.
		'''
		if self.db_type == 'postgres':
			self.cursor.execute('''
				INSERT INTO _error_logs(error)
				VALUES (%s);
				''',(message,))
		else:
			self.cursor.execute('''
				INSERT INTO _error_logs(error)
				VALUES (?);
				''',(message,))
		self.connection.commit()






class ComputationDB(object):
	'''
	ADDITIONAL COMPUTATION DB: to store computed results.
	This is intentionally separated from the main DB, so that computation can be done and stored also when the main DB is not writeable.
	'''
	def __init__(self,db):
		self.orig_db = db
		self.connection = sqlite3.connect(self.orig_db.computation_db_name)
		self.cursor = self.connection.cursor()
		self.orig_db.cursor.execute('''SELECT info_content FROM _dbinfo WHERE info_type='uuid';''')
		self.db_id = self.orig_db.cursor.fetchone()[0]
		CP_DBINIT = '''
				CREATE TABLE IF NOT EXISTS measures(
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				db_id TEXT NOT NULL,
				params TEXT NOT NULL,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				completed_at TIMESTAMP DEFAULT NULL,
				UNIQUE(db_id,name,params)
				);

				CREATE TABLE IF NOT EXISTS data_repositories(
				measure INTEGER REFERENCES measures(id) ON DELETE CASCADE,
				obj_id INTEGER NOT NULL,
				measured_at DATE NOT NULL,
				value REAL,
				PRIMARY KEY(measure,obj_id,measured_at)
				);

				CREATE INDEX IF NOT EXISTS repo_idx2 ON data_repositories(measure,measured_at,obj_id);

				CREATE TABLE IF NOT EXISTS data_users(
				measure INTEGER REFERENCES measures(id) ON DELETE CASCADE,
				obj_id INTEGER NOT NULL,
				measured_at DATE NOT NULL,
				value REAL,
				PRIMARY KEY(measure,obj_id,measured_at)
				);

				CREATE INDEX IF NOT EXISTS user_idx2 ON data_users(measure,measured_at,obj_id);

				'''
		for q in CP_DBINIT.split(';')[:-1]:
			self.cursor.execute(q)
		self.connection.commit()


	def get_measure_id(self,measure=None,params=None,check=False,create_if_absent=True):
		if isinstance(measure,int):
			if check:
				self.cursor.execute('SELECT id FROM measures WHERE id=? AND db_id=?;',(measure,self.db_id))
				ans = self.cursor.fetchone()
				if ans is None:
					raise ValueError('(db_id {}) No such measure id in computation_db: {}'.format(self.db_id,measure))
			return measure
		else:
			params = self.format_params(params)
			self.cursor.execute('SELECT id FROM measures WHERE name=? AND params=? AND db_id=?;',(measure,params,self.db_id))
			ans = self.cursor.fetchone()
			if ans is None:
				if not create_if_absent:
					raise ValueError('(db_id {}) No such measure in computation_db: {}, {}'.format(self.db_id,measure,params))
				else:
					self.cursor.execute('INSERT INTO measures(name,params,db_id) VALUES(?,?,?);',(measure,params,self.db_id))
					row_id = self.cursor.lastrowid
					return row_id
			else:
				return ans[0]

	def format_params(self,params):
		if params is None:
			params = {}
		if isinstance(params,dict):
			return json.dumps(params, sort_keys=True,indent=2)
		elif isinstance(params,str):
			return self.format_params(json.loads(params))
		else:
			raise ValueError('Unparseable params: {}'.format(params))

	def format_times(self,start_time,end_time):
		if start_time is None:
			start_time = datetime.datetime(2013,1,1)
			# start_time = self.get_start_time()
		if end_time is None:
			end_time = datetime.datetime.now()
			# end_time = self.get_end_time()

		if isinstance(start_time,str):
			try:
				start_time = datetime.datetime.strptime(start_time,'%Y-%m-%d')
			except ValueError:
				start_time = datetime.datetime.strptime(start_time,'%Y-%m-%d %H:%M:%S')

		if isinstance(end_time,str):
			try:
				end_time = datetime.datetime.strptime(end_time,'%Y-%m-%d')
			except ValueError:
				end_time = datetime.datetime.strptime(end_time,'%Y-%m-%d %H:%M:%S')

		return start_time,end_time

	def read(self,measure,table_name,params=None,start_time=None,end_time=None,obj_id=None):
		'''
		Syntax for output:
		[(<user/project>_id,datetime.datetime,value),...]
		'''
		measure_id = self.get_measure_id(measure=measure,params=params)
		start_time,end_time = self.format_times(start_time=start_time,end_time=end_time)
		if table_name not in ['repositories','users']:
			raise ValueError('table_name not recognized: {}'.format(table_name))
		else:
			if obj_id is None:
				self.cursor.execute('''
					SELECT obj_id,measured_at,value FROM data_{}
					WHERE measure=?
					AND date(datetime(?))<=measured_at AND measured_at<=date(datetime(?))
					ORDER BY obj_id,measured_at;
					'''.format(table_name)
					,(measure_id,start_time,end_time))
			else:
				self.cursor.execute('''
					SELECT obj_id,measured_at,value FROM data_{}
					WHERE measure=?
					AND date(datetime(?))<=measured_at AND measured_at<=date(datetime(?))
					AND obj_id=?
					ORDER BY obj_id,measured_at;
					'''.format(table_name)
					,(measure_id,start_time,end_time,obj_id))

			return self.cursor.fetchall()


	def batch_write(self,table_name,measure,params=None,data=None,autocommit=True):
		'''
		Syntax for data:
		[(<user/project>_id,datetime.datetime,value),...]
		'''
		measure_id = self.get_measure_id(measure=measure,params=params)
		if table_name not in ['repositories','users']:
			raise ValueError('table_name not recognized: {}'.format(table_name))
		else:
			self.cursor.executemany('''
				INSERT OR IGNORE INTO data_{} (measure,obj_id,measured_at,value)
				VALUES(?,?,?,?);
				'''.format(table_name)
				,( (measure_id,oid,measured_at,val) for (oid,measured_at,val) in data))
			if self.cursor.rowcount > 0:
				self.cursor.execute('''
					UPDATE measures SET completed_at=CURRENT_TIMESTAMP
					WHERE id=?
					;''',(measure_id,))

			if autocommit:
				self.connection.commit()

	def is_completed(self,measure=None,params=None):
		if isinstance(measure,int):
			self.cursor.execute('SELECT completed_at FROM measures WHERE id=? AND completed_at IS NOT NULL;',(measure,))
		else:
			params = self.format_params(params)
			self.cursor.execute('SELECT completed_at FROM measures WHERE name=? AND params=? AND db_id=? AND completed_at IS NOT NULL;',(measure,params,self.db_id))
		ans = self.cursor.fetchone()
		if ans is None:
			return False
		else:
			return True

	def clean_users(self,safe=True):
		if safe:
			self.cursor.execute('''
				DELETE FROM users
				WHERE
					EXISTS (SELECT 1 FROM identities i
						WHERE i.identity  = user.creation_identity AND user.creation_identity_type_id=i.identity_type_id )
					AND NOT EXISTS (SELECT 1 FROM identities
						WHERE identities.user_id  = user.id )
				;''')
		else:
			self.cursor.execute('''
				DELETE FROM users
				WHERE NOT EXISTS (SELECT 1 FROM identities
						WHERE identities.user_id  = user.id )
				;''')

		self.connection.commit()
