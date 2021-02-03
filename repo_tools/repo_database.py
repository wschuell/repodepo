import os
import datetime
import logging
import sqlite3

import csv
import copy
import json
import numpy as np

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


sqlite3.register_adapter(np.int64, int)

class Database(object):
	'''

	This class creates a database object with the main structure, with a few methods  to manipulate it.
	By default SQLite is used, but PostgreSQL is also an option

	To fill it, fillers are used (see Filler class).
	The object uses a specific data folder and a list of files used for the fillers, with name, keyword, and potential download link. (move to filler class?)

	'''

	def __init__(self,db_type='sqlite',db_name='repo_tools',db_folder='.',db_user='postgres',port='5432',host='localhost',data_folder='./datafolder',password=None,clean_first=False,do_init=False,timeout=5):
		self.db_type = db_type
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
			if password is not None:
				logger.warning('You are providing your password directly, this could be a security concern, consider using solutions like .pgpass file.')
			try:
				self.connection = psycopg2.connect(user=db_user,port=port,host=host,database=db_name,password=password)
			except psycopg2.OperationalError:
				pgpass_env = 'PGPASSFILE'
				default_pgpass = os.path.join(os.environ['HOME'],'.pgpass')
				if pgpass_env not in os.environ.keys():
					os.environ[pgpass_env] = default_pgpass
					self.logger.info('Password authentication failed,trying to set .pgpass env variable')
					self.connection = psycopg2.connect(user=db_user,port=port,host=host,database=db_name,password=password)
				else:
					raise
			self.cursor = self.connection.cursor()
		else:
			raise ValueError('Unknown DB type: {}'.format(db_type))

		if clean_first:
			self.clean_db()
		if do_init:
			self.init_db()
		self.logger = logger
		self.fillers = []
		self.data_folder = data_folder
		if not os.path.exists(self.data_folder):
			os.makedirs(self.data_folder)

		#storing info to be able to copy the db and have independent cursor/connection
		self.db_conninfo = {
				'db_type':db_type,
				'db_name':db_name,
				'db_folder':db_folder,
				'db_user':db_user,
				'port':port,
				'host':host,
				'password':password,
		}

	def copy(self,timeout=30):
		'''
		Returns a copy, without init, with independent connection and cursor
		'''
		return self.__class__(do_init=False,timeout=timeout,**self.db_conninfo)

	def init_db(self):
		'''
		Initializing the database, with correct tables, constraints and indexes.
		'''
		logger.info('Creating database ({}) table and indexes'.format(self.db_type))
		if self.db_type == 'sqlite':
			self.DB_INIT = '''
				CREATE TABLE IF NOT EXISTS sources(
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL UNIQUE,
				url_root TEXT
				);

				CREATE TABLE IF NOT EXISTS urls(
				id INTEGER PRIMARY KEY,
				source INTEGER REFERENCES sources(id) ON DELETE CASCADE,
				source_root INTEGER REFERENCES sources(id) ON DELETE CASCADE,
				url TEXT NOT NULL UNIQUE,
				cleaned_url INTEGER REFERENCES urls(id) ON DELETE CASCADE,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);

				CREATE TABLE IF NOT EXISTS repositories(
				id INTEGER PRIMARY KEY,
				source INTEGER REFERENCES sources(id) ON DELETE CASCADE,
				owner TEXT,
				name TEXT,
				url_id INTEGER REFERENCES urls(id) ON DELETE CASCADE,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP DEFAULT NULL,
				latest_commit_time TIMESTAMP DEFAULT NULL,
				cloned BOOLEAN DEFAULT 0,
				UNIQUE(source,owner,name)
				);

				CREATE TABLE IF NOT EXISTS identity_types(
				id INTEGER PRIMARY KEY,
				name TEXT UNIQUE
				);

				CREATE TABLE IF NOT EXISTS users(
				id INTEGER PRIMARY KEY,
				creation_identity_type_id INTEGER REFERENCES identity_types(id) ON DELETE CASCADE,
				creation_identity TEXT,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				UNIQUE(creation_identity_type_id,creation_identity)
				);


				CREATE TABLE IF NOT EXISTS identities(
				id INTEGER PRIMARY KEY,
				identity_type_id INTEGER REFERENCES identity_types(id) ON DELETE CASCADE,
				user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
				identity TEXT
				created_at TIMESTAMP,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				attributes TEXT,
				UNIQUE(identity_type_id,identity)
				);

				CREATE INDEX IF NOT EXISTS identities_idx ON identities(identity);
				CREATE INDEX IF NOT EXISTS identity_users_idx ON identities(user_id);

				CREATE TABLE IF NOT EXISTS merged_identities(
				id INTEGER PRIMARY KEY,
				main_identity_id INTEGER NOT NULL REFERENCES identities(id) ON DELETE CASCADE,
				secondary_identity_id INTEGER NOT NULL REFERENCES identities(id) ON DELETE CASCADE,
				main_user_id INTEGER NOT NULL,
				secondary_user_id INTEGER NOT NULL,
				affected_identities INTEGER,
				reason TEXT,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);

				CREATE INDEX IF NOT EXISTS merged_id_idx1 ON merged_identities(main_identity_id);
				CREATE INDEX IF NOT EXISTS merged_id_idx2 ON merged_identities(secondary_identity_id);
				CREATE INDEX IF NOT EXISTS merged_id_idx3 ON merged_identities(main_user_id);
				CREATE INDEX IF NOT EXISTS merged_id_idx4 ON merged_identities(secondary_user_id);

				CREATE TABLE IF NOT EXISTS commits(
				id INTEGER PRIMARY KEY,
				sha TEXT,
				author_id INTEGER REFERENCES identities(id) ON DELETE CASCADE,
				repo_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
				created_at TIMESTAMP,
				insertions INTEGER,
				deletions INTEGER,
				UNIQUE(sha)
				);

				CREATE INDEX IF NOT EXISTS commits_ac_idx ON commits(author_id,created_at);
				CREATE INDEX IF NOT EXISTS commits_rc_idx ON commits(repo_id,created_at);
				CREATE INDEX IF NOT EXISTS commits_cra_idx ON commits(created_at,repo_id,author_id);

				CREATE TABLE IF NOT EXISTS commit_repos(
				commit_id INTEGER REFERENCES commits(id) ON DELETE CASCADE,
				repo_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
				is_orig_repo BOOLEAN,
				PRIMARY KEY(commit_id,repo_id)
				);

				CREATE INDEX IF NOT EXISTS commit_repo_idx_rc ON commit_repos(repo_id,commit_id);

				CREATE TABLE IF NOT EXISTS commit_parents(
				child_id INTEGER REFERENCES commits(id) ON DELETE CASCADE,
				parent_id INTEGER REFERENCES commits(id) ON DELETE CASCADE,
				rank INTEGER,
				PRIMARY KEY(child_id,parent_id),
				UNIQUE(parent_id,child_id,rank)
				);

				CREATE TABLE IF NOT EXISTS forks(
				forking_repo_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
				forking_repo_url TEXT,
				forked_repo_id INTEGER NOT NULL REFERENCES repositories(id) ON DELETE CASCADE,
				forked_at TIMESTAMP DEFAULT NULL,
				fork_rank INTEGER DEFAULT 1,
				PRIMARY KEY(forking_repo_url,forked_repo_id)
				);
				CREATE INDEX IF NOT EXISTS forks_reverse_idx ON forks(forked_repo_id,forking_repo_id);
				CREATE INDEX IF NOT EXISTS forks_idx ON forks(forking_repo_id,forked_repo_id);

				CREATE TABLE IF NOT EXISTS table_updates(
				id INTEGER PRIMARY KEY,
				repo_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
				identity_id INTEGER REFERENCES identities(id) ON DELETE CASCADE,
				table_name TEXT,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				success BOOLEAN DEFAULT 1,
				latest_commit_time TIMESTAMP DEFAULT NULL
				);

				CREATE INDEX IF NOT EXISTS table_updates_idx ON table_updates(repo_id,table_name,updated_at);
				CREATE INDEX IF NOT EXISTS table_updates_identity_idx ON table_updates(identity_id,table_name,updated_at);

				CREATE TABLE IF NOT EXISTS full_updates(
				update_type TEXT,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);

				CREATE TABLE IF NOT EXISTS followers(
				follower_identity_type_id INTEGER REFERENCES identity_types(id) ON DELETE CASCADE,
				follower_login TEXT,
				follower_id INTEGER REFERENCES identities(id) ON DELETE CASCADE,
				followee_id INTEGER REFERENCES identities(id) ON DELETE CASCADE,
				created_at TIMESTAMP DEFAULT NULL,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY(followee_id,follower_identity_type_id,follower_login)
				);

				CREATE INDEX IF NOT EXISTS followers_idx ON followers(follower_id,followee_id);

				CREATE TABLE IF NOT EXISTS stars(
				repo_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
				identity_type_id INTEGER REFERENCES identity_types(id) ON DELETE CASCADE,
				login TEXT NOT NULL,
				identity_id INTEGER REFERENCES identities(id) ON DELETE CASCADE,
				starred_at TIMESTAMP NOT NULL,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY(repo_id,login,identity_type_id)
				);

				CREATE INDEX IF NOT EXISTS stars_idx ON stars(repo_id,starred_at);
				CREATE INDEX IF NOT EXISTS stars_idx2 ON stars(repo_id,created_at);
				CREATE INDEX IF NOT EXISTS stars_idx3 ON stars(identity_id,starred_at);

				CREATE TABLE IF NOT EXISTS packages(
				id INTEGER PRIMARY KEY,
				source_id INTEGER REFERENCES sources(id) ON DELETE CASCADE,
				insource_id INTEGER DEFAULT NULL,
				name TEXT,
				url_id INTEGER REFERENCES urls(id) ON DELETE CASCADE,
				repo_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				created_at TIMESTAMP DEFAULT NULL,
				UNIQUE(source_id,insource_id)
				);

				CREATE INDEX IF NOT EXISTS packages_idx ON packages(source_id,name);
				CREATE INDEX IF NOT EXISTS packages_date_idx ON packages(created_at);
				CREATE INDEX IF NOT EXISTS packages_repo_idx ON packages(repo_id);

				CREATE TABLE IF NOT EXISTS merged_repositories(
				id BIGSERIAL PRIMARY KEY,
				new_repo TEXT,
				obsolete_repo TEXT,
				merged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);
		'''
			for q in self.DB_INIT.split(';')[:-1]:
				self.cursor.execute(q)
			self.connection.commit()
		elif self.db_type == 'postgres':
			self.DB_INIT = '''
				CREATE TABLE IF NOT EXISTS sources(
				id BIGSERIAL PRIMARY KEY,
				name TEXT NOT NULL UNIQUE,
				url_root TEXT
				);

				CREATE TABLE IF NOT EXISTS urls(
				id BIGSERIAL PRIMARY KEY,
				source BIGINT REFERENCES sources(id) ON DELETE CASCADE,
				source_root BIGINT REFERENCES sources(id) ON DELETE CASCADE,
				url TEXT NOT NULL UNIQUE,
				cleaned_url BIGINT REFERENCES urls(id) ON DELETE CASCADE,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);

				CREATE TABLE IF NOT EXISTS repositories(
				id BIGSERIAL PRIMARY KEY,
				source BIGINT REFERENCES sources(id) ON DELETE CASCADE,
				owner TEXT,
				name TEXT,
				url_id BIGINT REFERENCES urls(id) ON DELETE CASCADE,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				latest_commit_time TIMESTAMP DEFAULT NULL,
				cloned BOOLEAN DEFAULT false,
				UNIQUE(source,owner,name)
				);

				CREATE TABLE IF NOT EXISTS identity_types(
				id BIGSERIAL PRIMARY KEY,
				name TEXT UNIQUE
				);

				CREATE TABLE IF NOT EXISTS users(
				id BIGSERIAL PRIMARY KEY,
				creation_identity_type_id BIGINT REFERENCES identity_types(id) ON DELETE CASCADE,
				creation_identity TEXT,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				UNIQUE(creation_identity_type_id,creation_identity)
				);

				CREATE TABLE IF NOT EXISTS identities(
				id BIGSERIAL PRIMARY KEY,
				identity_type_id BIGINT REFERENCES identity_types(id) ON DELETE CASCADE,
				user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
				identity TEXT,
				created_at TIMESTAMP,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				attributes JSONB,
				UNIQUE(identity_type_id,identity)
				);


				CREATE INDEX IF NOT EXISTS identities_idx ON identities(identity);
				CREATE INDEX IF NOT EXISTS identity_users_idx ON identities(user_id);

				CREATE TABLE IF NOT EXISTS merged_identities(
				id BIGSERIAL PRIMARY KEY,
				main_identity_id BIGINT NOT NULL REFERENCES identities(id) ON DELETE CASCADE,
				secondary_identity_id BIGINT NOT NULL REFERENCES identities(id) ON DELETE CASCADE,
				main_user_id BIGINT NOT NULL,
				secondary_user_id BIGINT NOT NULL,
				affected_identities BIGINT,
				reason TEXT,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);

				CREATE INDEX IF NOT EXISTS merged_id_idx1 ON merged_identities(main_identity_id);
				CREATE INDEX IF NOT EXISTS merged_id_idx2 ON merged_identities(secondary_identity_id);
				CREATE INDEX IF NOT EXISTS merged_id_idx3 ON merged_identities(main_user_id);
				CREATE INDEX IF NOT EXISTS merged_id_idx4 ON merged_identities(secondary_user_id);


				CREATE TABLE IF NOT EXISTS commits(
				id BIGSERIAL PRIMARY KEY,
				sha TEXT,
				author_id BIGINT REFERENCES identities(id) ON DELETE CASCADE,
				repo_id BIGINT REFERENCES repositories(id) ON DELETE CASCADE,
				created_at TIMESTAMP,
				insertions INT,
				deletions INT,
				UNIQUE(sha)
				);

				CREATE INDEX IF NOT EXISTS commits_ac_idx ON commits(author_id,created_at);
				CREATE INDEX IF NOT EXISTS commits_rc_idx ON commits(repo_id,created_at);
				CREATE INDEX IF NOT EXISTS commits_cra_idx ON commits(created_at,repo_id,author_id);

				CREATE TABLE IF NOT EXISTS commit_repos(
				commit_id BIGINT REFERENCES commits(id) ON DELETE CASCADE,
				repo_id BIGINT REFERENCES repositories(id) ON DELETE CASCADE,
				is_orig_repo BOOLEAN,
				PRIMARY KEY(commit_id,repo_id)
				);

				CREATE INDEX IF NOT EXISTS commit_repo_idx_rc ON commit_repos(repo_id,commit_id);


				CREATE TABLE IF NOT EXISTS commit_parents(
				child_id BIGINT REFERENCES commits(id) ON DELETE CASCADE,
				parent_id BIGINT REFERENCES commits(id) ON DELETE CASCADE,
				rank INT,
				PRIMARY KEY(child_id,parent_id),
				UNIQUE(parent_id,child_id,rank)
				);

				CREATE TABLE IF NOT EXISTS forks(
				forking_repo_id BIGINT REFERENCES repositories(id) ON DELETE CASCADE,
				forking_repo_url TEXT,
				forked_repo_id BIGINT NOT NULL REFERENCES repositories(id) ON DELETE CASCADE,
				forked_at TIMESTAMP DEFAULT NULL,
				fork_rank INTEGER DEFAULT 1,
				PRIMARY KEY(forking_repo_url,forked_repo_id)
				);
				CREATE INDEX IF NOT EXISTS forks_reverse_idx ON forks(forked_repo_id,forking_repo_id);
				CREATE INDEX IF NOT EXISTS forks_idx ON forks(forking_repo_id,forked_repo_id);

				CREATE TABLE IF NOT EXISTS table_updates(
				id BIGSERIAL PRIMARY KEY,
				repo_id BIGINT REFERENCES repositories(id) ON DELETE CASCADE,
				identity_id BIGINT REFERENCES identities(id) ON DELETE CASCADE,
				table_name TEXT,
				success BOOLEAN DEFAULT true,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				latest_commit_time TIMESTAMP DEFAULT NULL
				);

				CREATE INDEX IF NOT EXISTS table_updates_idx ON table_updates(repo_id,table_name,updated_at);
				CREATE INDEX IF NOT EXISTS table_updates_identity_idx ON table_updates(identity_id,table_name,updated_at);

				CREATE TABLE IF NOT EXISTS full_updates(
				update_type TEXT,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);


				CREATE TABLE IF NOT EXISTS stars(
				repo_id BIGINT REFERENCES repositories(id) ON DELETE CASCADE,
				identity_type_id BIGINT REFERENCES identity_types(id) ON DELETE CASCADE,
				login TEXT NOT NULL,
				identity_id BIGINT REFERENCES identities(id) ON DELETE CASCADE,
				starred_at TIMESTAMP NOT NULL,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY(repo_id,login,identity_type_id)
				);

				CREATE INDEX IF NOT EXISTS stars_idx ON stars(repo_id,starred_at);
				CREATE INDEX IF NOT EXISTS stars_idx2 ON stars(repo_id,created_at);
				CREATE INDEX IF NOT EXISTS stars_idx3 ON stars(identity_id,starred_at);

				CREATE TABLE IF NOT EXISTS followers(
				follower_identity_type_id BIGINT REFERENCES identity_types(id) ON DELETE CASCADE,
				follower_login TEXT,
				follower_id BIGINT REFERENCES identities(id) ON DELETE CASCADE,
				followee_id BIGINT REFERENCES identities(id) ON DELETE CASCADE,
				created_at TIMESTAMP DEFAULT NULL,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY(followee_id,follower_identity_type_id,follower_login)
				);

				CREATE INDEX IF NOT EXISTS followers_idx ON followers(follower_id,followee_id);


				CREATE TABLE IF NOT EXISTS packages(
				id BIGSERIAL PRIMARY KEY,
				source_id BIGINT REFERENCES sources(id) ON DELETE CASCADE,
				insource_id BIGINT DEFAULT NULL,
				name TEXT,
				url_id BIGINT REFERENCES urls(id) ON DELETE CASCADE,
				repo_id BIGINT REFERENCES repositories(id) ON DELETE CASCADE,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				created_at TIMESTAMP DEFAULT NULL,
				UNIQUE(source_id,insource_id)
				);

				CREATE INDEX IF NOT EXISTS packages_idx ON packages(source_id,name);
				CREATE INDEX IF NOT EXISTS packages_date_idx ON packages(created_at);
				CREATE INDEX IF NOT EXISTS packages_repo_idx ON packages(repo_id);

				CREATE TABLE IF NOT EXISTS merged_repositories(
				id BIGSERIAL PRIMARY KEY,
				new_repo TEXT,
				obsolete_repo TEXT,
				merged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);
				'''

			self.cursor.execute(self.DB_INIT)

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
			self.cursor.execute('DROP TABLE IF EXISTS packages;')
			self.cursor.execute('DROP TABLE IF EXISTS followers;')
			self.cursor.execute('DROP TABLE IF EXISTS stars;')
			self.cursor.execute('DROP TABLE IF EXISTS forks;')
			self.cursor.execute('DROP TABLE IF EXISTS commit_repos;')
			self.cursor.execute('DROP TABLE IF EXISTS commit_parents;')
			self.cursor.execute('DROP TABLE IF EXISTS commits;')
			self.cursor.execute('DROP TABLE IF EXISTS table_updates;')
			self.cursor.execute('DROP TABLE IF EXISTS merged_identities;')
			self.cursor.execute('DROP TABLE IF EXISTS identities;')
			self.cursor.execute('DROP TABLE IF EXISTS users;')
			self.cursor.execute('DROP TABLE IF EXISTS identity_types;')
			self.cursor.execute('DROP TABLE IF EXISTS full_updates;')
			self.cursor.execute('DROP TABLE IF EXISTS download_attempts;')
			self.cursor.execute('DROP TABLE IF EXISTS repositories;')
			self.cursor.execute('DROP TABLE IF EXISTS urls;')
			self.cursor.execute('DROP TABLE IF EXISTS sources;')
			self.connection.commit()


	def fill_db(self):
		for f in self.fillers:
			f.prepare()
			f.apply()
			self.logger.info('Filled with filler {}'.format(f.name))

	def add_filler(self,f):
		if f.name in [ff.name for ff in self.fillers if ff.unique]:
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
			self.cursor.executemany(''' INSERT INTO urls(source,source_root,url,cleaned_url)
				 VALUES((SELECT id FROM sources WHERE name=?),
				 				?,
								?,(SELECT id FROM urls WHERE url=?)) ON CONFLICT(url) DO UPDATE
								SET cleaned_url=excluded.cleaned_url;''',((source,source_root_id,url,url_cleaned) for url,url_cleaned,source_root_id in url_list))

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


	def register_packages(self,source,package_list,autocommit=True):
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
		else:
			self.cursor.executemany('''
				INSERT OR IGNORE INTO packages(repo_id,source_id,insource_id,name,created_at,url_id)
				VALUES(
					(SELECT r.id FROM urls u
						INNER JOIN repositories r ON r.url_id=u.cleaned_url
						AND u.url=?),?,?,?,?,(SELECT id FROM urls WHERE url=?))
				;''',((p[-1],source_id,*p) for p in package_list))
		if autocommit:
			self.connection.commit()

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
				SELECT s.name,r.owner,r.name,r.id,tu.updated_at
				FROM repositories r
				INNER JOIN sources s
				ON s.id=r.source
				LEFT OUTER JOIN table_updates tu
				ON tu.repo_id=r.id AND tu.table_name='stars'
				ORDER BY s.name,r.owner,r.name
				;''')
			return [{'source':r[0],'owner':r[1],'name':r[2],'repo_id':r[3],'last_star_update':r[4]} for r in self.cursor.fetchall()]

		elif option == 'starinfo':
			self.cursor.execute('''
				SELECT s.name,r.owner,r.name,r.id,tu.updated_at,tu.success
				FROM repositories r
				INNER JOIN sources s
				ON s.id=r.source
				LEFT OUTER JOIN table_updates tu
				ON tu.repo_id=r.id AND tu.table_name='stars'
				ORDER BY s.name,r.owner,r.name
				;''')
			return list(self.cursor.fetchall())

		elif option == 'forkinfo':
			self.cursor.execute('''
				SELECT s.name,r.owner,r.name,r.id,tu.updated_at,tu.success
				FROM repositories r
				INNER JOIN sources s
				ON s.id=r.source
				LEFT OUTER JOIN table_updates tu
				ON tu.repo_id=r.id AND tu.table_name='forks'
				ORDER BY s.name,r.owner,r.name
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

	def get_last_star(self,source,repo,owner):
		'''
		returns a dict with created_at, starred_at and login for the last registered star. All to None if does not exist
		'''
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

	def count_stars(self,source,repo,owner):
		'''
		Counts registered starring events of a repo
		'''
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

	def count_forks(self,source,repo,owner):
		'''
		Counts registered forks of a repo
		'''
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
		ans = self.cursor.fetchone()[0] # When no count, result is (None,)
		if ans is None:
			return 0
		else:
			return ans

	def count_users(self):
		self.cursor.execute('''
				SELECT COUNT(*) FROM users
				;''')
		ans = self.cursor.fetchone()[0] # When no count, result is (None,)
		if ans is None:
			return 0
		else:
			return ans

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
		ans = self.cursor.fetchone()[0] # When no count, result is (None,)
		if ans is None:
			return 0
		else:
			return ans

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


	def insert_update(self,table,repo_id=None,identity_id=None,success=True):
		'''
		Inserting an update in table_updates
		'''
		if self.db_type == 'postgres':
			self.cursor.execute('''INSERT INTO table_updates(repo_id,identity_id,table_name,success)
				VALUES(%s,%s,%s,%s)
				;''', (repo_id,identity_id,table,success))
		else:
			self.cursor.execute('''INSERT INTO table_updates(repo_id,identity_id,table_name,success)
				VALUES(?,?,?,?)
				;''', (repo_id,identity_id,table,success))
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
				SELECT user_id FROM identities WHERE id=%s
				;''',(identity2,))
		else:
			self.cursor.execute('''
				SELECT user_id FROM identities WHERE id=?
				;''',(identity2,))
		old_user_id2 = self.cursor.fetchone()[0]

		#Getting common user_id that will be used at the end
		if self.db_type == 'postgres':
			self.cursor.execute('''
				SELECT user_id FROM identities WHERE id=%s
				;''',(identity1,))
		else:
			self.cursor.execute('''
				SELECT user_id FROM identities WHERE id=?
				;''',(identity1,))
		user_id = self.cursor.fetchone()[0]

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

	def merge_repos(self,
		obsolete_id=None,
		obsolete_source=None,
		obsolete_owner=None,
		obsolete_name=None,
		new_id=None,
		new_source=None,
		new_owner=None,
		new_name=None,
		merging_reason_source='repository merge process'
		):
		'''
		Merge process of two repositories, e.g. when a URL redirect is detected. All information attributed to the new repo.
		Table_updates of obsolete are deleted (through cascade), just in case one forgets to update one table in the repo_id update process.
		'''

		# checks
		if (new_id is None and (new_owner is None or new_name is None)) or (obsolete_id is None and (obsolete_owner is None or obsolete_name is None)):
			raise SyntaxError('Insufficent info provided for merging repositories (id,source,owner,name): \n new ({},{},{},{}) \n obsolete ({},{},{},{})'.format(new_id,new_source,new_owner,new_name,obsolete_id,obsolete_source,obsolete_owner,obsolete_name))

		if new_id is None:
			new_id = self.get_repo_id(source=new_source,owner=new_owner,name=new_name)
		if obsolete_id is None:
			obsolete_id = self.get_repo_id(source=obsolete_source,owner=obsolete_owner,name=obsolete_name)
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
				url = 'https://{}/{}/{}'.format(url_root,new_owner,new_name)

				if self.db_type == 'postgres':
					self.cursor.execute('SELECT name FROM source WHERE id=%s;',(new_source_id,))
				else:
					self.cursor.execute('SELECT name FROM source WHERE id=?;',(new_source_id,))
				new_source_name = self.cursor.fetchone()[0]

				# add url
				self.register_urls(urls=[(url,url,new_source_id)],source=merging_reason_source)

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

					old_url = self.cursor.fetchone()[0]

					self.cursor.execute('''
						UPDATE repositories SET
							url_id=(SELECT id FROM urls WHERE url=%s),
							source_id=%s,
							owner=%s,
							name=%s
						WHERE id=%s
						;''',(url,new_source_id,new_owner,new_name,obsolete_id))
				else:
					self.cursor.execute('''SELECT cu.url
							FROM urls cu
							INNER JOIN urls u
								ON cu.id=u.cleaned_url
							INNER JOIN repositories r
								ON r.url_id=u.id
								AND r.id=?
						;''',(obsolete_id,))

					old_url = self.cursor.fetchone()[0]

					self.cursor.execute('''
						UPDATE repositories SET
							url_id=(SELECT id FROM urls WHERE url=?),
							source_id=?,
							owner=?,
							name=?
						WHERE id=?
						;''',(url,new_source_id,new_owner,new_name,obsolete_id))


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

					old_url = self.cursor.fetchone()[0]

					self.cursor.execute('''SELECT cu.url
							FROM urls cu
							INNER JOIN urls u
								ON cu.id=u.cleaned_url
							INNER JOIN repositories r
								ON r.url_id=u.id
								AND r.id=%s
						;''',(new_id,))

					url = self.cursor.fetchone()[0]

					self.cursor.execute('''
						UPDATE packages SET repo_id=%s
						WHERE repo_id=%s
						ON CONFLICT DO NOTHING
						;''',(new_id,obsolete_id))
					self.cursor.execute('''
						UPDATE commits SET repo_id=%s
						WHERE repo_id=%s
						ON CONFLICT DO NOTHING
						;''',(new_id,obsolete_id))
					self.cursor.execute('''
						UPDATE stars SET repo_id=%s
						WHERE repo_id=%s
						ON CONFLICT DO NOTHING
						;''',(new_id,obsolete_id))
					self.cursor.execute('''
						UPDATE commit_repos SET repo_id=%s
						WHERE repo_id=%s
						ON CONFLICT DO NOTHING
						;''',(new_id,obsolete_id))
					self.cursor.execute('''
						UPDATE forks SET forked_repo_id=%s
						WHERE forked_repo_id=%s
						ON CONFLICT DO NOTHING
						;''',(new_id,obsolete_id))
					self.cursor.execute('''
						UPDATE forks SET forking_repo_id=%s,forking_repo_url=%s
						WHERE forking_repo_id=%s
						ON CONFLICT DO NOTHING
						;''',(new_id,url[8:],obsolete_id))


					self.cursor.execute('DELETE FROM repositories WHERE id=%s CASCADE;',(obsolete_id,))


				else:
					self.cursor.execute('''SELECT cu.url
							FROM urls cu
							INNER JOIN urls u
								ON cu.id=u.cleaned_url
							INNER JOIN repositories r
								ON r.url_id=u.id
								AND r.id=?
						;''',(obsolete_id,))

					old_url = self.cursor.fetchone()[0]

					self.cursor.execute('''SELECT cu.url
							FROM urls cu
							INNER JOIN urls u
								ON cu.id=u.cleaned_url
							INNER JOIN repositories r
								ON r.url_id=u.id
								AND r.id=?
						;''',(new_id,))

					url = self.cursor.fetchone()[0]

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
						;''',(new_id,obsolete_id))
					self.cursor.execute('''
						UPDATE OR IGNORE commit_repos SET repo_id=?
						WHERE repo_id=?
						;''',(new_id,obsolete_id))
					self.cursor.execute('''
						UPDATE OR IGNORE forks SET forked_repo_id=?
						WHERE forked_repo_id=?
						;''',(new_id,obsolete_id))
					self.cursor.execute('''
						UPDATE OR IGNORE forks SET forking_repo_id=?,forking_repo_url=?
						WHERE forking_repo_id=?
						;''',(new_id,url[8:],obsolete_id))


					self.cursor.execute('DELETE FROM repositories WHERE id=? CASCADE;',(obsolete_id,))


			if self.db_type == 'postgres':
				self.cursor.execute('''
						INSERT INTO merged_repositories(new_repo,obsolete_repo)
						VALUES(%s,%s)
						;''',(url,old_url))
			else:
				self.cursor.execute('''
						INSERT INTO merged_repositories(new_repo,obsolete_repo)
						VALUES(?,?)
						;''',(url,old_url))

			self.connection.commit()
