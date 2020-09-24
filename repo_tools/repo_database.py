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
	A simple database to store the data and query it efficiently.

	By default SQLite is used, but PostgreSQL is also an option
	'''

	def __init__(self,db_type='sqlite',db_name='repo_tools',db_folder='.',db_user='postgres',port='5432',host='localhost',password=None,clean_first=False):
		self.db_type = db_type
		if db_type == 'sqlite':
			if db_name.startswith(':memory:'):
				self.connection = sqlite3.connect(db_name)
				self.in_ram = True
			else:
				self.in_ram = False
				self.db_path = os.path.join(db_folder,'{}.db'.format(db_name))
				if not os.path.exists(db_folder):
					os.makedirs(db_folder)
				self.connection = sqlite3.connect(self.db_path)
			self.cursor = self.connection.cursor()
		elif db_type == 'postgres':
			if password is not None:
				logger.warning('You are providing your password directly, this could be a security concern, consider using solutions like .pgpass file.')
			self.connection = psycopg2.connect(user=db_user,port=port,host=host,database=db_name,password=password)
			self.cursor = self.connection.cursor()
		else:
			raise ValueError('Unknown DB type: {}'.format(db_type))

		if clean_first:
			self.clean_db()
		self.init_db()
		self.logger = logger

	def init_db(self):
		'''
		Initializing the database, with correct tables, constraints and indexes.
		'''
		logger.info('Creating database ({}) table and indexes'.format(self.db_type))
		if self.db_type == 'sqlite':
			DB_INIT = '''
				CREATE TABLE IF NOT EXISTS sources(
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL UNIQUE,
				url_root TEXT NOt NULL UNIQUE
				);

				CREATE TABLE IF NOT EXISTS repositories(
				id INTEGER PRIMARY KEY,
				source INTEGER REFERENCES sources(id) ON DELETE CASCADE,
				owner TEXT,
				name TEXT,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				-- updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				UNIQUE(source,owner,name)
				);

				CREATE TABLE IF NOT EXISTS download_attempts(
				repo_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
				attempted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				success BOOLEAN DEFAULT 0
				);


				CREATE TABLE IF NOT EXISTS urls(
				source INTEGER REFERENCES sources(id) ON DELETE CASCADE,
				repo_url TEXT NOT NULL,
				repo_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY(source,repo_url)
				);
				'''
			for q in DB_INIT.split(';')[:-1]:
				self.cursor.execute(q)
			self.connection.commit()
		elif self.db_type == 'postgres':
			self.cursor.execute('''
				CREATE TABLE IF NOT EXISTS projects(
				id BIGSERIAL PRIMARY KEY,
				name TEXT UNIQUE,
				created_at TIMESTAMP NOT NULL
				);
				''')

			self.connection.commit()

	def clean_db(self):
		'''
		Dropping tables
		If there is a change in structure in the init script, this method should be called to 'reset' the state of the database
		'''
		logger.info('Cleaning database')
		self.cursor.execute('DROP TABLE IF EXISTS download_attempts;')
		self.cursor.execute('DROP TABLE IF EXISTS repositories;')
		self.cursor.execute('DROP TABLE IF EXISTS urls;')
		self.cursor.execute('DROP TABLE IF EXISTS sources;')
		self.connection.commit()

	def register_repo(self,source,owner,repo):
		'''
		Putting a repo in the database
		'''
		if self.db_type == 'postgres':
			self.cursor.execute(''' INSERT INTO repositories(source,owner,name)
				 VALUES((SELECT id FROM sources WHERE name=%s),
								%s,
								%s) ON CONFLICT DO NOTHING; ''',(source,owner,repo))
		else:
			self.cursor.execute(''' INSERT OR IGNORE INTO repositories(source,owner,name)
				 VALUES((SELECT id FROM sources WHERE name=?),
								?,
								?);''',(source,owner,repo))
		self.connection.commit()

	def register_source(self,source,source_urlroot):
		'''
		Putting a source in the database
		'''
		if self.db_type == 'postgres':
			self.cursor.execute(''' INSERT INTO sources(name,url_root)
				 VALUES(
								%s,%s) ON CONFLICT DO NOTHING;''',(source,source_urlroot))
		else:
			self.cursor.execute(''' INSERT INTO sources(name,url_root)
				 VALUES(
								?,?) ON CONFLICT DO NOTHING;''',(source,source_urlroot))
		self.connection.commit()

	def register_url(self,source,repo_url,repo_id=None):
		'''
		Putting URLs in the database
		'''
		if self.db_type == 'postgres':
			self.cursor.execute(''' INSERT INTO urls(source,repo_url,repo_id)
				 VALUES((SELECT id FROM sources WHERE name=%s),
								%s,%s) ON CONFLICT DO NOTHING;''',(source,repo_url,repo_id))
		else:
			self.cursor.execute(''' INSERT OR IGNORE INTO urls(source,repo_url,repo_id)
				 VALUES((SELECT id FROM sources WHERE name=?),
								?,?);''',(source,repo_url,repo_id))
		self.connection.commit()

	def update_url(self,source,repo_url,repo_id):
		'''
		Updating a URL in the database
		'''
		if self.db_type == 'postgres':
			self.cursor.execute(''' UPDATE urls SET repo_id=%s WHERE
				 source=(SELECT id FROM sources WHERE name=%s
								AND repo_url=%s);''',(repo_id,source,repo_url))
		else:
			self.cursor.execute(''' UPDATE urls SET repo_id=? WHERE
				 source=(SELECT id FROM sources WHERE name=?
								AND repo_url=?);''',(repo_id,source,repo_url))
		self.connection.commit()

	def get_repo_id(self,owner,name,source):
		'''
		Getting repo id, None if not in DB
		'''
		if self.db_type == 'postgres':
			self.cursor.execute(''' SELECT id FROM repositories WHERE
									source=(SELECT id FROM sources WHERE name=%s)
									AND owner=%s
									AND name=%s;''',(source,owner,name))
		else:
			self.cursor.execute(''' SELECT id FROM repositories WHERE
									source=(SELECT id FROM sources WHERE name=?)
									AND owner=?
									AND name=?;''',(source,owner,name))
		repo_id = self.cursor.fetchone()
		if repo_id is None:
			return None
		else:
			return repo_id[0]
		
	def submit_download_attempt(self,source,owner,repo,success):
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
		if self.db_type == 'postgres':
			self.cursor.execute(''' INSERT INTO download_attempts(repo_id,success)
			 VALUES(%s,%s);''',(repo_id,success))
		else:
			self.cursor.execute(''' INSERT INTO download_attempts(repo_id,success)
			 VALUES(?,?);''',(repo_id,success))
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

		elif option == 'no_dl':

			self.cursor.execute('''
				SELECT s.name,s.url_root,r.owner,r.name
				FROM repositories r
				INNER JOIN sources s
				ON s.id=r.source
				INNER JOIN download_attempts da
				ON da.repo_id=r.id
				GROUP BY s.name,s.url_root,r.owner,r.name
				HAVING COUNT(*)=0
				ORDER BY s.name,r.owner,r.name
				
				;''')
			return list(self.cursor.fetchall())

		else:
			raise ValueError('Unknown option for repo_list: {}'.format(option))