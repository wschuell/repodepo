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

	def __init__(self,db_type='sqlite',db_name='repo_tools',db_folder='.',db_user='postgres',port='5432',host='localhost',password=None,clean_first=False,do_init=True,timeout=5):
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
				self.connection = sqlite3.connect(self.db_path,timeout=timeout)
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
		if do_init:
			self.init_db()
		self.logger = logger

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
			DB_INIT = '''
				CREATE TABLE IF NOT EXISTS sources(
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL UNIQUE,
				url_root TEXT NOT NULL UNIQUE
				);

				CREATE TABLE IF NOT EXISTS repositories(
				id INTEGER PRIMARY KEY,
				source INTEGER REFERENCES sources(id) ON DELETE CASCADE,
				owner TEXT,
				name TEXT,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP DEFAULT NULL,
				latest_commit_time TIMESTAMP DEFAULT NULL,
				cloned BOOLEAN DEFAULT 0,
				UNIQUE(source,owner,name)
				);

				CREATE TABLE IF NOT EXISTS urls(
				source INTEGER REFERENCES sources(id) ON DELETE CASCADE,
				repo_url TEXT NOT NULL,
				repo_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY(source,repo_url)
				);

				CREATE TABLE IF NOT EXISTS stars(
				repo_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
				login TEXT NOT NULL,
				starred_at TIMESTAMP NOT NULL,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY(repo_id,login)
				);

				CREATE INDEX IF NOT EXISTS stars_idx ON stars(repo_id,starred_at);
				CREATE INDEX IF NOT EXISTS stars_idx2 ON stars(repo_id,created_at);

				CREATE TABLE IF NOT EXISTS users(
				id INTEGER PRIMARY KEY,
				name TEXT,
				email TEXT UNIQUE,
				github_login TEXT,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);

				CREATE INDEX IF NOT EXISTS users_gh_idx ON users(github_login);

				CREATE TABLE IF NOT EXISTS commits(
				id INTEGER PRIMARY KEY,
				sha TEXT,
				author_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
				repo_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
				created_at TIMESTAMP,
				insertions INTEGER,
				deletions INTEGER,
				UNIQUE(sha)
				);

				CREATE TABLE IF NOT EXISTS commit_parents(
				child_id INTEGER REFERENCES commits(id) ON DELETE CASCADE,
				parent_id INTEGER REFERENCES commits(id) ON DELETE CASCADE,
				rank INTEGER,
				PRIMARY KEY(child_id,parent_id),
				UNIQUE(parent_id,child_id,rank)
				);

				CREATE TABLE IF NOT EXISTS table_updates(
				id INTEGER PRIMARY KEY,
				repo_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
				user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
				table_name TEXT,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				success BOOLEAN DEFAULT 1,
				latest_commit_time TIMESTAMP DEFAULT NULL
				);

				CREATE INDEX IF NOT EXISTS table_updates_idx ON table_updates(repo_id,table_name,updated_at);
				CREATE INDEX IF NOT EXISTS table_updates_user_idx ON table_updates(user_id,table_name,updated_at);

				CREATE TABLE IF NOT EXISTS full_updates(
				update_type TEXT,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);

				CREATE TABLE IF NOT EXISTS followers(
				id INTEGER PRIMARY KEY,
				github_login TEXT NOT NULL,
				followers INTEGER,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);

				CREATE INDEX IF NOT EXISTS followers_idx ON followers(github_login,created_at);
				CREATE INDEX IF NOT EXISTS followers_idx2 ON followers(created_at);


		'''
			for q in DB_INIT.split(';')[:-1]:
				self.cursor.execute(q)
			self.connection.commit()
		elif self.db_type == 'postgres':
			self.cursor.execute('''
				CREATE TABLE IF NOT EXISTS sources(
				id BIGSERIAL PRIMARY KEY,
				name TEXT NOT NULL UNIQUE,
				url_root TEXT NOT NULL UNIQUE
				);

				CREATE TABLE IF NOT EXISTS repositories(
				id BIGSERIAL PRIMARY KEY,
				source BIGINT REFERENCES sources(id) ON DELETE CASCADE,
				owner TEXT,
				name TEXT,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				latest_commit_time TIMESTAMP DEFAULT NULL,
				cloned BOOLEAN DEFAULT false,
				UNIQUE(source,owner,name)
				);

				CREATE TABLE IF NOT EXISTS urls(
				source BIGINT REFERENCES sources(id) ON DELETE CASCADE,
				repo_url TEXT NOT NULL,
				repo_id BIGINT REFERENCES repositories(id) ON DELETE CASCADE,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY(source,repo_url)
				);

				CREATE TABLE IF NOT EXISTS stars(
				repo_id BIGINT REFERENCES repositories(id),
				login TEXT NOT NULL,
				starred_at TIMESTAMP NOT NULL,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY(repo_id,login)
				);

				CREATE INDEX IF NOT EXISTS stars_idx ON stars(repo_id,starred_at);
				CREATE INDEX IF NOT EXISTS stars_idx2 ON stars(repo_id,created_at);

			CREATE TABLE IF NOT EXISTS users(
				id BIGSERIAL PRIMARY KEY,
				name TEXT,
				email TEXT UNIQUE,
				github_login TEXT,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);

				CREATE TABLE IF NOT EXISTS commits(
				id BIGSERIAL PRIMARY KEY,
				sha TEXT,
				author_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
				repo_id BIGINT REFERENCES repositories(id) ON DELETE CASCADE,
				created_at TIMESTAMP,
				insertions INT,
				deletions INT,
				UNIQUE(sha)
				);

				CREATE TABLE IF NOT EXISTS commit_parents(
				child_id BIGINT REFERENCES commits(id) ON DELETE CASCADE,
				parent_id BIGINT REFERENCES commits(id) ON DELETE CASCADE,
				rank INT,
				PRIMARY KEY(child_id,parent_id),
				UNIQUE(parent_id,child_id,rank)
				);

				CREATE TABLE IF NOT EXISTS table_updates(
				id BIGSERIAL PRIMARY KEY,
				repo_id BIGINT REFERENCES repositories(id) ON DELETE CASCADE,
				user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
				table_name TEXT,
				success BOOLEAN DEFAULT true,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				latest_commit_time TIMESTAMP DEFAULT NULL
				);

				CREATE INDEX IF NOT EXISTS table_updates_idx ON table_updates(repo_id,table_name,updated_at);
				CREATE INDEX IF NOT EXISTS table_updates_user_idx ON table_updates(user_id,table_name,updated_at);

				CREATE TABLE IF NOT EXISTS full_updates(
				update_type TEXT,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);


				CREATE TABLE IF NOT EXISTS followers(
				id BIGSERIAL PRIMARY KEY,
				github_login TEXT NOT NULL,
				followers BIGINT,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);

				CREATE INDEX IF NOT EXISTS followers_idx ON followers(github_login,created_at);
				CREATE INDEX IF NOT EXISTS followers_idx2 ON followers(created_at);

				''')

			self.connection.commit()

	def clean_db(self):
		'''
		Dropping tables
		If there is a change in structure in the init script, this method should be called to 'reset' the state of the database
		'''
		logger.info('Cleaning database')
		self.cursor.execute('DROP TABLE IF EXISTS followers;')
		self.cursor.execute('DROP TABLE IF EXISTS commit_parents;')
		self.cursor.execute('DROP TABLE IF EXISTS commits;')
		self.cursor.execute('DROP TABLE IF EXISTS table_updates;')
		self.cursor.execute('DROP TABLE IF EXISTS users;')
		self.cursor.execute('DROP TABLE IF EXISTS stars;')
		self.cursor.execute('DROP TABLE IF EXISTS full_updates;')
		self.cursor.execute('DROP TABLE IF EXISTS download_attempts;')
		self.cursor.execute('DROP TABLE IF EXISTS urls;')
		self.cursor.execute('DROP TABLE IF EXISTS repositories;')
		self.cursor.execute('DROP TABLE IF EXISTS sources;')
		self.connection.commit()

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

	def register_source(self,source,source_urlroot):
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
					AND now() - f.created_at > %s*'1 second'::interval
					GROUP BY u.github_login
					HAVING f.github_login IS NULL
					;''',(time_delay,))
			else:
				self.cursor.execute('''
					SELECT u.github_login FROM
						(SELECT DISTINCT uu.github_login FROM users uu
						WHERE uu.github_login IS NOT NULL) AS u
					LEFT JOIN followers f
					ON f.github_login=u.github_login
					AND (julianday('now') - julianday(f.created_at))*24*3600 > ?
					GROUP BY u.github_login
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

	def create_indexes(self,table=None):
		'''
		Creating indexes for the various tables that are not specified at table creation, where insertion time could be impacted by their presence
		'''
		if table == 'user' or table is None:
			self.logger.info('Creating indexes for table user')
			self.cursor.execute('''
				CREATE INDEX IF NOT EXISTS user_names_idx ON users(name)
				;''')
		elif table == 'commits' or table is None:
			self.logger.info('Creating indexes for table commits')
			self.cursor.execute('''
				CREATE INDEX IF NOT EXISTS commits_ac_idx ON commits(author_id,created_at)
				;''')
			self.cursor.execute('''
				CREATE INDEX IF NOT EXISTS commits_rc_idx ON commits(repo_id,created_at)
				;''')
			self.cursor.execute('''
				CREATE INDEX IF NOT EXISTS commits_cra_idx ON commits(created_at,repo_id,author_id)
				;''')
		elif table == 'commit_parents' or table is None:
			self.logger.info('Creating indexes for table commit_parents')
			pass
		self.connection.commit()

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

	def get_source_info(self,source):
		'''
		Returns source_urlroot if source exists, otherwise throws and error
		'''
		if self.db_type == 'postgres':
			self.cursor.execute('SELECT id,url_root FROM sources WHERE name=%s;',(source,))
		else:
			self.cursor.execute('SELECT id,url_root FROM sources WHERE name=?;',(source,))
		ans = self.cursor.fetchone()
		if ans is None:
			raise ValueError('Unregistered source {}'.format(source))
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

	def insert_update(self,table,repo_id=None,user_id=None,success=True):
		'''
		Inserting an update in table_updates
		'''
		if self.db_type == 'postgres':
			self.cursor.execute('''INSERT INTO table_updates(repo_id,user_id,table_name,success)
				VALUES(%s,%s,%s,%s)
				;''', (repo_id,user_id,table,success))
		else:
			self.cursor.execute('''INSERT INTO table_updates(repo_id,user_id,table_name,success)
				VALUES(?,?,?,?)
				;''', (repo_id,user_id,table,success))
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

	def set_gh_login(self,user_id,login,autocommit=True):
		'''
		Sets a login for a given user (id refers to a unique email, which can refer to several logins)
		'''
		if self.db_type == 'postgres':
			self.cursor.execute('''UPDATE users SET github_login=%s WHERE id=%s;''',(login,user_id))
			self.cursor.execute('''INSERT INTO table_updates(user_id,table_name,success) VALUES(%s,'login',%s);''',(user_id,(login is not None)))
		else:
			self.cursor.execute('''UPDATE users SET github_login=? WHERE id=?;''',(login,user_id))
			self.cursor.execute('''INSERT INTO table_updates(user_id,table_name,success) VALUES(?,'login',?);''',(user_id,(login is not None)))

		if autocommit:
			self.connection.commit()
