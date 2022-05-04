import psycopg2
import psycopg2.extras
import itertools
from collections import OrderedDict

from .stats import DBStats

class GHTorrentStats(DBStats):
	'''
	Comparison of data present in the database with data present in a GHTorrent SQL DB
	Creation of temporary tables should be granted on the GHTorrent DB
	'''
	def __init__(self,ght_cur,ght_conn,max_time=None,full_results=False,limit=None,**kwargs):
		self.ght_cur = ght_cur
		self.ght_conn = ght_conn
		self.limit = limit
		self.full_results = full_results
		DBStats.__init__(self,**kwargs)
		if max_time is None:
			ght_cur.execute('SELECT MAX(created_at) FROM users;')
			self.max_time = ght_cur.fetchone()[0]
		else:
			self.max_time = max_time

	def fill_temp_commits(self):
		'''
		Temporary table with commits
		sha
		'''
		self.ght_cur.execute('''CREATE TEMP TABLE IF NOT EXISTS temp_repo_tools_commits(
							sha TEXT PRIMARY KEY
							);''')
		self.ght_cur.execute('SELECT * FROM temp_repo_tools_commits LIMIT 1;')
		if len(list(self.ght_cur.fetchall())) == 0:
			self.logger.info('Filling commits in GHTorrent DB temp table')

			self.db.cursor.execute('SELECT sha FROM commits WHERE created_at<=%(max_time)s',{'max_time':self.max_time})

			if self.limit is None:
				results = self.db.cursor.fetchall()
			else:
				results = itertools.islice(self.db.cursor.fetchall(),self.limit)

			psycopg2.extras.execute_batch(self.ght_cur,'''
				INSERT INTO temp_repo_tools_commits(sha)
				VALUES(%(sha)s)
				;''',({'sha':r[0]} for r in results))
			self.logger.info('Filled commits in GHTorrent DB temp table')


	def fill_temp_users(self):
		'''
		Temporary table with all users having a github account
		user_login
		'''
		self.ght_cur.execute('''CREATE TEMP TABLE IF NOT EXISTS temp_repo_tools_users(
							login TEXT PRIMARY KEY
							);''')
		self.ght_cur.execute('SELECT * FROM temp_repo_tools_users LIMIT 1;')
		if len(list(self.ght_cur.fetchall())) == 0:
			self.logger.info('Filling users in GHTorrent DB temp table')
			self.db.cursor.execute('''SELECT DISTINCT i.identity FROM identities i
				INNER JOIN identity_types it
				ON it.id=i.identity_type_id AND it.name='github_login'
				INNER JOIN identities i2
				ON i.user_id=i2.user_id
				INNER JOIN commits c
				ON c.author_id=i2.id
				 AND c.created_at<=%(max_time)s
				;''',{'max_time':self.max_time})

			if self.limit is None:
				results = self.db.cursor.fetchall()
			else:
				results = itertools.islice(self.db.cursor.fetchall(),self.limit)


			psycopg2.extras.execute_batch(self.ght_cur,'''
				INSERT INTO temp_repo_tools_users(login)
				VALUES(%(login)s)
				;''',({'login':r[0]} for r in results))
			self.logger.info('Filled users in GHTorrent DB temp table')

	def fill_temp_repos(self):
		'''
		Temporary table with all repositories
		owner,name,cloned
		'''
		self.ght_cur.execute('''CREATE TEMP TABLE IF NOT EXISTS temp_repo_tools_repos(
							owner TEXT,
							name TEXT,
							cloned BOOLEAN,
							PRIMARY KEY(owner,name)
							);''')
		self.ght_cur.execute('SELECT * FROM temp_repo_tools_repos LIMIT 1;')
		if len(list(self.ght_cur.fetchall())) == 0:
			self.logger.info('Filling repositories in GHTorrent DB temp table')
			self.db.cursor.execute('''SELECT DISTINCT r.owner,r.name,r.cloned FROM repositories r
				INNER JOIN commits c
				ON c.repo_id=r.id
				 AND c.created_at<=%(max_time)s
				;''',{'max_time':self.max_time})

			if self.limit is None:
				results = self.db.cursor.fetchall()
			else:
				results = itertools.islice(self.db.cursor.fetchall(),self.limit)


			psycopg2.extras.execute_batch(self.ght_cur,'''
				INSERT INTO temp_repo_tools_repos(owner,name,cloned)
				VALUES(%(owner)s,%(name)s,%(cloned)s)
				;''',({'owner':r[0],'name':r[1],'cloned':r[2]} for r in results))
			self.logger.info('Filled repos in GHTorrent DB temp table')

	def fill_temp_userrepos(self):
		'''
		Temporary table with all repositories-users links
		owner,name,cloned
		'''
		self.ght_cur.execute('''CREATE TEMP TABLE IF NOT EXISTS temp_repo_tools_userrepos(
							owner TEXT,
							name TEXT,
							login TEXT,
							PRIMARY KEY(owner,name,login)
							);''')
		self.ght_cur.execute('SELECT * FROM temp_repo_tools_userrepos LIMIT 1;')
		if len(list(self.ght_cur.fetchall())) == 0:
			self.logger.info('Filling user/repos edges in GHTorrent DB temp table')
			# self.db.cursor.execute('''SELECT DISTINCT r.owner,r.name,i2.identity FROM repositories r
			# 	INNER JOIN commits c
			# 	ON c.repo_id=r.id
			# 	 AND c.created_at<=%(max_time)s
			# 	INNER JOIN identities i
			# 	ON c.author_id=i.id
			# 	INNER JOIN identities i2
			# 	ON i2.user_id =i.user_id
			# 	INNER JOIN identity_types it
			# 	ON it.id=i2.identity_type_id AND it.name='github_login'
			# 	;''',{'max_time':self.max_time})
			self.db.cursor.execute('''SELECT DISTINCT r.owner,r.name,i2.identity FROM repositories r
				INNER JOIN commit_repos cr
				ON cr.repo_id=r.id
				INNER JOIN commits c
				ON cr.commit_id=c.id
				 AND c.created_at<=%(max_time)s
				INNER JOIN identities i
				ON c.author_id=i.id
				INNER JOIN identities i2
				ON i2.user_id =i.user_id
				INNER JOIN identity_types it
				ON it.id=i2.identity_type_id AND it.name='github_login'
				;''',{'max_time':self.max_time})

			if self.limit is None:
				results = self.db.cursor.fetchall()
			else:
				results = itertools.islice(self.db.cursor.fetchall(),self.limit)


			psycopg2.extras.execute_batch(self.ght_cur,'''
				INSERT INTO temp_repo_tools_userrepos(owner,name,login)
				VALUES(%(owner)s,%(name)s,%(user)s)
				;''',({'owner':r[0],'name':r[1],'user':r[2]} for r in results))

			self.ght_cur.execute('CREATE INDEX IF NOT EXISTS userrepo_uidx ON temp_repo_tools_userrepos(login);')
			self.logger.info('Filled user/repos edges in GHTorrent DB temp table')

	def fill_temp_userrepos_ght(self):
		'''
		Temporary table with all repositories-users links found in ght for the relevant repos
		owner,name,cloned
		'''
		self.fill_temp_repos()
		self.ght_cur.execute('''CREATE TEMP TABLE IF NOT EXISTS temp_repo_tools_userrepos_ght(
							owner TEXT,
							name TEXT,
							login TEXT,
							PRIMARY KEY(owner,name,login)
							);''')
		self.ght_cur.execute('SELECT * FROM temp_repo_tools_userrepos_ght LIMIT 1;')
		if len(list(self.ght_cur.fetchall())) == 0:
			self.logger.info('Filling user/repos edges GHT in GHTorrent DB temp table')
		
			self.ght_cur.execute('''
				INSERT INTO temp_repo_tools_userrepos_ght(owner,name,login)
				SELECT  MIN(u.login) AS rowner, MIN(r.name) AS rname, MIN(u2.login) AS u2login FROM temp_repo_tools_repos tr
				INNER JOIN projects r
				ON tr.name=r.name
				INNER JOIN users u
				ON r.owner_id=u.id
				AND tr.owner=u.login
					INNER JOIN project_commits cr
					ON cr.project_id=r.id
					INNER JOIN commits c
					ON c.id=cr.commit_id
				INNER JOIN users u2
				ON c.author_id=u2.id
				GROUP BY u2.id,r.id
				;''')

			self.ght_cur.execute('CREATE INDEX IF NOT EXISTS userrepo_ght_uidx ON temp_repo_tools_userrepos_ght(login);')
			self.logger.info('Filled user/repos edges GHT in GHTorrent DB temp table')

	def fill_temp_stars(self):
		'''
		Temporary table with stars
		starrer,repo_owner,repo_name
		'''
		self.ght_cur.execute('''CREATE TEMP TABLE IF NOT EXISTS temp_repo_tools_stars(
							starrer TEXT,
							owner TEXT,
							name TEXT,
							PRIMARY KEY(starrer,owner,name)
							);''')
		self.ght_cur.execute('SELECT * FROM temp_repo_tools_stars LIMIT 1;')
		if len(list(self.ght_cur.fetchall())) == 0:
			self.logger.info('Filling stars in GHTorrent DB temp table')
			self.db.cursor.execute('''SELECT DISTINCT s.login,r.owner,r.name FROM stars s
				INNER JOIN repositories r
				ON s.repo_id=r.id
				AND s.starred_at<=%(max_time)s
				;''',{'max_time':self.max_time})

			if self.limit is None:
				results = self.db.cursor.fetchall()
			else:
				results = itertools.islice(self.db.cursor.fetchall(),self.limit)


			psycopg2.extras.execute_batch(self.ght_cur,'''
				INSERT INTO temp_repo_tools_stars(starrer,owner,name)
				VALUES(%(starrer)s,%(owner)s,%(name)s)
				;''',({'owner':r[1],'name':r[2],'starrer':r[0]} for r in results))
			self.logger.info('Filled stars in GHTorrent DB temp table')

	def fill_temp_forks(self):
		'''
		Temporary table with forks
		forked_owner,forked_name,forking_owner,forking_name
		'''
		self.ght_cur.execute('''CREATE TEMP TABLE IF NOT EXISTS temp_repo_tools_forks(
							forked_owner TEXT,
							forked_name TEXT,
							forking_owner TEXT,
							forking_name TEXT,
							PRIMARY KEY(forking_owner,forking_name)
							);''')
		self.ght_cur.execute('SELECT * FROM temp_repo_tools_forks LIMIT 1;')
		if len(list(self.ght_cur.fetchall())) == 0:
			self.logger.info('Filling forks in GHTorrent DB temp table')
			self.db.cursor.execute('''SELECT DISTINCT f.forking_repo_url,r.owner,r.name FROM forks f
				INNER JOIN repositories r
				ON f.forked_repo_id=r.id
				AND f.forked_at<=%(max_time)s
				AND f.fork_rank=1
				AND f.forking_repo_url LIKE 'github.com/%%'
				;''',{'max_time':self.max_time})

			if self.limit is None:
				results = self.db.cursor.fetchall()
			else:
				results = itertools.islice(self.db.cursor.fetchall(),self.limit)


			psycopg2.extras.execute_batch(self.ght_cur,'''
				INSERT INTO temp_repo_tools_forks(forked_owner,forked_name,forking_owner,forking_name)
				VALUES(%(forked_owner)s,%(forked_name)s,%(forking_owner)s,%(forking_name)s)
				;''',({'forked_owner':r[1],'forked_name':r[2],'forking_owner':r[0][11:].split('/')[0],'forking_name':r[0][11:].split('/')[1]} for r in results))
			self.logger.info('Filled forks in GHTorrent DB temp table')

class UserGHTStats(GHTorrentStats):
	def get(self,**kwargs):
		self.fill_temp_users()
		self.fill_temp_repos()

		results = OrderedDict()

		self.ght_cur.execute('SELECT COUNT(*) FROM temp_repo_tools_users;')
		results['users_rust'] = self.ght_cur.fetchone()[0]

		if self.full_results:
			self.ght_cur.execute('SELECT COUNT(*) FROM users;')
			results['users_ght_total'] = self.ght_cur.fetchone()[0]



		self.ght_cur.execute('''SELECT COUNT(DISTINCT u2.id),COUNT(DISTINCT tu.login) FROM temp_repo_tools_repos tr
			INNER JOIN projects r
			ON tr.name=r.name
			INNER JOIN users u
			ON r.owner_id=u.id
			AND tr.owner=u.login
			INNER JOIN commits c
			ON c.project_id=r.id
			INNER JOIN users u2
			ON u2.id=c.author_id
			LEFT OUTER JOIN temp_repo_tools_users tu
			ON tu.login=u2.login
			;''')
		u_ght,u_common1 = self.ght_cur.fetchone()
		results['users_ght'] = u_ght
		results['users_missing_rust'] = u_ght - u_common1

		results['users_common'] = u_common1
		results['users_missing_ght'] = results['users_rust'] - results['users_common']


		self.ght_cur.execute('''SELECT COUNT(*) FROM temp_repo_tools_users tu
			INNER JOIN users u
			ON tu.login=u.login
			;''')
		results['globalusers_common'] = self.ght_cur.fetchone()[0]
		results['globalusers_missing_ght'] = results['users_rust'] - results['users_common']


		self.logger.info('Computed users result')

		return results


class CommitsGHTStats(GHTorrentStats):
	def get(self,**kwargs):
		self.fill_temp_commits()
		self.fill_temp_repos()

		results = OrderedDict()

		self.ght_cur.execute('SELECT COUNT(*) FROM temp_repo_tools_commits;')
		results['commits_rust'] = self.ght_cur.fetchone()[0]

		if self.full_results:
			self.ght_cur.execute('SELECT COUNT(*) FROM commits;')
			results['commits_ght_total'] = self.ght_cur.fetchone()[0]

		self.ght_cur.execute('''SELECT COUNT(DISTINCT c.id),COUNT(DISTINCT tc.sha) FROM temp_repo_tools_repos tr
			INNER JOIN projects r
			ON tr.name=r.name
			INNER JOIN users u
			ON r.owner_id=u.id
			AND tr.owner=u.login
			INNER JOIN commits c
			ON c.project_id=r.id
			INNER JOIN users u2
			ON u2.id=c.author_id
			LEFT OUTER JOIN temp_repo_tools_commits tc
			ON tc.sha=c.sha
			;''')
		c_ght,c_common1 = self.ght_cur.fetchone()
		results['commits_ght'] = c_ght
		results['commits_missing_rust'] = c_ght - c_common1
	
		results['commits_common'] = c_common1
		results['commits_missing_ght'] = results['commits_rust'] - results['commits_common']

		self.ght_cur.execute('''SELECT COUNT(*) FROM temp_repo_tools_commits tc
			INNER JOIN commits c
			ON tc.sha=c.sha
			;''')
		results['globalcommits_common'] = self.ght_cur.fetchone()[0]
		results['globalcommits_missing_ght'] = results['commits_rust'] - results['globalcommits_common']


		self.logger.info('Computed commits result')

		return results

class ReposGHTStats(GHTorrentStats):
	def get(self,**kwargs):
		self.fill_temp_repos()

		results = OrderedDict()

		self.ght_cur.execute('SELECT COUNT(*) FROM temp_repo_tools_repos;')
		results['repos_rust'] = self.ght_cur.fetchone()[0]

		if self.full_results:
			self.ght_cur.execute('SELECT COUNT(*) FROM projects;')
			results['repos_ght_total'] = self.ght_cur.fetchone()[0]

		# self.ght_cur.execute('''SELECT COUNT(*) FROM temp_repo_tools_repos tr
		# 	INNER JOIN projects r
		# 	ON 'https://api.github.com/repos/'||tr.owner||'/'||tr.name=r.url
		# 	;''')
		self.ght_cur.execute('''SELECT COUNT(*) FROM temp_repo_tools_repos tr
			INNER JOIN projects r
			ON tr.name=r.name
			INNER JOIN users u
			ON r.owner_id=u.id
			AND tr.owner=u.login
			;''')
		results['repos_common'] = self.ght_cur.fetchone()[0]
		results['repos_missing_ght'] = results['repos_rust'] - results['repos_common']

		self.logger.info('Computed repos result')

		return results

class StarsGHTStats(GHTorrentStats):
	def get(self,**kwargs):
		self.fill_temp_stars()
		self.fill_temp_repos()

		results = OrderedDict()

		self.ght_cur.execute('SELECT COUNT(*) FROM temp_repo_tools_stars;')
		results['stars_rust'] = self.ght_cur.fetchone()[0]

		if self.full_results:
			self.ght_cur.execute('SELECT COUNT(*) FROM watchers;')
			results['stars_ght_total'] = self.ght_cur.fetchone()[0]


		self.ght_cur.execute('''SELECT COUNT(DISTINCT CONCAT(u2.id,'/',r.id)),COUNT(DISTINCT CASE WHEN ts.starrer IS NOT NULL THEN CONCAT(ts.starrer,'//',ts.owner,'/',ts.name) ELSE NULL END) FROM temp_repo_tools_repos tr
			INNER JOIN projects r
			ON tr.name=r.name
			INNER JOIN users u
			ON r.owner_id=u.id
			AND tr.owner=u.login
			INNER JOIN watchers s
			ON s.repo_id=r.id
			INNER JOIN users u2
			ON u2.id=s.user_id
			LEFT OUTER JOIN temp_repo_tools_stars ts
			ON ts.starrer=u2.login AND ts.name=r.name AND ts.owner=u.login
			;''')
		s_ght,s_common1 = self.ght_cur.fetchone()
		results['stars_ght'] = s_ght
		results['stars_missing_rust'] = s_ght - s_common1

		self.ght_cur.execute('''SELECT COUNT(*) FROM temp_repo_tools_stars ts
			INNER JOIN users u
			ON ts.owner=u.login
			INNER JOIN projects r
			ON ts.name=r.name
			AND r.owner_id=u.id
			INNER JOIN users us
			ON ts.starrer=us.login
			INNER JOIN watchers w
			ON w.user_id=us.id AND w.repo_id=r.id
			;''')
		results['stars_common'] = self.ght_cur.fetchone()[0]
		results['stars_missing_ght'] = results['stars_rust'] - results['stars_common']

		self.logger.info('Computed stars result')

		return results

class ForksGHTStats(GHTorrentStats):
	def get(self,**kwargs):
		self.fill_temp_forks()
		self.fill_temp_repos()

		results = OrderedDict()

		self.ght_cur.execute('SELECT COUNT(*) FROM temp_repo_tools_forks;')
		results['forks_rust'] = self.ght_cur.fetchone()[0]

		if self.full_results:
			self.ght_cur.execute('SELECT COUNT(*) FROM forks;')
			results['forks_ght_total'] = self.ght_cur.fetchone()[0]

		self.ght_cur.execute('''SELECT COUNT(DISTINCT CONCAT(f.id,'/',r.id)),COUNT(DISTINCT CASE WHEN tf.forked_owner IS NOT NULL THEN CONCAT(tf.forked_owner,'/',tf.forked_name,'//',tf.forking_owner,'/',tf.forking_name) ELSE NULL END) FROM temp_repo_tools_repos tr
			INNER JOIN projects r
			ON tr.name=r.name
			INNER JOIN users u
			ON r.owner_id=u.id
			AND tr.owner=u.login
			INNER JOIN projects f
			ON f.forked_from=r.id
			INNER JOIN users u2
			ON u2.id=f.owner_id
			LEFT OUTER JOIN temp_repo_tools_forks tf
			ON tf.forked_owner=u.login AND tf.forked_name=r.name AND tf.forking_owner=u2.login AND tf.forking_name=f.name
			;''')
		f_ght,f_common1 = self.ght_cur.fetchone()
		results['forks_ght'] = f_ght
		results['forks_missing_rust'] = f_ght - f_common1

		self.ght_cur.execute('''SELECT COUNT(*) FROM temp_repo_tools_forks tf
			INNER JOIN users u
			ON tf.forking_owner=u.login
			INNER JOIN projects r
			ON tf.forking_name=r.name
			AND r.owner_id=u.id
			INNER JOIN projects r2
			ON r.forked_from=r2.id
			AND tf.forked_name=r2.name
			INNER JOIN users u2
			ON r2.owner_id=u2.id
			AND tf.forked_owner=u2.login
			;''')
		results['forks_common'] = self.ght_cur.fetchone()[0]
		results['forks_missing_ght'] = results['forks_rust'] - results['forks_common']

		self.logger.info('Computed forks result')

		return results

class UserReposGHTStats(GHTorrentStats):
	def get(self,**kwargs):
		self.fill_temp_userrepos()
		self.fill_temp_userrepos_ght()
		self.fill_temp_repos()

		results = OrderedDict()

		self.ght_cur.execute('SELECT COUNT(*) FROM temp_repo_tools_userrepos;')
		results['userrepos_edges_rust'] = self.ght_cur.fetchone()[0]

		if self.full_results:
			self.ght_cur.execute('''SELECT COUNT(*) FROM
		(SELECT DISTINCT project_id,author_id FROM project_commits c) AS sq
			;''')
			results['userrepos_edges_ght_total'] = self.ght_cur.fetchone()[0]

		# self.ght_cur.execute('''SELECT COUNT(*) FROM
		# (SELECT DISTINCT tur.owner,tur.name,tur.login FROM temp_repo_tools_userrepos tur
		# 	INNER JOIN users u
		# 	ON tur.login=u.login
		# 	INNER JOIN commits c
		# 	ON c.author_id=u.id
		# 	INNER JOIN projects r
		# 	ON r.id=c.project_id
		# 	AND r.name=tur.name
		# 	INNER JOIN users u2
		# 	ON u2.id=r.owner_id
		# 	AND u2.login=tur.owner) AS sq
		# 	;''')

		# self.ght_cur.execute('''SELECT COUNT(DISTINCT CONCAT(u2.id,'/',r.id)),
		# 								COUNT(DISTINCT CASE WHEN tur.login IS NOT NULL THEN CONCAT(tur.login,'//',tur.owner,'/',tur.name) ELSE NULL END)
		# 	FROM 
		# 		(SELECT u2.id AS u2id, r.id AS rid,MIN(r.name) AS rname, MIN(r.owner) AS rowner, MIN(u2.login) AS u2login FROM temp_repo_tools_repos tr
		# 		INNER JOIN projects r
		# 		ON tr.name=r.name
		# 		INNER JOIN users u
		# 		ON r.owner_id=u.id
		# 		AND tr.owner=u.login
		# 			INNER JOIN project_commits cr
		# 			ON cr.project_id=r.id
		# 			INNER JOIN commits c
		# 			ON c.id=cr.commit_id
		# 		INNER JOIN users u2
		# 		ON c.author_id=u2.id
		# 		GROUP BY u2.id,r.id) AS sq
		# 	LEFT OUTER JOIN temp_repo_tools_userrepos tur
		# 	ON tur.login=sq.u2login AND tur.owner=sq.rowner AND tur.name=sq.rname
		# 	;''')
		self.ght_cur.execute('''SELECT COUNT(DISTINCT CONCAT(turg.login,'//',turg.owner,'/',turg.name)),
				COUNT(DISTINCT CASE WHEN tur.login IS NOT NULL THEN CONCAT(tur.login,'//',tur.owner,'/',tur.name) ELSE NULL END)
			FROM temp_repo_tools_userrepos_ght turg
			LEFT OUTER JOIN temp_repo_tools_userrepos tur
			ON tur.login=turg.login AND tur.owner=turg.owner AND tur.name=turg.name

			;''')
		ure_ght,ure_common1 = self.ght_cur.fetchone()
		results['userrepos_edges_ght'] = ure_ght
		results['userrepos_edges_missing_rust'] = ure_ght - ure_common1

		# self.ght_cur.execute('''SELECT COUNT(*) FROM
		# (SELECT tur.owner,tur.name,tur.login FROM temp_repo_tools_userrepos tur
		# 	WHERE EXISTS (
		# 		SELECT 1 FROM users u2
		# 		INNER JOIN projects r
		# 		ON u2.login=tur.owner
		# 		AND r.name=tur.name
		# 		AND u2.id=r.owner_id
		# 		INNER JOIN project_commits cr
		# 		ON cr.project_id=r.id
		# 		INNER JOIN commits c
		# 		ON c.id=cr.commit_id
		# 		INNER JOIN users u
		# 		ON c.author_id=u.id
		# 		AND tur.login=u.login
		# 	)
		# 	) AS sq
		# 	;''')
		self.ght_cur.execute('''SELECT COUNT(DISTINCT CONCAT(tur.login,'//',tur.owner,'/',tur.name))
			FROM temp_repo_tools_userrepos tur
			INNER JOIN temp_repo_tools_userrepos_ght turg
			ON tur.login=turg.login AND tur.owner=turg.owner AND tur.name=turg.name

			;''')
		results['userrepos_edges_common'] = self.ght_cur.fetchone()[0]
		results['userrepos_edges_missing_ght'] = results['userrepos_edges_rust'] - results['userrepos_edges_common']

		self.logger.info('Computed userrepos_edges result')

		return results


class GHTorrentGlobalStats(GHTorrentStats):
	def get(self,**kwargs):
		results = OrderedDict()

		for (name,cl) in [
				('user_stats',UserGHTStats),
				('repos_stats',ReposGHTStats),
				('commits_stats',CommitsGHTStats),
				('stars_stats',StarsGHTStats),
				('forks_stats',ForksGHTStats),
				('userrepos_edges',UserReposGHTStats),
				]:

			s = cl(db=self.db,ght_cur=self.ght_cur,ght_conn=self.ght_conn,limit=None)
			s.get_result()
			results[name] = s.results

		return results



