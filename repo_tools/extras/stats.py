import psycopg2
from collections import OrderedDict
import oyaml as yaml
from ..getters import generic_getters

class Stats(generic_getters.Getter):
	'''
	abstract class to be inherited from
	'''

	def format_result(self,**kwargs):
		'''
		formats results in yml format
		'''
		if not hasattr(self,'results'):
			self.get_result(**kwargs)
		return yaml.dump(self.results)

	def print_result(self,**kwargs):
		'''
		prints results in yml format
		'''
		print(self.format_result(**kwargs))

	def save(self,filepath,**kwargs):
		'''
		saves output from format_resuts in a file
		'''
		with open(filepath,'w') as f:
			f.write(self.format_result(**kwargs))

	def get_result(self,db=None,force=False,**kwargs):
		'''
		sets self.results as an ordered dict, potentially nested, by calling .get() if necessary
		'''
		if db is None:
			db = self.db
		if not hasattr(self,'results') or force:
			self.results = self.get(db=db,**kwargs)
		return self.results

	def get(self,db,**kwargs):
		'''
		sets self.results as an ordered dict, potentially nested
		'''
		raise NotImplementedError

class DBStats(Stats):
	'''
	abstract class for DB stats
	'''
	# def __init__(self,db,**kwargs):
	# 	self.db = db
	# 	Stats.__init__(self,**kwargs)

	def get_result(self,db=None,**kwargs):
		'''
		sets self.results as an ordered dict, potentially nested, by calling .get() if necessary
		'''
		if db is None:
			db = self.db
		else:
			raise NotImplementedError('For DBStats and children classes, call to another DB is not implemented/passed down to .get()')
		if db is None:
			raise ValueError('please set a database to query from')
		Stats.get_result(self,db=db,**kwargs)

class PackageStats(DBStats):
	'''
	packages stats
	'''
	def get(self,db,**kwargs):
		results = OrderedDict()
		results['nb_total'] = self.get_nb_packages()
		results['nb_withurl'] = self.get_nb_withurl()
		results['nb_withrepo'] = self.get_nb_withrepo(cloned=False)
		results['nb_withrepo_cloned'] = self.get_nb_withrepo(cloned=True)

		return results


	def get_nb_packages(self):
		self.db.cursor.execute('SELECT COUNT(*) FROM packages;')
		return self.db.cursor.fetchone()[0]

	def get_nb_withurl(self):
		ans = OrderedDict()

		self.db.cursor.execute('SELECT COUNT(url_id) FROM packages;')
		ans['total'] = self.db.cursor.fetchone()[0]

		self.db.cursor.execute('SELECT COUNT(DISTINCT url_id) FROM packages;')
		ans['distinct'] = self.db.cursor.fetchone()[0]

		return ans

	def get_nb_withrepo(self,cloned=False):
		ans = OrderedDict()

		if cloned:
			self.db.cursor.execute('''SELECT COUNT(p.repo_id),'_all' AS sname FROM packages p
										INNER JOIN repositories r
										ON r.id=p.repo_id AND r.cloned
									UNION
									(SELECT COUNT(p.repo_id),s.name AS sname FROM packages p
									INNER JOIN urls u
									ON p.url_id=u.id
									INNER JOIN sources s
									ON u.source_root=s.id
									INNER JOIN repositories r
									ON p.repo_id=r.id AND r.cloned
									GROUP BY s.name
									ORDER BY s.name)
									ORDER BY sname;''')
			ans['total'] = OrderedDict()
			for cnt,s in self.db.cursor.fetchall():
				ans['total'][s] = cnt

			self.db.cursor.execute('''SELECT COUNT(DISTINCT p.repo_id),'_all' AS sname FROM packages p
									UNION
									(SELECT COUNT(DISTINCT p.repo_id),s.name AS sname FROM packages p
									INNER JOIN urls u
									ON p.url_id=u.id
									INNER JOIN sources s
									ON u.source_root=s.id
									INNER JOIN repositories r
									ON p.repo_id=r.id AND r.cloned
									GROUP BY s.name
									ORDER BY s.name)
									ORDER BY sname;''')
			ans['distinct'] = OrderedDict()
			for cnt,s in self.db.cursor.fetchall():
				ans['distinct'][s] = cnt

		else:
			self.db.cursor.execute('''SELECT COUNT(p.repo_id),'_all' AS sname FROM packages p
									UNION
									(SELECT COUNT(p.repo_id),s.name AS sname FROM packages p
									INNER JOIN urls u
									ON p.url_id=u.id
									INNER JOIN sources s
									ON u.source_root=s.id
									GROUP BY s.name
									ORDER BY s.name)
									ORDER BY sname;''')
			ans['total'] = OrderedDict()
			for cnt,s in self.db.cursor.fetchall():
				ans['total'][s] = cnt

			self.db.cursor.execute('''SELECT COUNT(DISTINCT p.repo_id),'_all' AS sname FROM packages p
									UNION
									(SELECT COUNT(DISTINCT p.repo_id),s.name AS sname FROM packages p
									INNER JOIN urls u
									ON p.url_id=u.id
									INNER JOIN sources s
									ON u.source_root=s.id
									GROUP BY s.name
									ORDER BY s.name)
									ORDER BY sname;''')
			ans['distinct'] = OrderedDict()
			for cnt,s in self.db.cursor.fetchall():
				ans['distinct'][s] = cnt

		return ans


class URLStats(DBStats):
	'''
	URLs stats
	'''
	def get(self,db,**kwargs):
		results = OrderedDict()
		results['nb_total'] = self.get_nb_urls()
		results['nb_withrepo'] = self.get_nb_withrepo(cloned=False)
		results['nb_withrepo_cloned'] = self.get_nb_withrepo(cloned=True)

		return results

	def get_nb_urls(self):
		self.db.cursor.execute('SELECT COUNT(DISTINCT COALESCE(cleaned_url,id)) FROM urls;')
		return self.db.cursor.fetchone()[0]

	def get_nb_withrepo(self,cloned=False):
		ans = OrderedDict()

		if cloned:
			self.db.cursor.execute('''SELECT COUNT(u.cleaned_url),'_all' AS sname FROM urls u
									INNER JOIN repositories r
									ON u.id=r.url_id AND r.cloned
									UNION
									(SELECT COUNT(u.cleaned_url),s.name AS sname FROM urls u
									INNER JOIN sources s
									ON u.source_root=s.id
									INNER JOIN repositories r
									ON u.id=r.url_id AND r.cloned
									GROUP BY s.name
									ORDER BY s.name)
									ORDER BY sname;''')
			ans['total'] = OrderedDict()
			for cnt,s in self.db.cursor.fetchall():
				ans['total'][s] = cnt

			self.db.cursor.execute('''SELECT COUNT(DISTINCT u.cleaned_url),'_all' AS sname FROM urls u
									INNER JOIN repositories r
									ON u.id=r.url_id AND r.cloned
									UNION
									(SELECT COUNT(DISTINCT u.cleaned_url),s.name AS sname FROM urls u
									INNER JOIN sources s
									ON u.source_root=s.id
									INNER JOIN repositories r
									ON u.id=r.url_id AND r.cloned
									GROUP BY s.name
									ORDER BY s.name)
									ORDER BY sname;''')

			ans['distinct'] = OrderedDict()
			for cnt,s in self.db.cursor.fetchall():
				ans['distinct'][s] = cnt

		else:
			self.db.cursor.execute('''SELECT COUNT(u.cleaned_url),'_all' AS sname FROM urls u
									INNER JOIN repositories r
									ON u.id=r.url_id
									UNION
									(SELECT COUNT(u.cleaned_url),s.name AS sname FROM urls u
									INNER JOIN sources s
									ON u.source_root=s.id
									INNER JOIN repositories r
									ON u.id=r.url_id
									GROUP BY s.name
									ORDER BY s.name)
									ORDER BY sname;''')
			ans['total'] = OrderedDict()
			for cnt,s in self.db.cursor.fetchall():
				ans['total'][s] = cnt

			self.db.cursor.execute('''SELECT COUNT(DISTINCT u.cleaned_url),'_all' AS sname FROM urls u
									INNER JOIN repositories r
									ON u.id=r.url_id
									UNION
									(SELECT COUNT(DISTINCT u.cleaned_url),s.name AS sname FROM urls u
									INNER JOIN sources s
									ON u.source_root=s.id
									INNER JOIN repositories r
									ON u.id=r.url_id
									GROUP BY s.name
									ORDER BY s.name)
									ORDER BY sname;''')

			ans['distinct'] = OrderedDict()
			for cnt,s in self.db.cursor.fetchall():
				ans['distinct'][s] = cnt


		return ans


class RepoStats(DBStats):
	'''
	repositories stats
	'''
	def get(self,db,**kwargs):
		results = OrderedDict()
		results['nb_total'] = self.get_nb_repos(cloned=False)
		results['nb_cloned'] = self.get_nb_repos(cloned=True)
		results['nb_1dev'] = self.get_nb_1dev()
		results['nb_100dev'] = self.get_nb_100dev()

		return results

	def get_nb_repos(self,cloned=False):
		if cloned:
			self.db.cursor.execute('''SELECT COUNT(*),'_all' AS sname FROM repositories r WHERE r.cloned
				UNION
					(SELECT COUNT(*),s.name AS sname FROM repositories r
					INNER JOIN urls u
					ON u.id=r.url_id AND r.cloned
					INNER JOIN sources s
					ON s.id=u.source_root
					GROUP BY s.name
					ORDER BY s.name)
				ORDER BY sname;''')
		else:
			self.db.cursor.execute('''SELECT COUNT(*),'_all' AS sname FROM repositories r
				UNION
					(SELECT COUNT(*),s.name AS sname FROM repositories r
					INNER JOIN urls u
					ON u.id=r.url_id
					INNER JOIN sources s
					ON s.id=u.source_root
					GROUP BY s.name
					ORDER BY s.name)
				ORDER BY sname;''')
		ans = OrderedDict()
		for cnt,s in self.db.cursor.fetchall():
			ans[s] = cnt
		return ans

	def get_nb_1dev(self):
		self.db.cursor.execute('''
			WITH repos1dev AS (SELECT rr.id,rr.url_id FROM repositories rr
						INNER JOIN commits c
						ON c.repo_id=rr.id
						INNER JOIN identities i
						ON i.id=c.author_id AND NOT i.is_bot
						GROUP BY rr.id,rr.url_id
						HAVING COUNT(DISTINCT i.user_id) =1)
			SELECT COUNT(DISTINCT r.id),'_all' AS sname FROM repos1dev r
				UNION
					(SELECT COUNT(DISTINCT r.id),s.name AS sname FROM repos1dev r
					INNER JOIN urls u
					ON u.id=r.url_id
					INNER JOIN sources s
					ON s.id=u.source_root
					GROUP BY s.name
					ORDER BY s.name)
				ORDER BY sname;''')
		ans = OrderedDict()
		for cnt,s in self.db.cursor.fetchall():
			ans[s] = cnt
		return ans

	def get_nb_100dev(self):
		self.db.cursor.execute('''
			WITH repos1dev AS (SELECT rr.id,rr.url_id FROM repositories rr
						INNER JOIN commits c
						ON c.repo_id=rr.id
						INNER JOIN identities i
						ON i.id=c.author_id AND NOT i.is_bot
						GROUP BY rr.id,rr.url_id
						HAVING COUNT(DISTINCT i.user_id) >=100)
			SELECT COUNT(DISTINCT r.id),'_all' AS sname FROM repos1dev r
				UNION
					(SELECT COUNT(DISTINCT r.id),s.name AS sname FROM repos1dev r
					INNER JOIN urls u
					ON u.id=r.url_id
					INNER JOIN sources s
					ON s.id=u.source_root
					GROUP BY s.name
					ORDER BY s.name)
				ORDER BY sname;''')
		ans = OrderedDict()
		for cnt,s in self.db.cursor.fetchall():
			ans[s] = cnt
		return ans


class CommitsStats(DBStats):
	'''
	commits stats
	'''
	def get(self,db,**kwargs):
		results = OrderedDict()
		results['nb_total'] = OrderedDict()
		results['nb_total']['no_bots'] = self.get_nb_commits(onlybots=False)
		results['nb_total']['only_bots'] = self.get_nb_commits(onlybots=True)
		return results

	def get_nb_commits(self,onlybots=False):
		if onlybots:
			self.db.cursor.execute('''
				SELECT COUNT(*),'_all' AS sname FROM commits c
						INNER JOIN identities i
						ON i.id=c.author_id
						AND i.is_bot
					UNION
						(SELECT COUNT(*),s.name AS sname FROM repositories r
						INNER JOIN urls u
						ON u.id=r.url_id
						INNER JOIN sources s
						ON s.id=u.source_root
						RIGHT OUTER JOIN commits c
						ON r.id=c.repo_id
						INNER JOIN identities i
						ON i.id=c.author_id
						AND i.is_bot
						GROUP BY s.name
						ORDER BY s.name)
					ORDER BY sname;''')
		else:
			self.db.cursor.execute('''
				SELECT COUNT(*),'_all' AS sname FROM commits c
						INNER JOIN identities i
						ON i.id=c.author_id
						AND NOT i.is_bot
					UNION
						(SELECT COUNT(*),s.name AS sname FROM repositories r
						INNER JOIN urls u
						ON u.id=r.url_id
						INNER JOIN sources s
						ON s.id=u.source_root
						RIGHT OUTER JOIN commits c
						ON r.id=c.repo_id
						INNER JOIN identities i
						ON i.id=c.author_id
						AND NOT i.is_bot
						GROUP BY s.name
						ORDER BY s.name)
					ORDER BY sname;''')
		ans = OrderedDict()
		for cnt,s in self.db.cursor.fetchall():
			ans[s] = cnt
		return ans

class IdentitiesStats(DBStats):
	'''
	identities stats
	'''
	def get(self,db,**kwargs):
		results = OrderedDict()
		results['nb_total'] = OrderedDict()
		results['nb_total']['no_bots'] = self.get_nb_identities(onlybots=False)
		results['nb_total']['only_bots'] = self.get_nb_identities(onlybots=True)

		return results

	def get_nb_identities(self,onlybots=False):
		if onlybots:
			self.db.cursor.execute('''
			SELECT COUNT(*),it.name FROM identities i
			INNER JOIN identity_types it
			ON it.id=i.identity_type_id
			AND i.is_bot
			GROUP BY it.name
			''')
		else:
			self.db.cursor.execute('''
			SELECT COUNT(*),it.name FROM identities i
			INNER JOIN identity_types it
			ON it.id=i.identity_type_id
			AND NOT i.is_bot
			GROUP BY it.name
			''')
		ans = OrderedDict()
		for cnt,s in self.db.cursor.fetchall():
			ans[s] = cnt
		return ans

class UsersStats(DBStats):
	'''
	users stats
	'''
	def get(self,db,**kwargs):
		results = OrderedDict()
		results['nb_total'] = OrderedDict()
		results['nb_total']['no_bots'] = self.get_nb_users(onlybots=False)
		results['nb_total']['only_bots'] = self.get_nb_users(onlybots=True)
		results['nb_total']['total'] = self.get_total()
		results['github_gitlab'] = self.get_ghgl_users()
		return results

	def get_nb_users(self,onlybots=False):
		if onlybots:
			self.db.cursor.execute('''
			SELECT COUNT(DISTINCT i.user_id),it.name FROM identities i
			INNER JOIN identity_types it
			ON it.id=i.identity_type_id
			AND i.is_bot
			GROUP BY it.name
			''')
		else:
			self.db.cursor.execute('''
			SELECT COUNT(DISTINCT i.user_id),it.name FROM identities i
			INNER JOIN identity_types it
			ON it.id=i.identity_type_id
			AND NOT i.is_bot
			GROUP BY it.name
			''')
		ans = OrderedDict()
		for cnt,s in self.db.cursor.fetchall():
			ans[s] = cnt
		return ans

	def get_total(self,onlybots=False):
		
		ans = OrderedDict()
		self.db.cursor.execute('''
			SELECT COUNT(*) FROM users u
			;''')
		ans['total'] = self.db.cursor.fetchone()[0]

		self.db.cursor.execute('''
			SELECT (SELECT COUNT(*) FROM users u) - (SELECT COUNT(DISTINCT i.user_id) FROM identities i)
			;''')
		ans['no_identity'] = self.db.cursor.fetchone()[0]


		return ans

	def get_ghgl_users(self):
		ans = OrderedDict()
		self.db.cursor.execute('''
			SELECT COUNT(*) FROM users u
			INNER JOIN identities i
			ON i.user_id = u.id
			INNER JOIN identity_types it
			ON it.id=i.identity_type_id AND it.name='github_login'
			INNER JOIN identities i2
			ON i2.user_id = u.id
			INNER JOIN identity_types it2
			ON it2.id=i2.identity_type_id AND it2.name='gitlab_login'
			;''')
		ans['common'] = self.db.cursor.fetchone()[0]


		self.db.cursor.execute('''
			SELECT COUNT(*) FROM users u
			INNER JOIN identities i
			ON i.user_id = u.id
			INNER JOIN identity_types it
			ON it.id=i.identity_type_id AND it.name='github_login'
			INNER JOIN identities i2
			ON i2.user_id = u.id
			INNER JOIN identity_types it2
			ON it2.id=i2.identity_type_id AND it2.name='gitlab_login'
			AND i.identity=i2.identity
			;''')
		ans['common_samelogin'] = self.db.cursor.fetchone()[0]


		self.db.cursor.execute('''
			SELECT COUNT(*) FROM (
				(SELECT DISTINCT i.user_id FROM identities i
			EXCEPT
				SELECT DISTINCT u.id FROM users u
				INNER JOIN identities i
				ON i.user_id = u.id
				INNER JOIN identity_types it
				ON it.id=i.identity_type_id AND it.name IN ('github_login','gitlab_login')
				)
			) sq
			;''')
		ans['no_gh_no_gl'] = self.db.cursor.fetchone()[0]

		return ans


class GlobalStats(DBStats):
	def get(self,db,**kwargs):
		results = OrderedDict()

		for (name,cl) in [
				('packages',PackageStats),
				('urls',URLStats),
				('repositories',RepoStats),
				('commits',CommitsStats),
				('identities',IdentitiesStats),
				('users',UsersStats),
				]:
			self.logger.info('Computing {}'.format(cl.__name__))
			s = cl(db=db)
			s.get_result()
			results[name] = s.results
			self.logger.info('Computed {}'.format(cl.__name__))

		return results

