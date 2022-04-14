import psycopg2
from collections import OrderedDict
import oyaml as yaml
import networkx as nx

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
		results['nb_1package'] = self.get_single_package()
		results['nb_multipackages'] = self.get_multiplicity_packages()
		results['nb_maxpackages'] = self.get_multiplicity_packages(option='max')
		results['nb_avgpackages'] = self.get_multiplicity_packages(option='avg')

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

	def get_single_package(self):
		self.db.cursor.execute('''
			SELECT COUNT(*),'_all' AS sname FROM
						(SELECT p.repo_id , COUNT(*)
									FROM packages p
									WHERE p.repo_id IS NOT NULL
									GROUP BY p.repo_id
									HAVING COUNT(*)=1) a
			UNION
			(SELECT COUNT(*),b.sname FROM
						(SELECT p.repo_id,s.name AS sname , COUNT(*)
									FROM packages p
									INNER JOIN repositories r ON r.id=p.repo_id
									INNER JOIN sources s
									ON s.id=r.source
									GROUP BY p.repo_id,s.name
									HAVING COUNT(*)=1) b
						GROUP BY sname)
			ORDER BY sname
			;''')
		ans = OrderedDict()
		for cnt,s in self.db.cursor.fetchall():
			ans[s] = cnt
		return ans

	def get_multiplicity_packages(self,option=None):
		if option is None:
			self.db.cursor.execute('''
				SELECT COUNT(*),'_all' AS sname FROM
							(SELECT p.repo_id , COUNT(*)
									FROM packages p
									WHERE p.repo_id IS NOT NULL
									GROUP BY p.repo_id
									HAVING COUNT(*)>1) a
				UNION
				(SELECT COUNT(*),b.sname FROM
							(SELECT p.repo_id,s.name AS sname , COUNT(*)
									FROM packages p
									INNER JOIN repositories r ON r.id=p.repo_id
									INNER JOIN sources s
									ON s.id=r.source
									GROUP BY p.repo_id,s.name
									HAVING COUNT(*)>1) b
						GROUP BY sname)
				ORDER BY sname
				;''')
		elif option == 'max':
			self.db.cursor.execute('''
				SELECT MAX(cnt),'_all' AS sname FROM
							(SELECT p.repo_id , COUNT(*) as cnt
									FROM packages p
									WHERE p.repo_id IS NOT NULL
									GROUP BY p.repo_id
									--HAVING COUNT(*)>1
									) a
				UNION
				(SELECT MAX(cnt),b.sname FROM
							(SELECT p.repo_id,s.name AS sname , COUNT(*) as cnt
									FROM packages p
									INNER JOIN repositories r ON r.id=p.repo_id
									INNER JOIN sources s
									ON s.id=r.source
									GROUP BY p.repo_id,s.name
									--HAVING COUNT(*)>1
									) b
						GROUP BY b.sname)
				ORDER BY sname
				;''')
		elif option == 'avg':
			self.db.cursor.execute('''
				SELECT AVG(cnt),'_all' AS sname FROM
							(SELECT p.repo_id , COUNT(*) as cnt
									FROM packages p
									WHERE p.repo_id IS NOT NULL
									GROUP BY p.repo_id
									--HAVING COUNT(*)>1
									) a
				UNION
				(SELECT AVG(cnt),b.sname FROM
							(SELECT p.repo_id,s.name AS sname , COUNT(*) as cnt
									FROM packages p
									INNER JOIN repositories r ON r.id=p.repo_id
									INNER JOIN sources s
									ON s.id=r.source
									GROUP BY p.repo_id,s.name
									--HAVING COUNT(*)>1
									) b
						GROUP BY sname)
				ORDER BY sname
				;''')
		else:
			raise ValueError('option not implemented: {}'.format(option))
		ans = OrderedDict()
		for cnt,s in self.db.cursor.fetchall():
			if option == 'avg':
				ans[s] = float(cnt)
			else:
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

		results['with_forks'] = OrderedDict()
		results['with_forks']['nb_total'] = OrderedDict()
		results['with_forks']['nb_total']['no_bots'] = self.get_nb_commits_forks(onlybots=False)
		results['with_forks']['nb_total']['only_bots'] = self.get_nb_commits_forks(onlybots=True)
		results['with_forks']['max'] = OrderedDict()
		results['with_forks']['max']['no_bots'] = self.get_nb_commits_forks(onlybots=False,option='max')
		results['with_forks']['max']['only_bots'] = self.get_nb_commits_forks(onlybots=True,option='max')
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

	def get_nb_commits_forks(self,onlybots=False,option=None):
		self.db.cursor.execute('CREATE TEMPORARY TABLE IF NOT EXISTS commit_repos_multiplicity(commit_id BIGINT PRIMARY KEY,fork_count REAL);')
		self.db.cursor.execute('SELECT commit_id FROM commit_repos_multiplicity LIMIT 1;')
		if len(list(self.db.cursor.fetchall())) == 0:
			self.db.cursor.execute('''
				INSERT INTO commit_repos_multiplicity(commit_id,fork_count)
				SELECT commit_id,COUNT(*) FROM commit_repos
				GROUP BY commit_id
				;''')
		if onlybots:
			self.db.cursor.execute('''
				SELECT COUNT(*),'_all' AS sname,MAX(crm.fork_count) FROM commits c
						INNER JOIN identities i
						ON i.id=c.author_id
						AND i.is_bot
						INNER JOIN commit_repos_multiplicity crm
						ON crm.commit_id=c.id
						AND crm.fork_count>1
					UNION
						(SELECT COUNT(*),s.name AS sname,MAX(crm.fork_count) FROM repositories r
						INNER JOIN urls u
						ON u.id=r.url_id
						INNER JOIN sources s
						ON s.id=u.source_root
						RIGHT OUTER JOIN commits c
						ON r.id=c.repo_id
						INNER JOIN identities i
						ON i.id=c.author_id
						AND i.is_bot
						INNER JOIN commit_repos_multiplicity crm
						ON crm.commit_id=c.id
						AND crm.fork_count>1
						GROUP BY s.name
						ORDER BY s.name)
					ORDER BY sname;''')
		else:
			self.db.cursor.execute('''
				SELECT COUNT(*),'_all' AS sname,MAX(crm.fork_count) FROM commits c
						INNER JOIN identities i
						ON i.id=c.author_id
						AND NOT i.is_bot
						INNER JOIN commit_repos_multiplicity crm
						ON crm.commit_id=c.id
						AND crm.fork_count>1
					UNION
						(SELECT COUNT(*),s.name AS sname,MAX(crm.fork_count) FROM repositories r
						INNER JOIN urls u
						ON u.id=r.url_id
						INNER JOIN sources s
						ON s.id=u.source_root
						RIGHT OUTER JOIN commits c
						ON r.id=c.repo_id
						INNER JOIN identities i
						ON i.id=c.author_id
						AND NOT i.is_bot
						INNER JOIN commit_repos_multiplicity crm
						ON crm.commit_id=c.id
						AND crm.fork_count>1
						GROUP BY s.name
						ORDER BY s.name)
					ORDER BY sname;''')
		ans = OrderedDict()
		for cnt,s,m in self.db.cursor.fetchall():
			if option == 'max':
				ans[s] = int(m)
			elif option is None:
				ans[s] = cnt
			else:
				raise ValueError('Option not implemented: {}'.format(option))
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


class DepsStats(DBStats):
	'''
	dependencies stats
	'''
	def get(self,db,**kwargs):
		results = OrderedDict()
		results['packagespace'] = OrderedDict()
		results['packagespace']['nb_links'] = self.get_nb_links(space='p',filtered=False)
		# results['packagespace']['nb_cycles'] =
		# results['packagespace']['nb_in_cycles'] =

		results['packagespace_filtered'] = OrderedDict()
		results['packagespace_filtered']['nb_links'] = self.get_nb_links(space='p',filtered=True)
		# results['packagespace_filtered']['nb_cycles'] =
		# results['packagespace_filtered']['nb_in_cycles'] =

		results['repospace'] = OrderedDict()
		results['repospace']['nb_links'] = self.get_nb_links(space='r',filtered=False)
		# results['repospace']['nb_cycles'] =
		# results['repospace']['nb_in_cycles'] =

		results['repospace_filtered'] = OrderedDict()
		results['repospace_filtered']['nb_links'] = self.get_nb_links(space='r',filtered=True)
		# results['repospace_filtered']['nb_cycles'] =
		# results['repospace_filtered']['nb_in_cycles'] =
		self.get_network()
		self.get_network_filtered()

		return results

	def get_network(self):
		if not hasattr(self,'network_p'):
			self.db.cursor.execute('''
				SELECT DISTINCT pd.depending_on_package,pv.package_id
						FROM package_dependencies pd
						INNER JOIN package_versions pv
						ON pv.id=pd.depending_version
				;''')

			self.network_p = nx.DiGraph()
			self.network_p.add_edges_from(self.db.cursor.fetchall())

		if not hasattr(self,'network_r'):
			self.db.cursor.execute('''
					SELECT DISTINCT p1.repo_id,p2.repo_id
						FROM package_dependencies pd
						INNER JOIN package_versions pv
						ON pv.id=pd.depending_version
						INNER JOIN packages p1
						ON pd.depending_on_package=p1.id
						INNER JOIN packages p2
						ON pv.package_id=p2.id
				;''')

			self.network_r = nx.DiGraph()
			self.network_r.add_edges_from(self.db.cursor.fetchall())

	def get_network_filtered(self):
		if not hasattr(self,'network_p_filtered'):
			self.db.cursor.execute('''
				SELECT DISTINCT pd.depending_on_package,pv.package_id
						FROM package_dependencies pd
						INNER JOIN package_versions pv
						ON pv.id=pd.depending_version
						AND pv.package_id NOT IN (SELECT package_id FROM filtered_deps_package)
				;''')

			self.network_p_filtered = nx.DiGraph()
			self.network_p_filtered.add_edges_from(self.db.cursor.fetchall())

		if not hasattr(self,'network_r_filtered'):
			self.db.cursor.execute('''
					SELECT DISTINCT p1.repo_id AS repo_source_id,p2.repo_id AS repo_dest_id
						FROM package_dependencies pd
						INNER JOIN package_versions pv
						ON pv.id=pd.depending_version
						INNER JOIN packages p1
						ON pd.depending_on_package=p1.id
						INNER JOIN packages p2
						ON pv.package_id=p2.id
						AND pv.package_id NOT IN (SELECT package_id FROM filtered_deps_package)
						AND p1.repo_id NOT IN (SELECT repo_id FROM filtered_deps_repo)
					EXCEPT
						SELECT repo_source_id,repo_dest_id FROM filtered_deps_repoedges

				;''')

			self.network_r_filtered = nx.DiGraph()
			self.network_r_filtered.add_edges_from(self.db.cursor.fetchall())

	def get_nb_links(self,space='r',filtered=True):
		if space == 'p':
			if not filtered:
				self.db.cursor.execute('''
					SELECT COUNT(*) FROM (
						SELECT DISTINCT pd.depending_on_package,pv.package_id
						FROM package_dependencies pd
						INNER JOIN package_versions pv
						ON pv.id=pd.depending_version) a
					;''')
			else:
				self.db.cursor.execute('''
					SELECT COUNT(*) FROM (
							SELECT DISTINCT pd.depending_on_package,pv.package_id
							FROM package_dependencies pd
							INNER JOIN package_versions pv
							ON pv.id=pd.depending_version
							INNER JOIN filtered_deps_package fdp
							ON pd.depending_on_package=fdp.package_id
						) a
					;''')
		elif space =='r':
			if not filtered:
				self.db.cursor.execute('''
					SELECT COUNT(*) FROM (
						SELECT DISTINCT p1.repo_id,p2.repo_id
						FROM package_dependencies pd
						INNER JOIN package_versions pv
						ON pv.id=pd.depending_version
						INNER JOIN packages p1
						ON pd.depending_on_package=p1.id
						INNER JOIN packages p2
						ON pv.package_id=p2.id) a
					;''')
			else:
				self.db.cursor.execute('''
					SELECT COUNT(*) FROM (
						SELECT DISTINCT p1.repo_id,p2.repo_id
						FROM package_dependencies pd
						INNER JOIN package_versions pv
						ON pv.id=pd.depending_version
						INNER JOIN packages p1
						ON pd.depending_on_package=p1.id
						INNER JOIN packages p2
						ON pv.package_id=p2.id
							INNER JOIN filtered_deps_package fdp
							ON pd.depending_on_package=fdp.package_id
					UNION
						SELECT DISTINCT p1.repo_id,p2.repo_id
						FROM package_dependencies pd
						INNER JOIN package_versions pv
						ON pv.id=pd.depending_version
						INNER JOIN packages p1
						ON pd.depending_on_package=p1.id
						INNER JOIN packages p2
						ON pv.package_id=p2.id
							INNER JOIN filtered_deps_repo fdr
							ON p1.repo_id=fdr.repo_id
					UNION
						SELECT DISTINCT p1.repo_id,p2.repo_id
						FROM package_dependencies pd
						INNER JOIN package_versions pv
						ON pv.id=pd.depending_version
						INNER JOIN packages p1
						ON pd.depending_on_package=p1.id
						INNER JOIN packages p2
						ON pv.package_id=p2.id
							INNER JOIN filtered_deps_repoedges fde
							ON p1.repo_id=fde.repo_dest_id
							AND p2.repo_id=fde.repo_source_id
						) a
					;''')
		else:
			raise ValueError('Space should be r or p, provided: {}'.format(space))

		ans = self.db.cursor.fetchone()
		if ans is None or ans[0] is None:
			return 0
		else:
			return ans[0]


class GlobalStats(DBStats):
	def get(self,db,**kwargs):
		results = OrderedDict()

		for (name,cl) in [
				# ('packages',PackageStats),
				# ('urls',URLStats),
				('repositories',RepoStats),
				# ('commits',CommitsStats),
				# ('identities',IdentitiesStats),
				# ('users',UsersStats),
				('dependencies',DepsStats),
				]:
			self.logger.info('Computing {}'.format(cl.__name__))
			s = cl(db=db)
			s.get_result()
			results[name] = s.results
			self.logger.info('Computed {}'.format(cl.__name__))

		return results

