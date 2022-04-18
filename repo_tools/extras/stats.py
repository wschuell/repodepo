import psycopg2
from collections import OrderedDict
import oyaml as yaml
import networkx as nx
import datetime

from ..getters import generic_getters,edge_getters,rank_getters

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
	def __init__(self,detailed=False,limit=10**4,**kwargs):
		DBStats.__init__(self,**kwargs)
		self.limit = limit
		self.detailed = detailed

	def get(self,db,**kwargs):
		self.set_network()
		self.set_network_filtered()
		self.set_network_timestamp()
		self.set_network_filtered_timestamp()

		results = OrderedDict()
		results['packagespace'] = OrderedDict()
		results['packagespace']['nb_links'] = self.get_nb_links(space='p',filtered=False)
		results['packagespace']['cycles'] = self.get_cycles(space='p',filtered=False,detailed=self.detailed,network=self.network_p)

		results['packagespace_filtered'] = OrderedDict()
		results['packagespace_filtered']['nb_links_filtered'] = self.get_nb_links(space='p',filtered=True)
		results['packagespace_filtered']['cycles'] = self.get_cycles(space='p',filtered=True,detailed=self.detailed,network=self.network_p_filtered)

		# results['repospace'] = OrderedDict()
		# results['repospace']['nb_links'] = self.get_nb_links(space='r',filtered=False)
		# results['repospace']['cycles'] = self.get_cycles(space='r',filtered=False,detailed=self.detailed,network=self.network_r_timestamp)

		results['repospace_filtered'] = OrderedDict()
		results['repospace_filtered']['nb_links_filtered'] = self.get_nb_links(space='r',filtered=True)
		results['repospace_filtered']['cycles'] = self.get_cycles(space='r',filtered=True,detailed=self.detailed,network=self.network_r_filtered_timestamp)


		return results

	def set_network(self):
		if not hasattr(self,'network_p'):
			# self.db.cursor.execute('''
			# 	SELECT DISTINCT pd.depending_on_package,pv.package_id
			# 			FROM package_dependencies pd
			# 			INNER JOIN package_versions pv
			# 			ON pv.id=pd.depending_version
			# 			AND pd.depending_on_package != pv.package_id
			# 	;''')
			self.db.cursor.execute('''
				SELECT DISTINCT
					do_package_id,package_id
				FROM
					(SELECT p.id AS package_id,p_do.id AS do_package_id,pv.id AS version_id
						FROM package_dependencies pd
						INNER JOIN package_versions pv
						ON pd.depending_version =pv.id
						-- AND pv.created_at <= ref_time
						INNER JOIN packages p
						ON pv.package_id=p.id AND p.repo_id IS NOT NULL
						INNER JOIN packages p_do
						ON pd.depending_on_package=p_do.id AND p_do.repo_id IS NOT NULL
						AND p_do.repo_id != p.repo_id
						--AND (pv.package_id NOT IN (SELECT package_id FROM filtered_deps_package))
						--AND (p_do.repo_id NOT IN (SELECT repo_id FROM filtered_deps_repo))
					) AS dep_q
				INNER JOIN (
					SELECT DISTINCT FIRST_VALUE(id) OVER (PARTITION BY package_id ORDER BY created_at DESC,version_str DESC) AS last_v_id
					FROM package_versions pv
					-- WHERE pv.created_at <= :ref_time
					) AS lastv_q
				ON dep_q.version_id=lastv_q.last_v_id
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
					AND p1.repo_id!=p2.repo_id
				;''')

			self.network_r = nx.DiGraph()
			self.network_r.add_edges_from(self.db.cursor.fetchall())

	def set_network_filtered(self):
		if not hasattr(self,'network_p_filtered'):
			# self.db.cursor.execute('''
			# 	SELECT DISTINCT pd.depending_on_package,pv.package_id
			# 			FROM package_dependencies pd
			# 			INNER JOIN package_versions pv
			# 			ON pv.id=pd.depending_version
			# 			AND pv.package_id NOT IN (SELECT package_id FROM filtered_deps_package)
			# 			AND pd.depending_on_package != pv.package_id
			# 	;''')

			self.db.cursor.execute('''
				SELECT DISTINCT
					do_package_id,package_id
				FROM
					(SELECT p.id AS package_id,p_do.id AS do_package_id,pv.id AS version_id
						FROM package_dependencies pd
						INNER JOIN package_versions pv
						ON pd.depending_version =pv.id
						-- AND pv.created_at <= ref_time
						INNER JOIN packages p
						ON pv.package_id=p.id AND p.repo_id IS NOT NULL
						INNER JOIN packages p_do
						ON pd.depending_on_package=p_do.id AND p_do.repo_id IS NOT NULL
						AND p_do.repo_id != p.repo_id
						AND (pv.package_id NOT IN (SELECT package_id FROM filtered_deps_package))
						AND (p_do.repo_id NOT IN (SELECT repo_id FROM filtered_deps_repo))
					) AS dep_q
				INNER JOIN (
					SELECT DISTINCT FIRST_VALUE(id) OVER (PARTITION BY package_id ORDER BY created_at DESC,version_str DESC) AS last_v_id
					FROM package_versions pv
					-- WHERE pv.created_at <= :ref_time
					) AS lastv_q
				ON dep_q.version_id=lastv_q.last_v_id
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
					AND p1.repo_id!=p2.repo_id
					EXCEPT
						SELECT repo_source_id,repo_dest_id FROM filtered_deps_repoedges

				;''')

			self.network_r_filtered = nx.DiGraph()
			self.network_r_filtered.add_edges_from(self.db.cursor.fetchall())

	def set_network_timestamp(self,timestamp=datetime.datetime.now()):
		if not hasattr(self,'network_r_timestamp'):
			mat = edge_getters.RepoToRepoDeps(db=self.db,ref_time=timestamp,filter_deps=False).get_result()
			self.network_r_timestamp = nx.convert_matrix.from_scipy_sparse_matrix(mat,create_using=nx.DiGraph)


	def set_network_filtered_timestamp(self,timestamp=datetime.datetime.now()):
		if not hasattr(self,'network_r_filtered_timestamp'):
			mat = edge_getters.RepoToRepoDeps(db=self.db,ref_time=timestamp,filter_deps=True).get_result()
			self.network_r_filtered_timestamp = nx.convert_matrix.from_scipy_sparse_matrix(mat,create_using=nx.DiGraph)



	def get_cycles(self,network,filtered=True,space='r',detailed=None):
		G = network

		scyc = nx.simple_cycles(G)
		ans = OrderedDict()
		ans['cycle_detection_stopped'] = False
		ans['nb_cycles'] = 0
		ans['nb_2cycles'] = 0
		ans['maxlen_cycle'] = 0
		elts = dict()
		links = dict()
		rk_direct,rk_indirect,rk_names = rank_getters.RepoRankNameGetter(db=self.db).get_result()

		self.db.cursor.execute('SELECT p.id,s.name,p.name FROM packages p INNER JOIN sources s ON s.id=p.source_id;')

		package_names = {int(p_id):'/'.join([s,pname]) for p_id,s,pname in self.db.cursor.fetchall()}

		def get_name(rk):
			# return rk_names[rk_indirect[int(rk)]]
			if space == 'r':
				return str(rk_names[int(rk)])
			else:
				return str(package_names[int(rk)]) # rk=p_id

		for i,c in enumerate(scyc):
			if self.limit is not None and i >= self.limit:
				self.logger.warning('Stopping cycle detection after {} cycles'.format(i))
				ans['cycle_detection_stopped'] = True
				break
			ans['nb_cycles'] += 1 
			if len(c)==2:
				ans['nb_2cycles'] += 1 

			ans['maxlen_cycle'] = max([len(c),ans['maxlen_cycle']])
			for cc in c:
				if cc in elts.keys():
					elts[cc] += 1
				else:
					elts[cc] = 1
			previous = c[-1]
			for cc in c:
				lk = (previous,cc)
				if lk in links.keys():
					links[lk] += 1
				else:
					links[lk] = 1
				previous = cc
		ans['total_elements_involved'] = len(elts)
		ans['total_links_involved'] = len(links)
		if detailed:
			# ans['elts_involved'] = sorted([{int(e):v} for e,v in elts.items()],key=lambda x: -list(x.values())[0])[:10]
			ans['elements_involved'] = sorted([{get_name(e):v} for e,v in elts.items()],key=lambda x: -list(x.values())[0])[:10]
			links_involved = sorted([{(get_name(e[0]),get_name(e[1])):v} for e,v in links.items()],key=lambda x: -list(x.values())[0])[:10]
			ans['links_involved'] =  [{str(dk):dv for dk,dv in d.items()} for d in links_involved]
			if space == 'r':
				ans['packagespace_links'] = OrderedDict()
				main_lk_l = [list(lk.keys())[0] for lk in links_involved]
				for main_lk in main_lk_l[:5]:
					source_s = main_lk[0].split('/')[0]
					repo_s = '/'.join(main_lk[0].split('/')[1:])
					source_d = main_lk[1].split('/')[0]
					repo_d = '/'.join(main_lk[1].split('/')[1:])
					ans['packagespace_links'][str(main_lk)] = self.get_packagespace_edges(source_s=source_s,repo_s=repo_s,source_d=source_d,repo_d=repo_d)
		return ans


	def get_packagespace_edges(self,source_s,repo_s,source_d,repo_d):
		'''
		Translating a dependency repo_source->repo_dest to the list of underlying package deps 
		'''
		repo_owner_s = repo_s.split('/')[0]
		repo_name_s = '/'.join(repo_s.split('/')[1:])
		repo_owner_d = repo_d.split('/')[0]
		repo_name_d = '/'.join(repo_d.split('/')[1:])
		if self.db.db_type == 'postgres':
			self.db.cursor.execute('''
				SELECT DISTINCT CONCAT(sps.name,'/',ps.name),CONCAT(spd.name,'/',pd.name)
				FROM repositories rs
				INNER JOIN sources ss
				ON rs.owner=%(rowner_s)s AND rs.name=%(rname_s)s
				AND rs.source=ss.id
				AND ss.name=%(source_s)s
				INNER JOIN packages ps
				ON ps.repo_id=rs.id
				INNER JOIN sources sps
				ON sps.id=ps.source_id
				INNER JOIN package_versions psv
				ON psv.package_id =ps.id
				INNER JOIN package_dependencies pdd
				ON pdd.depending_version =psv.id
				INNER JOIN packages pd 
				ON pd.id=pdd.depending_on_package 
				INNER JOIN sources spd
				ON spd.id=pd.source_id
				INNER JOIN repositories rd 
				ON rd.id=pd.repo_id
				AND rd.owner=%(rowner_d)s AND rd.name=%(rname_d)s
				INNER JOIN sources sd
				ON sd.id=rd.source AND sd.name=%(source_d)s
					;''',{'source_s':source_s,'rowner_s':repo_owner_s,'rname_s':repo_name_s,
							'source_d':source_d,'rowner_d':repo_owner_d,'rname_d':repo_name_d})
		else:

			self.db.cursor.execute('''
				SELECT DISTINCT sps.name || '/' || ps.name,spd.name || '/' || pd.name
				FROM repositories rs
				INNER JOIN sources ss
				ON rs.owner=:rowner_s AND rs.name=:rname_s
				AND rs.source=ss.id
				AND ss.name=:source_s
				INNER JOIN packages ps
				ON ps.repo_id=rs.id
				INNER JOIN sources sps
				ON sps.id=ps.source_id
				INNER JOIN package_versions psv
				ON psv.package_id =ps.id
				INNER JOIN package_dependencies pdd
				ON pdd.depending_version =psv.id
				INNER JOIN packages pd 
				ON pd.id=pdd.depending_on_package 
				INNER JOIN sources spd
				ON spd.id=pd.source_id
				INNER JOIN repositories rd 
				ON rd.id=pd.repo_id
				AND rd.owner=:rowner_d AND rd.name=:rname_d
				INNER JOIN sources sd
				ON sd.id=rd.source AND sd.name=:source_d
					;''',{'source_s':source_s,'rowner_s':repo_owner_s,'rname_s':repo_name_s,
							'source_d':source_d,'rowner_d':repo_owner_d,'rname_d':repo_name_d})

		return [str(e) for e in self.db.cursor.fetchall()]


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

		for (name,cl,kwargs_cl) in [
				# ('packages',PackageStats,dict()),
				# ('urls',URLStats,dict()),
				# ('repositories',RepoStats,dict()),
				# ('commits',CommitsStats,dict()),
				# ('identities',IdentitiesStats,dict()),
				# ('users',UsersStats,dict()),
				('dependencies',DepsStats,dict(detailed=True)),
				]:
			self.logger.info('Computing {}'.format(cl.__name__))
			s = cl(db=db,**kwargs_cl)
			s.get_result()
			results[name] = s.results
			self.logger.info('Computed {}'.format(cl.__name__))

		return results

