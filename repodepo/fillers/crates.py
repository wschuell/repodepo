import datetime
import os
import psycopg2

from .. import fillers
from ..fillers import generic

class CratesFiller(generic.PackageFiller):
	"""
	wrapper around generic.PackageFiller for a crates.io database
	"""

	def __init__(self,
			source='crates',
			source_urlroot=None,
			port=54320,
			user='postgres',
			database='crates_db',
			password=None,
			host='localhost',
			only_packages = False,
			package_limit=None,
			package_limit_is_global=False,
			force=False,
			deps_to_delete= [], #[('juju','charmhelpers'),
							#('arraygen','arraygen-docfix'),
							#('expr-parent','expr-child'),],
			page_size=10**4,
					**kwargs):
		self.source = source
		self.source_urlroot = source_urlroot
		self.package_limit = package_limit
		self.package_limit_is_global = package_limit_is_global
		self.conninfo = {'database':database,
						'port':port,
						'user':user,
						'password':password,
						'host':host}
		self.force = force
		self.page_size = page_size
		self.only_packages = only_packages

		if deps_to_delete is None:
			self.deps_to_delete = []
		else:
			self.deps_to_delete = deps_to_delete

		fillers.Filler.__init__(self,**kwargs)

	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder
		data_folder = self.data_folder

		#create folder if needed
		if not os.path.exists(data_folder):
			os.makedirs(data_folder)

		self.db.register_source(source=self.source,source_urlroot=self.source_urlroot)
		if self.source_urlroot is None:
			self.source_url_root = self.db.get_source_info(source=self.source)[1]

		crates_conn = psycopg2.connect(**self.conninfo)
		try:
			self.db.connection.commit()
			self.package_list = list(self.get_packages_from_crates(conn=crates_conn))
			self.db.connection.commit()
			if not self.force:
				if self.db.db_type == 'postgres':
					self.db.cursor.execute('''SELECT MAX(p.created_at) FROM packages p
									INNER JOIN sources s
									ON p.source_id=s.id
									AND s.name=%s
									; ''',(self.source,))
				else:
					self.db.cursor.execute('''SELECT MAX(p.created_at) FROM packages p
									INNER JOIN sources s
									ON p.source_id=s.id
									AND s.name=?
									; ''',(self.source,))
				ans = self.db.cursor.fetchone()
				if ans is not None and ans[0] is not None:
					last_created_at = ans[0]
					if isinstance(last_created_at,str):
						last_created_at = datetime.datetime.strptime(last_created_at,'%Y-%m-%d %H:%M:%S')
					self.package_list = [(i,n,c_at,r) for (i,n,c_at,r) in self.package_list if c_at>last_created_at]


			self.db.connection.commit()
			if not self.only_packages and (self.force or len(self.package_list)>0):
				self.package_version_list = list(self.get_package_versions_from_crates(conn=crates_conn))
				self.db.connection.commit()
				self.package_version_download_list = list(self.get_package_version_downloads_from_crates(conn=crates_conn))
				self.db.connection.commit()
				self.package_deps_list = list(self.get_package_deps_from_crates(conn=crates_conn))
				self.db.connection.commit()
			else:
				self.package_version_list = []
				self.package_version_download_list = []
				self.package_deps_list = []
		finally:
			crates_conn.close()

	def get_packages_from_crates(self,conn,limit=None):
		'''
		From a connection to a crates.io database, output the list of packages as expected by RepoCrawler.add_packages()
		package id, package name, created_at (datetime.datetime),repo_url
		'''
		cursor = conn.cursor()

		if limit is None:
			limit = self.package_limit


		if limit is not None:
			if not isinstance(limit,int):
				raise ValueError('limit should be an integer, given {}'.format(limit))
			else:
				limit_str = ' ORDER BY id LIMIT {}'.format(limit)
		else:
			limit_str = ''

		cursor.execute('''
			SELECT id,name,created_at,COALESCE(repository,homepage,documentation) FROM crates {}
			;'''.format(limit_str))

		return cursor.fetchall()

	def get_package_versions_from_crates(self,conn,limit=None):
		'''
		From a connection to a crates.io database, output the list of package versions as expected
		package id, version name, created_at (datetime.datetime)
		'''
		cursor = conn.cursor()

		if limit is None and self.package_limit_is_global:
			limit = self.package_limit


		if limit is not None:
			if not isinstance(limit,int):
				raise ValueError('limit should be an integer, given {}'.format(limit))
			else:
				limit_str = ' LIMIT {}'.format(limit)
		elif self.package_limit is not None:
			if not isinstance(self.package_limit,int):
				raise ValueError('limit should be an integer, given {}'.format(self.package_limit))
			else:
				limit_str = 'INNER JOIN (SELECT id FROM crates ORDER BY id LIMIT {}) c ON c.id=v.crate_id'.format(self.package_limit)
		else:
			limit_str = ''

		cursor.execute('''
			SELECT crate_id,num,created_at FROM versions v {}
			;'''.format(limit_str))

		return cursor.fetchall()

	def get_package_version_downloads_from_crates(self,conn,limit=None):
		'''
		From a connection to a crates.io database, output the list of package versions as expected
		package id, version name, download_count, created_at (datetime.datetime)
		'''
		cursor = conn.cursor()

		if limit is None and self.package_limit_is_global:
			limit = self.package_limit


		if limit is not None:
			if not isinstance(limit,int):
				raise ValueError('limit should be an integer, given {}'.format(limit))
			else:
				limit_str = ' LIMIT {}'.format(limit)
		elif self.package_limit is not None:
			if not isinstance(self.package_limit,int):
				raise ValueError('limit should be an integer, given {}'.format(self.package_limit))
			else:
				limit_str = 'INNER JOIN (SELECT id FROM crates ORDER BY id LIMIT {}) c ON c.id=v.crate_id'.format(self.package_limit)
		else:
			limit_str = ''

		cursor.execute('''
			SELECT v.crate_id,v.num,vd.downloads,vd.date FROM version_downloads vd
			INNER JOIN versions v
			ON v.id=vd.version_id
			{}
			;'''.format(limit_str))

		return cursor.fetchall()


	def get_package_deps_from_crates(self,conn,limit=None):
		'''
		From a connection to a crates.io database, output the list of package deps as expected
		depending package id (in source), depending version_name, depending_on_package source_id, semver
		'''
		cursor = conn.cursor()

		if limit is None and self.package_limit_is_global:
			limit = self.package_limit


		if limit is not None:
			if not isinstance(limit,int):
				raise ValueError('limit should be an integer, given {}'.format(limit))
			else:
				limit_str = ' LIMIT {}'.format(limit)
		elif self.package_limit is not None:
			if not isinstance(self.package_limit,int):
				raise ValueError('limit should be an integer, given {}'.format(self.package_limit))
			else:
				limit_str = '''INNER JOIN (SELECT id FROM crates ORDER BY id LIMIT {lim}) c
						ON c.id=v.crate_id
						INNER JOIN (SELECT id FROM crates ORDER BY id LIMIT {lim}) c2
						ON c2.id=d.crate_id'''.format(lim=self.package_limit)
		else:
			limit_str = ''

		cursor.execute('''
			SELECT v.crate_id,v.num,d.crate_id,d.req FROM dependencies d
			INNER JOIN versions v
			ON v.id=d.version_id
			AND NOT d.optional AND d.kind=0
			{}
			;'''.format(limit_str))

		return cursor.fetchall()

