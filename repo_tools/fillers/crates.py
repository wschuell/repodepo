import datetime
import os
import psycopg2

from repo_tools import fillers
from repo_tools.fillers import generic
import repo_tools as rp

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
			package_limit=None,
			force=False,
					**kwargs):
		self.source = source
		self.source_urlroot = source_urlroot
		self.package_limit = package_limit
		self.conninfo = {'database':database,
						'port':port,
						'user':user,
						'password':password,
						'host':host}
		self.force = force
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
			self.package_list = self.get_packages_from_crates(conn=crates_conn)
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
				if ans is not None:
					last_created_at = ans[0]
					if isinstance(last_created_at,str):
						last_created_at = datetime.datetime.strptime(last_created_at,'%Y-%m-%d %H:%M:%S')
					self.package_list = [(i,n,c_at,r) for (i,n,c_at,r) in self.package_list if c_at>last_created_at]
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
				limit_str = ' LIMIT {}'.format(limit)
		else:
			limit_str = ''

		cursor.execute('''
			SELECT id,name,created_at,repository FROM crates {}
			;'''.format(limit_str))

		return cursor.fetchall()


	def apply(self):
		self.fill_packages()
		self.db.connection.commit()
