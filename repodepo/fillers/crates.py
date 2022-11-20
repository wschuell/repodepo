import datetime
import os
import psycopg2
import shutil
import copy
import time
import glob

from .. import fillers
from ..fillers import generic
from ..extras import check_sqlname_safe

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
			regen_crates_db=False,
			min_regen_date=None,
			crates_folder=None,
			force_pastdl=False,
			fallback_db='postgres',
			packages_done=False,
			versions_done=False,
			downloads_done=False,
			deps_done=False,
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

		self.regen_crates_db = regen_crates_db
		self.min_regen_date = min_regen_date
		self.fallback_db = fallback_db
		self.crates_folder = crates_folder
		self.force_pastdl = force_pastdl

		self.packages_done = packages_done
		self.versions_done = versions_done
		self.downloads_done = downloads_done
		self.deps_done = deps_done

		if deps_to_delete is None:
			self.deps_to_delete = []
		else:
			self.deps_to_delete = deps_to_delete

		fillers.Filler.__init__(self,**kwargs)

	def regenerate_crates_db(self,min_regen_date=None,force_pastdl=None):
		if force_pastdl is None:
			force_pastdl = self.force_pastdl
		if min_regen_date is None:
			min_regen_date = self.min_regen_date
		if self.crates_folder is None:
			self.crates_folder = os.path.join(self.data_folder,'crates')
		if not os.path.exists(self.crates_folder):
			os.makedirs(self.crates_folder)

		# Try to connect, create if not exists
		try:
			crates_conn = psycopg2.connect(**self.conninfo)
		except:
			fallback_conninfo = copy.deepcopy(self.conninfo)
			fallback_conninfo['database'] = self.fallback_db
			try:
				temp_crates_conn = psycopg2.connect(**fallback_conninfo)
			except:
				self.logger.info(f'Could not connect to fallback database: {self.fallback_db}')
				raise
			temp_cur = temp_crates_conn.cursor()
			check_sqlname_safe(self.conninfo['database'])
			try:
				temp_cur.execute(f'''CREATE DATABASE {self.conninfo['database']};''')
				new_db = True
			except:
				self.logger.info(f'Could not create database from fallback database: {self.fallback_db}')
				raise
			temp_crates_conn.commit()
			temp_crates_conn.close()

			crates_conn = psycopg2.connect(**self.conninfo)
		crates_cur = crates_conn.cursor()

		crates_cur.execute('''SELECT EXISTS (
    					SELECT FROM 
						        pg_tables
						    WHERE 
						        schemaname = 'public' AND 
						        tablename  = 'crates'
						    		);''')
		if crates_cur.fetchone()[0]:
			crates_cur.execute('SELECT MAX(created_at) FROM crates;')
			max_date = crates_cur.fetchone()[0]
		else:
			max_date = None
		
		t_str = datetime.datetime.now().strftime('%Y%m%d')

		only_pastdl = False
		if max_date is not None and (min_regen_date is None or max_date >= min_regen_date):
			if not force_pastdl:
				crates_conn.close()
				return
			else:
				only_pastdl = True
		elif max_date is not None:
			oldmax_str = max_date.strftime('%Y_%m_%d')
			check_sqlname_safe(oldmax_str)
			crates_cur.execute(f'''ALTER SCHEMA public RENAME TO "{oldmax_str}";''')
			crates_cur.execute('''CREATE SCHEMA IF NOT EXISTS public;''')
			crates_conn.commit()
			crates_conn.close()
			del crates_cur
			del crates_conn
			crates_conn = psycopg2.connect(**self.conninfo)
			crates_cur = crates_conn.cursor()
		pastdl_folder = os.path.join(self.crates_folder,'past_version_downloads')
		if not os.path.exists(pastdl_folder):
			os.makedirs(pastdl_folder)
		if not only_pastdl:
			self.download(url='https://static.crates.io/db-dump.tar.gz',destination=os.path.join(self.crates_folder,f'dump_{t_str}.tar.gz'))
			self.untar(orig_file=os.path.join(self.crates_folder,datetime.datetime.now().strftime(f'dump_{t_str}.tar.gz')),destination=os.path.join(self.crates_folder,f'dump_{t_str}'))

			subfolder = os.path.basename(glob.glob(os.path.join(self.crates_folder,f'dump_{t_str}','*'))[0])
			source_file = os.path.join(self.crates_folder,f'dump_{t_str}',subfolder,'data','version_downloads.csv')
			dest_file = os.path.join(pastdl_folder,f'version_downloads_{t_str}.csv')
			if not os.path.exists(dest_file):
				shutil.copyfile(src=source_file,dst=dest_file)
			with open(os.path.join(self.crates_folder,f'dump_{t_str}',subfolder,'schema.sql'),'r') as f:
				schema_script = f.read()
				ifnot_str = ' IF NOT EXISTS'
				for create_str in ('CREATE SEQUENCE','CREATE INDEX','CREATE UNIQUE INDEX','CREATE MATERIALIZED VIEW'):
					schema_script = create_str.join([(c if c.startswith(ifnot_str) or i==0 else ifnot_str+c) for i,c in enumerate(schema_script.split(create_str))])
				
				for obj_str in ('TYPE','FUNCTION','TRIGGER','TABLE'):
					check_sqlname_safe(obj_str)
					create_str = f'CREATE {obj_str} '
					new_schema_script = ''
					for i,c in enumerate(schema_script.split(create_str)):
						if i==0:
							new_schema_script += c
						else:
							typename = c.split(' ')[0].split('(')[0]#.split('.')[-1]
							
							if obj_str == 'TRIGGER':
								tablename = c.split('ON ')[1].split(' ')[0]
								new_schema_script += f'DROP {obj_str} IF EXISTS {typename} ON {tablename} CASCADE;\n'
							else:
								new_schema_script += f'DROP {obj_str} IF EXISTS {typename} CASCADE;\n'
							new_schema_script += create_str
							new_schema_script += c
					schema_script = new_schema_script


			with open(os.path.join(self.crates_folder,f'dump_{t_str}',subfolder,'import.sql'),'r') as f:
				import_script = f.read()
		
			start = 'CREATE EXTENSION IF NOT EXISTS '
			end = ' WITH SCHEMA public;'
			extensions = [l.replace(start,'').replace(end,'') for l in schema_script.split('\n') if l.startswith(start) and l.endswith(end)]
			for e in extensions:
				check_sqlname_safe(e)
			
				cmd1 = f'''CREATE EXTENSION IF NOT EXISTS {e} WITH SCHEMA public;'''
				self.logger.info(cmd1)
				crates_cur.execute(cmd1)

				cmd2 = f'''ALTER EXTENSION {e} SET SCHEMA public;'''
				self.logger.info(cmd2)
				crates_cur.execute(cmd2)

			crates_cur.execute(schema_script.replace('CREATE SCHEMA heroku_ext;',''))
			crates_conn.commit()
			crates_conn.close()
			del crates_cur
			del crates_conn
			time.sleep(5)
			crates_conn = psycopg2.connect(**self.conninfo)
			crates_cur = crates_conn.cursor()

			self.exec_script_copyfrom(cur=crates_cur,script=import_script,folder=os.path.join(self.crates_folder,f'dump_{t_str}',subfolder),oneline_cmd=True)
		
		pastdl_files = glob.glob(os.path.join(pastdl_folder,'version_downloads*.csv'))
		for filepath in sorted(pastdl_files):
			filename = os.path.basename(filepath)
			with open(filepath,'r') as f:
				header = f.readline()[:-1] # removing \n char
			for c in header.split(','):
				check_sqlname_safe(c)
			col_str = f'''("{'","'.join(header.split(','))}")'''
			pastdl_script = f'''
				BEGIN;
				CREATE TEMPORARY TABLE temp_v_dl(LIKE version_downloads INCLUDING DEFAULTS) ON COMMIT DROP;
				\\copy temp_v_dl {col_str} FROM '{filename}' WITH CSV HEADER;
				INSERT INTO version_downloads (version_id,downloads,date)
				SELECT t.version_id,t.downloads,t.date FROM temp_v_dl t
				INNER JOIN versions v
				ON v.id=t.version_id
				ON CONFLICT DO NOTHING;
				COMMIT;
				'''
			self.exec_script_copyfrom(cur=crates_cur,script=pastdl_script,folder=pastdl_folder,oneline_cmd=False)

		crates_conn.commit()
		crates_conn.close()

	def exec_script_copyfrom(self,cur,script,folder='.',oneline_cmd=False):
		# Assuming each command is a one liner! Because \copy statements do not end with a ; in import.sql
		cmd_list = [l for l in script.split('\n') if not l.replace(' ','').replace('\t','').startswith('--') and not l=='']
		if not oneline_cmd:
			cmd_list = [c for c in '\n'.join(cmd_list).split(';') if c.replace(' ','').replace('\t','').replace('\n','')!='']
		
		for cmd in cmd_list:
			if not cmd.lower().replace(' ','').replace('\t','').replace('\n','').startswith('\\copy'):
				cur.execute(cmd+';')
			else:
				if '\\copy' in cmd:
					copy_string = '\\copy'
				else:
					copy_string = '\\COPY'
				if ' from ' in cmd:
					from_string = 'from'
				else:
					from_string = 'FROM'
				table = [s for s in cmd.split(copy_string)[1].split(' ') if s!=''][0].replace('"','')
				check_sqlname_safe(table)
				columns = tuple([cl.replace('"','').replace(' ','') for cl in cmd.split('(')[1].split(')')[0].split(',')])
				for cl in columns:
					check_sqlname_safe(cl)
				col_string = f'''({','.join(columns)})'''
				filepath = [s for s in cmd.split(from_string)[1].split(' ') if s!=''][0].replace("'",'')
				self.logger.info(f'Copying {table} from {filepath}')
				with open(os.path.join(folder,filepath),'r') as f:
					#f.readline() # discard header
					#cur.copy_expert(f'''COPY {table} {col_string} FROM STDIN WITH (FORMAT CSV)''', f)
					copy_cmd = cmd.replace(copy_string,'COPY').replace(f"'{filepath}'",'STDIN')
					#self.logger.info(copy_cmd)
					cur.copy_expert(copy_cmd, f)
				self.logger.info(f'Finished copying {table} from {filepath}')

	def apply(self):
		generic.PackageFiller.apply(self)
		self.db.cursor.execute('''INSERT INTO full_updates(update_type) SELECT 'crates';''')
		self.db.connection.commit()


	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder
		data_folder = self.data_folder


		self.db.cursor.execute('''SELECT COUNT(*) FROM full_updates WHERE update_type='crates';''')
		crates_fullupdates = self.db.cursor.fetchone()[0]
		if  crates_fullupdates > 0 and not self.force:
			if self.min_regen_date is not None:
				self.db.cursor.execute('''SELECT MAX(created_at) FROM packages;''')
				max_package = self.db.cursor.fetchone()[0]
				date_check = (max_package is not None) and (self.min_regen_date <= max_package)
			else:
				date_check = True
			if date_check:
				self.done = True
				return
		elif not self.force:
			if self.db.db_type == 'postgres':
				self.db.cursor.execute('''SELECT 1 FROM packages p 
										INNER JOIN sources s ON s.id=p.source_id AND s.name=%(source)s 
										LIMIT 1;''',{'source':self.source})
			else:
				self.db.cursor.execute('''SELECT 1 FROM packages p 
										INNER JOIN sources s ON s.id=p.source_id AND s.name=:source 
										LIMIT 1;''',{'source':self.source})

			if self.db.cursor.fetchone() is not None:
				self.packages_done = True

			if self.db.db_type == 'postgres':
				self.db.cursor.execute('''SELECT 1 FROM sources s 
											INNER JOIN packages p ON s.id=p.source_id AND s.name=%(source)s 
											INNER JOIN package_versions pv ON pv.package_id=p.id
											LIMIT 1;''',{'source':self.source})
			else:
				self.db.cursor.execute('''SELECT 1 FROM sources s 
											INNER JOIN packages p ON s.id=p.source_id AND s.name=:source
											INNER JOIN package_versions pv ON pv.package_id=p.id
											LIMIT 1;''',{'source':self.source})

			if self.db.cursor.fetchone() is not None:
				self.versions_done = True

			if self.db.db_type == 'postgres':
				self.db.cursor.execute('''SELECT 1 FROM sources s 
											INNER JOIN packages p ON s.id=p.source_id AND s.name=%(source)s 
											INNER JOIN package_versions pv ON pv.package_id=p.id
											INNER JOIN package_version_downloads pvd
											ON pvd.package_version=pv.id
											LIMIT 1;''',{'source':self.source})
			else:
				self.db.cursor.execute('''SELECT 1 FROM sources s 
											INNER JOIN packages p ON s.id=p.source_id AND s.name=:source
											INNER JOIN package_versions pv ON pv.package_id=p.id
											INNER JOIN package_version_downloads pvd
											ON pvd.package_version=pv.id
											LIMIT 1;''',{'source':self.source})

			if self.db.cursor.fetchone() is not None:
				self.downloads_done = True

			if self.db.db_type == 'postgres':
				self.db.cursor.execute('''SELECT 1 FROM sources s 
											INNER JOIN packages p ON s.id=p.source_id AND s.name=%(source)s
											INNER JOIN package_dependencies pd
											ON pd.depending_on_package=p.id
											LIMIT 1;''',{'source':self.source})
			else:
				self.db.cursor.execute('''SELECT 1 FROM sources s 
											INNER JOIN packages p ON s.id=p.source_id AND s.name=:source
											INNER JOIN package_dependencies pd
											ON pd.depending_on_package=p.id
											LIMIT 1;''',{'source':self.source})

			if self.db.cursor.fetchone() is not None:
				self.deps_done = True


		#create folder if needed
		if not os.path.exists(data_folder):
			os.makedirs(data_folder)

		if self.regen_crates_db:
			self.regenerate_crates_db()

		self.db.register_source(source=self.source,source_urlroot=self.source_urlroot)
		if self.source_urlroot is None:
			self.source_url_root = self.db.get_source_info(source=self.source)[1]

		crates_conn = psycopg2.connect(**self.conninfo)
		try:
			self.db.connection.commit()
			if self.packages_done:
				self.package_list = []
			else:
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

			self.package_version_list = []
			self.package_version_download_list = []
			self.package_deps_list = []
			
			if not self.only_packages: # and (self.force or len(self.package_list)>0):
				if not self.versions_done:
					self.package_version_list = list(self.get_package_versions_from_crates(conn=crates_conn))
					self.db.connection.commit()
				if not self.downloads_done:
					self.package_version_download_list = self.get_package_version_downloads_from_crates(conn=crates_conn)
					# self.package_version_download_list = list(self.get_package_version_downloads_from_crates(conn=crates_conn))
					self.db.connection.commit()
				if not self.deps_done:
					self.package_deps_list = list(self.get_package_deps_from_crates(conn=crates_conn))
					self.db.connection.commit()
			
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
			SELECT id,name,created_at,COALESCE(repository,homepage,documentation),NULL FROM crates {}
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

