import os
import hashlib
import csv
import copy
import pygit2
import json
import shutil
import datetime
import subprocess

from psycopg2 import extras

from repo_tools import fillers
import repo_tools as rp


class PackagesDepsFilter(fillers.Filler):
	'''
	fills from a string list
	'''
	default_source = 'crates'
	def __init__(self,input_list=None,input_file=None,reason='filter from list',source=None,**kwargs):
		if source is None:
			self.info_source = self.default_source
		else:
			self.info_source = source
		if input_list is not None:
			self.input_list = input_list
		elif input_file is not None:
			filepath = os.path.join(self.data_folder,self.bot_file)

			with open(filepath,"rb") as f:
				filehash = hashlib.sha256(f.read()).hexdigest()
				self.source = '{}_{}'.format(self.input_file,filehash)
				self.db.register_source(source=self.source)

			with open(filepath,'r') as f:
				self.input_list = f.read().split('\n')
		else:
			raise ValueError('input_list and input_file cannot both be None')
		self.reason = reason
		fillers.Filler.__init__(self,**kwargs)

	def apply(self):
		self.fill_filters()
		self.db.connection.commit()

	def fill_filters(self):
		if self.db.db_type == 'postgres':
			extras.execute_batch(self.db.cursor,'''
				INSERT INTO filtered_deps_package(package_id,reason)
				VALUES((SELECT id FROM packages WHERE name=%(pname)s AND source=(SELECT id FROM sources WHERE name=%(psource)s)),%(reason)s)
				 ON CONFLICT DO NOTHING;
				''',({'pname':pname,'reason':self.reason,'psource':self.info_source} for pname in self.input_list))
		else:
			self.db.cursor.executemany('''
				INSERT OR IGNORE INTO filtered_deps_package(package_id,reason)
				VALUES((SELECT id FROM packages WHERE name=:pname AND source=(SELECT id FROM sources WHERE name=:psource) ),:reason)
				''',({'pname':pname,'reason':self.reason,'psource':self.info_source} for pname in self.input_list))

class RepoDepsFilter(PackagesDepsFilter):

	default_source = 'GitHub'
	def fill_filters(self):
		if self.db.db_type == 'postgres':
			extras.execute_batch(self.db.cursor,'''
				INSERT INTO filtered_deps_repo(repo_id,reason)
				VALUES((SELECT id FROM repositories WHERE name=%(rname)s AND owner=%(rowner)s AND source=(SELECT id FROM sources WHERE name=%(psource)s)),%(reason)s)
				 ON CONFLICT DO NOTHING;
				 ''',({'rname':rname,'rowner':rowner,'reason':self.reason,'psource':self.info_source} for rowner,rname in [i.split('/') for i in self.input_list]))
		else:
			self.db.cursor.executemany('''
				INSERT OR IGNORE INTO filtered_deps_repo(repo_id,reason)
				VALUES((SELECT id FROM repositories WHERE name=:rname AND owner=:rowner AND source=(SELECT id FROM sources WHERE name=:psource) ),:reason)
				''',({'rname':rname,'rowner':rowner,'reason':self.reason,'psource':self.info_source} for rowner,rname in [i.split('/') for i in self.input_list]))


class RepoEdgesDepsFilter(PackagesDepsFilter):

	default_source = 'GitHub'
	def fill_filters(self):
		if self.db.db_type == 'postgres':
			extras.execute_batch(self.db.cursor,'''
				INSERT INTO filtered_deps_repoedges(repo_source_id,repo_dest_id,reason)
				VALUES((SELECT id FROM repositories WHERE name=%(rname1)s AND owner=%(rowner1)s
					AND source=(SELECT id FROM sources WHERE name=%(psource1)s)),
				(SELECT id FROM repositories WHERE name=%(rname2)s AND owner=%(rowner2)s
					AND source=(SELECT id FROM sources WHERE name=%(psource2)s))
				,%(reason)s) ON CONFLICT DO NOTHING;
				''',({'rname1':rname1,'rowner1':rowner1,'rname2':rname2,'rowner2':rowner2,'reason':self.reason,'psource1':psource1,'psource2':psource2} for psource1,rowner1,rname1,psource2,rowner2,rname2 in [i.replace('/',',').split(',') for i in self.input_list]))
		else:
			self.db.cursor.executemany('''
				INSERT OR IGNORE INTO filtered_deps_repoedges(repo_source_id,repo_dest_id,reason)
				VALUES((SELECT id FROM repositories WHERE name=:rname1 AND owner=:rowner1
					AND source=(SELECT id FROM sources WHERE name=:psource1)),
				(SELECT id FROM repositories WHERE name=:rname2 AND owner=:rowner2
					AND source=(SELECT id FROM sources WHERE name=:psource2))
				,:reason)
				''',({'rname1':rname1,'rowner1':rowner1,'rname2':rname2,'rowner2':rowner2,'reason':self.reason,'psource1':psource1,'psource2':psource2} for psource1,rowner1,rname1,psource2,rowner2,rname2 in [i.replace('/',',').split(',') for i in self.input_list]))


class AutoRepoEdges2Cycles(RepoEdgesDepsFilter):

	default_source = 'GitHub'
	def __init__(self,**kwargs):
		RepoEdgesDepsFilter.__init__(self,input_list=[],reason='autoremove of 2-cycles in repo space base on earliest package creation date',**kwargs)

	def prepare(self):
		self.get_input_list()

	def get_input_list(self):
		self.db.cursor.execute('''
			WITH repodeps AS (SELECT DISTINCT p1.repo_id AS r1,p2.repo_id AS r2
						FROM package_dependencies pd
						INNER JOIN package_versions pv
						ON pd.depending_version =pv.id
						INNER JOIN packages p1
						ON p1.id=pd.depending_on_package
						INNER JOIN packages p2
						ON p2.id=pv.package_id
						AND p1.repo_id!=p2.repo_id)
			SELECT (SELECT name FROM sources WHERE id=rr1.source),rr1.OWNER,rr1.name,(SELECT name FROM sources WHERE id=rr2.source),rr2.OWNER,rr2.name --, rd1.r1,rd1.r2,dt1.min_d,dt2.min_d
				FROM repodeps rd1
				INNER JOIN repodeps rd2
				ON rd1.r1=rd2.r2 AND rd1.r2=rd2.r1
				INNER JOIN repositories rr1
				ON rd1.r1 = rr1.id
				INNER JOIN (SELECT p1.repo_id,MIN(p1.created_at) AS min_d FROM packages p1 GROUP BY repo_id) dt1
				ON dt1.repo_id=rr1.id
				INNER JOIN repositories rr2
				ON rd1.r2 = rr2.id
				INNER JOIN (SELECT p2.repo_id,MIN(p2.created_at) AS min_d FROM packages p2 GROUP BY repo_id) dt2
				ON dt2.repo_id=rr2.id AND dt2.min_d >= dt1.min_d
			''')

		self.input_list = [ '{}/{}/{},{}/{}/{}'.format(s1,r1o,r1n,s2,r2o,r2n) for (s1,r1o,r1n,s2,r2o,r2n) in self.db.cursor.fetchall()]
