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

from .. import fillers


class PackageDepsFilter(fillers.Filler):
	'''
	fills from a string list
	'''
	default_source = 'crates'
	def __init__(self,input_list=None,input_file=None,reason='filter from list',source=None,header=True,**kwargs):
		fillers.Filler.__init__(self,**kwargs)
		if source is None:
			self.info_source = self.default_source
		else:
			self.info_source = source
		self.header = header
		self.reason = reason
		self.input_list = input_list
		self.input_file = input_file

	def prepare(self):
		fillers.Filler.prepare(self)

		if self.input_list is not None:
			self.input_list = self.input_list
		elif self.input_file is not None:
			filepath = os.path.join(self.data_folder,self.input_file)

			with open(filepath,"rb") as f:
				filehash = hashlib.sha256(f.read()).hexdigest()
				self.source = '{}_{}'.format(self.input_file,filehash)
				self.db.register_source(source=self.source)

			with open(filepath,'r') as f:
				reader = csv.reader(f)
				if self.header:
					next(reader)
				self.input_list = list(reader)
		else:
			raise ValueError('input_list and input_file cannot both be None')
		self.input_list = [self.parse_element(e) for e in self.input_list]

	def apply(self):
		self.fill_filters()
		self.db.connection.commit()

	def parse_element(self,elt):
		'''
		output (source,packagename)
		'''
		if isinstance(elt,str):
			elt = (elt,)
		if len(elt) == 2:
			return elt
		elif len(elt) == 1:
			if '/' in elt[0]:
				return (elt[0].split('/')[0],'/'.join(elt[0].split('/')[1:]))
			else:
				return (self.info_source,elt[0])
		else:
			raise ValueError('{} not parsable'.format(elt))

	def fill_filters(self):
		if self.db.db_type == 'postgres':
			extras.execute_batch(self.db.cursor,'''
				INSERT INTO filtered_deps_package(package_id,reason)
				--VALUES((SELECT id FROM packages WHERE name=%(pname)s AND source_id=(SELECT id FROM sources WHERE name=%(psource)s)),%(reason)s)
				SELECT p.id,%(reason)s FROM packages p
				INNER JOIN sources s
				ON p.name=%(pname)s
				AND s.id=p.source_id
				AND s.name=%(psource)s
				 ON CONFLICT DO NOTHING;
				''',({'pname':pname,'reason':self.reason,'psource':s} for s,pname in self.input_list))
		else:
			self.db.cursor.executemany('''
				INSERT OR IGNORE INTO filtered_deps_package(package_id,reason)
				--VALUES((SELECT id FROM packages WHERE name=:pname AND source_id=(SELECT id FROM sources WHERE name=:psource) ),:reason)
				SELECT p.id,:reason FROM packages p
				INNER JOIN sources s
				ON p.name=:pname
				AND p.source_id=s.id
				AND s.name=:psource
				''',({'pname':pname,'reason':self.reason,'psource':s} for s,pname in self.input_list))

class RepoDepsFilter(PackageDepsFilter):

	default_source = 'GitHub'
	def fill_filters(self):
		if self.db.db_type == 'postgres':
			extras.execute_batch(self.db.cursor,'''
				INSERT INTO filtered_deps_repo(repo_id,reason)
				--VALUES((SELECT id FROM repositories WHERE name=%(rname)s AND owner=%(rowner)s AND source=(SELECT id FROM sources WHERE name=%(psource)s)),%(reason)s)
				SELECT r.id,%(reason)s FROM repositories r
				INNER JOIN sources s
				ON r.name=%(rname)s
				AND r.owner=%(rowner)s
				AND r.source=s.id
				AND s.name=%(psource)s
				 ON CONFLICT DO NOTHING;
				 ''',({'rname':rname,'rowner':rowner,'reason':self.reason,'psource':s} for s,rowner,rname in [(s,i.split('/')[0],i.split('/')[1]) for s,i in self.input_list]))
		else:
			self.db.cursor.executemany('''
				INSERT OR IGNORE INTO filtered_deps_repo(repo_id,reason)
				--VALUES((SELECT id FROM repositories WHERE name=:rname AND owner=:rowner AND source=(SELECT id FROM sources WHERE name=:psource) ),:reason)
				SELECT r.id,:reason FROM repositories r
				INNER JOIN sources s
				ON r.name=:rname
				AND r.owner=:rowner
				AND r.source=s.id
				AND s.name=:psource
				''',({'rname':rname,'rowner':rowner,'reason':self.reason,'psource':s} for s,rowner,rname in [(s,i.split('/')[0],i.split('/')[1]) for s,i in self.input_list]))

	def parse_element(self,elt):
		'''
		output (sourcename,owner/reponame)
		'''
		if isinstance(elt,str):
			elt = (elt,)
		if len(elt) == 2:
			if '/' not in elt[1]:
				return (self.info_source,'/'.join(elt))
			else:
				# return (elt[0],elt[1].split('/')[0],'/'.join(elt[1].split('/')[1:]))
				return elt
		elif len(elt) == 3:
			return (elt[0],'/'.join([elt[1:]]))
		elif len(elt) == 1:
			if len(elt[0].split('/')) == 3:
				return (elt[0].split('/')[0],'/'.join(elt[0].split('/')[1:]))
			elif len(elt[0].split('/')) == 2:
				return (self.info_source,elt[0])

		raise ValueError('{} not parsable'.format(elt))


class PackageEdgesDepsFilter(PackageDepsFilter):

	default_source = 'crates'
	def fill_filters(self):
		if self.db.db_type == 'postgres':
			extras.execute_batch(self.db.cursor,'''
				INSERT INTO filtered_deps_packageedges(package_source_id,package_dest_id,reason)
				SELECT ps.id,pd.id,%(reason)s
				FROM packages ps
				INNER JOIN sources ss
				ON ps.name=%(pname1)s
				AND ss.id=ps.source_id
				AND ss.name=%(psource1)s
				INNER JOIN packages pd
				ON pd.name=%(pname2)s
				INNER JOIN sources sd
				ON sd.id=pd.source_id
				AND sd.name=%(psource2)s
				ON CONFLICT DO NOTHING;
				''',({'pname1':pname1,'pname2':pname2,'reason':self.reason,'psource1':psource1,'psource2':psource2} for psource1,pname1,psource2,pname2 in self.input_list))
		else:
			self.db.cursor.executemany('''
				INSERT INTO filtered_deps_packageedges(package_source_id,package_dest_id,reason)
				SELECT ps.id,pd.id,:reason
				FROM packages ps
				INNER JOIN sources ss
				ON ps.name=:pname1
				AND ss.id=ps.source_id
				AND ss.name=:psource1
				INNER JOIN packages pd
				ON pd.name=:pname2
				INNER JOIN sources sd
				ON sd.id=pd.source_id
				AND sd.name=:psource2
				ON CONFLICT DO NOTHING;
				''',({'pname1':pname1,'pname2':pname2,'reason':self.reason,'psource1':psource1,'psource2':psource2} for psource1,pname1,psource2,pname2 in self.input_list))

	def parse_element(self,elt):
		'''
		output (sourcename,pname_source,s2,pname_dest)
		'''
		if isinstance(elt,str):
			elt = (elt,)
		if len(elt) == 2:
			return tuple(list(PackageDepsFilter.parse_element(self,elt[0]))+list(PackageDepsFilter.parse_element(self,elt[1])))
		elif len(elt) == 4:
			return tuple(list(PackageDepsFilter.parse_element(self,elt[:2]))+list(PackageDepsFilter.parse_element(self,elt[2:])))
		raise ValueError('{} not parsable'.format(elt))


class RepoEdgesDepsFilter(PackageDepsFilter):

	default_source = 'GitHub'
	def fill_filters(self):
		if self.db.db_type == 'postgres':
			extras.execute_batch(self.db.cursor,'''
				INSERT INTO filtered_deps_repoedges(repo_source_id,repo_dest_id,reason)
					--VALUES((SELECT id FROM repositories WHERE name=%(rname1)s AND owner=%(rowner1)s
					--	AND source=(SELECT id FROM sources WHERE name=%(psource1)s)),
					--(SELECT id FROM repositories WHERE name=%(rname2)s AND owner=%(rowner2)s
					--	AND source=(SELECT id FROM sources WHERE name=%(psource2)s))
					--,%(reason)s)
				SELECT rs.id,rd.id,%(reason)s
				FROM repositories rs
				INNER JOIN sources ss
				ON rs.name=%(rname1)s
				AND rs.owner=%(rowner1)s
				AND ss.id=rs.source
				AND ss.name=%(psource1)s
				INNER JOIN repositories rd
				ON rd.name=%(rname2)s
				AND rd.owner=%(rowner2)s
				INNER JOIN sources sd
				ON sd.id=rd.source
				AND sd.name=%(psource2)s
				ON CONFLICT DO NOTHING;
				''',({'rname1':rname1,'rowner1':rowner1,'rname2':rname2,'rowner2':rowner2,'reason':self.reason,'psource1':psource1,'psource2':psource2} for psource1,rowner1,rname1,psource2,rowner2,rname2 in [(s1,r1.split('/')[0],r1.split('/')[1],s2,r2.split('/')[0],r2.split('/')[1]) for s1,r1,s2,r2 in self.input_list]))
		else:
			self.db.cursor.executemany('''
				INSERT OR IGNORE INTO filtered_deps_repoedges(repo_source_id,repo_dest_id,reason)
					--VALUES((SELECT id FROM repositories WHERE name=:rname1 AND owner=:rowner1
					--	AND source=(SELECT id FROM sources WHERE name=:psource1)),
					--(SELECT id FROM repositories WHERE name=:rname2 AND owner=:rowner2
					--	AND source=(SELECT id FROM sources WHERE name=:psource2))
					--,:reason)
				SELECT rs.id,rd.id,:reason
				FROM repositories rs
				INNER JOIN sources ss
				ON rs.name=:rname1
				AND rs.owner=:rowner1
				AND ss.id=rs.source
				AND ss.name=:psource1
				INNER JOIN repositories rd
				ON rd.name=:rname2
				AND rd.owner=:rowner2
				INNER JOIN sources sd
				ON sd.id=rd.source
				AND sd.name=:psource2
				''',({'rname1':rname1,'rowner1':rowner1,'rname2':rname2,'rowner2':rowner2,'reason':self.reason,'psource1':psource1,'psource2':psource2} for psource1,rowner1,rname1,psource2,rowner2,rname2 in [(s1,r1.split('/')[0],r1.split('/')[1],s2,r2.split('/')[0],r2.split('/')[1]) for s1,r1,s2,r2 in self.input_list]))

	def parse_element(self,elt):
		'''
		output (sourcename,owner/reponame,s2,o2/rn2)
		'''
		if isinstance(elt,str):
			elt = (elt,)
		if len(elt) == 2:
			return tuple(list(RepoDepsFilter.parse_element(self,elt[0]))+list(RepoDepsFilter.parse_element(self,elt[1])))
		elif len(elt) == 4:
			return tuple(list(RepoDepsFilter.parse_element(self,elt[:2]))+list(RepoDepsFilter.parse_element(self,elt[2:])))
		elif len(elt) == 6:
			return tuple(list(RepoDepsFilter.parse_element(self,elt[:3]))+list(RepoDepsFilter.parse_element(self,elt[3:])))
		raise ValueError('{} not parsable'.format(elt))



class AutoPackageEdges2Cycles(PackageEdgesDepsFilter):

	default_source = 'GitHub'
	def __init__(self,**kwargs):
		PackageEdgesDepsFilter.__init__(self,input_list=[],reason='autoremove of 2-cycles in package space base on earliest package creation date',**kwargs)

	def prepare(self):
		PackageEdgesDepsFilter.prepare(self)
		self.get_input_list()

	def get_input_list(self):
		self.db.cursor.execute('''
			WITH pdeps AS (SELECT DISTINCT pv.package_id AS p1,pd.depending_on_package AS p2
						FROM package_dependencies pd
						INNER JOIN package_versions pv
						ON pd.depending_version =pv.id
						AND pv.package_id!=pd.depending_on_package)
			SELECT (SELECT name FROM sources WHERE id=pp1.source_id),pp1.name,(SELECT name FROM sources WHERE id=pp2.source_id),pp2.name 
				FROM pdeps pd1
				INNER JOIN pdeps pd2
				ON pd1.p1=pd2.p2 AND pd1.p2=pd2.p1
				INNER JOIN packages pp1
				ON pd1.p1 = pp1.id
				INNER JOIN packages pp2
				ON pd1.p2 = pp2.id
				AND pp1.created_at <= pp2.created_at
			''')

		self.input_list = list(self.db.cursor.fetchall())
		self.input_list = [self.parse_element(e) for e in self.input_list]


class AutoRepoEdges2Cycles(RepoEdgesDepsFilter):

	default_source = 'GitHub'
	def __init__(self,**kwargs):
		RepoEdgesDepsFilter.__init__(self,input_list=[],reason='autoremove of 2-cycles in repo space base on earliest repo or package creation date',**kwargs)

	def prepare(self):
		RepoEdgesDepsFilter.prepare(self)
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
				ON dt2.repo_id=rr2.id AND COALESCE(rr2.created_at,dt2.min_d) >= COALESCE(rr1.created_at,dt1.min_d)
			''')

		self.input_list = [ (s1,'/'.join([r1o,r1n]),s2,'/'.join([r2o,r2n])) for (s1,r1o,r1n,s2,r2o,r2n) in self.db.cursor.fetchall()]
		self.input_list = [self.parse_element(e) for e in self.input_list]


class FiltersFolderFiller(fillers.Filler):
	'''
	meta filler
	'''
	def __init__(self,input_folder='filters',
			repoedges_file='filtered_repoedges.csv',
			packageedges_file='filtered_packageedges.csv',
			repos_file='filtered_repos.csv',
			packages_file='filtered_packages.csv',
			**kwargs):
		self.input_folder = input_folder
		self.packages_file = packages_file
		self.repos_file = repos_file
		self.repoedges_file = repoedges_file
		self.packageedges_file = packageedges_file
		fillers.Filler.__init__(self,**kwargs)

	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder

		folder = os.path.join(self.data_folder,self.input_folder)

		self.db.add_filler(PackageDepsFilter(input_file=os.path.join(folder,self.packages_file)))
		self.db.add_filler(RepoDepsFilter(input_file=os.path.join(folder,self.repos_file)))
		self.db.add_filler(RepoEdgesDepsFilter(input_file=os.path.join(folder,self.repoedges_file)))
		self.db.add_filler(PackageEdgesDepsFilter(input_file=os.path.join(folder,self.packageedges_file)))


class FiltersLibFolderFiller(FiltersFolderFiller):
	def __init__(self,**kwargs):
		FiltersFolderFiller.__init__(self,input_folder=os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(fillers.__file__)),'data','filters')))
