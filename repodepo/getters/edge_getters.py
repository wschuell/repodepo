from . import Getter

from scipy import sparse
import datetime
import numpy as np
import pandas as pd
import copy
import json

class DevToRepo(Getter):
	def __init__(self,db,start_time=datetime.datetime(2010,1,1),end_time=datetime.datetime.now(),**kwargs):
		self.start_time = start_time
		self.end_time = end_time
		Getter.__init__(self,db=db,**kwargs)

	def query(self):
		if self.db.db_type == 'postgres':
			return '''
				SELECT main_q.user_id,
					user_q.user_rank,
					main_q.repo_id,
					repo_q.repo_rank,
					(main_q.cnt::DOUBLE PRECISION /COALESCE(SUM(main_q.cnt) OVER (PARTITION BY main_q.repo_id),1.))::DOUBLE PRECISION AS norm_value,
					main_q.cnt::DOUBLE PRECISION AS abs_value
				FROM
					(SELECT i.user_id,c.repo_id,count(*) AS cnt FROM commits c
					INNER JOIN identities i
					ON c.author_id=i.id AND c.created_at>%(start_time)s AND c.created_at<=%(end_time)s AND NOT i.is_bot
					GROUP BY i.user_id ,c.repo_id
					--ORDER BY count(*) DESC
					) AS main_q
				INNER JOIN (
					SELECT id AS user_id,
					RANK() OVER
						(ORDER BY id) AS user_rank,
					is_bot
					FROM users uu
					) AS user_q
				ON main_q.user_id=user_q.user_id
				INNER JOIN (
					SELECT id AS repo_id,
					RANK() OVER
						(ORDER BY id) AS repo_rank
					FROM repositories rr
					) AS repo_q
				ON main_q.repo_id=repo_q.repo_id
			;'''
		else:
			return '''
				SELECT main_q.user_id,
					user_q.user_rank,
					main_q.repo_id,
					repo_q.repo_rank,
					(main_q.cnt*1./COALESCE(SUM(main_q.cnt) OVER (PARTITION BY main_q.repo_id),1.)) AS norm_value,
					main_q.cnt*1. AS abs_value
				FROM
					(SELECT i.user_id,c.repo_id,count(*) AS cnt FROM commits c
					INNER JOIN identities i
					ON c.author_id=i.id AND c.created_at>:start_time AND c.created_at<=:end_time AND NOT i.is_bot
					GROUP BY i.user_id ,c.repo_id
					--ORDER BY count(*) DESC
					) AS main_q
				INNER JOIN (
					SELECT id AS user_id,
					RANK() OVER
						(ORDER BY id) AS user_rank,
					is_bot
					FROM users uu
					) AS user_q
				ON main_q.user_id=user_q.user_id
				INNER JOIN (
					SELECT id AS repo_id,
					RANK() OVER
						(ORDER BY id) AS repo_rank
					FROM repositories rr
					) AS repo_q
				ON main_q.repo_id=repo_q.repo_id
			;'''


	def query_attributes(self):
		return {
		'start_time':self.start_time,
		'end_time':self.end_time,
		}

	def parse_results(self,query_result,abs_value=False):
		data = []
		coords_u = []
		coords_r = []
		for (user_id,user_rank,repo_id,repo_rank,cnt,cnt_abs) in query_result:
			if abs_value:
				data.append(cnt_abs)
			else:
				data.append(cnt)
			coords_u.append(user_rank-1)
			coords_r.append(repo_rank-1)
		return {'data':data,'coords_r':coords_r,'coords_u':coords_u}

	def get_umax(self,db=None):
		if db is None:
			db = self.db
		db.cursor.execute('SELECT COUNT(*) FROM users u;')
		return db.cursor.fetchone()[0]

	def get_rmax(self,db=None):
		if db is None:
			db = self.db
		db.cursor.execute('SELECT COUNT(*) FROM repositories r;')
		return db.cursor.fetchone()[0]

	def get(self,db,abs_value=False,raw_result=False,**kwargs):
		u_max = self.get_umax(db=db)
		r_max = self.get_rmax(db=db)
		db.cursor.execute(self.query(),self.query_attributes())
		# query_result = list(db.cursor.fetchall())
		# self.parse_results(query_result=query_result)
		if raw_result:
			return ({'user_id':uid,'user_rank':urk,'repo_id':rid,'repo_rank':rrk,'norm_value':normval,'abs_value':absval} for (uid,urk,rid,rrk,normval,absval) in db.cursor.fetchall())
		else:
			parsed_results = self.parse_results(query_result=db.cursor.fetchall(),abs_value=abs_value)
			ans_mat = sparse.csr_matrix((parsed_results['data'],(parsed_results['coords_r'],parsed_results['coords_u'])),shape=(r_max,u_max))
			return ans_mat


class DevToRepoAddMax(DevToRepo):
	def __init__(self,db,repo_list,**kwargs):
		self.repo_list = tuple(int(r) for r in repo_list)
		DevToRepo.__init__(self,db=db,**kwargs)

	def get_umax(self,db=None):
		if db is None:
			db = self.db
		return DevToRepo.get_umax(self,db=db) + len(self.repo_list)

	def query(self):
		if self.db.db_type == 'postgres':
			return '''
				SELECT main_q.user_id,
					user_q.user_rank,
					main_q.repo_id,
					repo_q.repo_rank,
					(GREATEST(0,main_q.cnt-CASE WHEN main_q.repo_id IN %(repo_list)s
									THEN MAX(main_q.cnt) OVER (PARTITION BY main_q.repo_id)
									ELSE 0
									END)*1./COALESCE(
								SUM(main_q.cnt) OVER (PARTITION BY main_q.repo_id)

							,1))::DOUBLE PRECISION AS norm_value,
					GREATEST(0,main_q.cnt-CASE WHEN main_q.repo_id IN %(repo_list)s
									THEN MAX(main_q.cnt) OVER (PARTITION BY main_q.repo_id)
									ELSE 0
									END)::DOUBLE PRECISION AS abs_value,
					(CASE WHEN main_q.repo_id IN %(repo_list)s
									THEN MAX(main_q.cnt) OVER (PARTITION BY main_q.repo_id)
										*1./(SUM(main_q.cnt) OVER (PARTITION BY main_q.repo_id)
											)
									ELSE 0
									END)::DOUBLE PRECISION AS max_value
				FROM
					(SELECT i.user_id,c.repo_id,count(*) AS cnt FROM commits c
					INNER JOIN identities i
					ON c.author_id=i.id AND c.created_at>%(start_time)s AND c.created_at<=%(end_time)s AND NOT i.is_bot
					GROUP BY i.user_id ,c.repo_id
					--ORDER BY count(*) DESC
					) AS main_q
				INNER JOIN (
					SELECT id AS user_id,
					RANK() OVER
						(ORDER BY id) AS user_rank,
					is_bot
					FROM users uu
					) AS user_q
				ON main_q.user_id=user_q.user_id
				INNER JOIN (
					SELECT id AS repo_id,
					RANK() OVER
						(ORDER BY id) AS repo_rank
					FROM repositories rr
					) AS repo_q
				ON main_q.repo_id=repo_q.repo_id
			;'''
		else:
			return '''
				SELECT main_q.user_id,
					user_q.user_rank,
					main_q.repo_id,
					repo_q.repo_rank,
					(MAX(0,main_q.cnt-CASE WHEN main_q.repo_id IN {repo_list_str}
									THEN MAX(main_q.cnt) OVER (PARTITION BY main_q.repo_id)
									ELSE 0
									END)*1./COALESCE(
								SUM(main_q.cnt) OVER (PARTITION BY main_q.repo_id)

							,1)) AS norm_value,
					MAX(0,main_q.cnt-CASE WHEN main_q.repo_id IN {repo_list_str}
									THEN MAX(main_q.cnt) OVER (PARTITION BY main_q.repo_id)
									ELSE 0
									END) AS abs_value,
					CASE WHEN main_q.repo_id IN {repo_list_str}
									THEN MAX(main_q.cnt) OVER (PARTITION BY main_q.repo_id)
										*1./(SUM(main_q.cnt) OVER (PARTITION BY main_q.repo_id)
											)
									ELSE 0
									END AS max_value
				FROM
					(SELECT i.user_id,c.repo_id,count(*) AS cnt FROM commits c
					INNER JOIN identities i
					ON c.author_id=i.id AND c.created_at>:start_time AND c.created_at<=:end_time AND NOT i.is_bot
					GROUP BY i.user_id ,c.repo_id
					--ORDER BY count(*) DESC
					) AS main_q
				INNER JOIN (
					SELECT id AS user_id,
					RANK() OVER
						(ORDER BY id) AS user_rank,
					is_bot
					FROM users uu
					) AS user_q
				ON main_q.user_id=user_q.user_id
				INNER JOIN (
					SELECT id AS repo_id,
					RANK() OVER
						(ORDER BY id) AS repo_rank
					FROM repositories rr
					) AS repo_q
				ON main_q.repo_id=repo_q.repo_id
			;'''.format(**self.query_attributes())


	def parse_results(self,query_result,abs_value=False):
		data = []
		coords_u = []
		coords_r = []
		remaining_repos = list(copy.deepcopy(self.repo_list))
		offset = 0
		u_max = DevToRepo.get_umax(self)
		for (user_id,user_rank,repo_id,repo_rank,cnt,cnt_abs,cnt_max) in query_result:
			if abs_value:
				data.append(cnt_abs)
			else:
				data.append(cnt)
			coords_u.append(user_rank-1)
			coords_r.append(repo_rank-1)
			# if repo_id in remaining_repos:
				# data.append(cnt_max)
				# coords_r.append(repo_rank-1)
				# coords_u.append(u_max + offset)
				# offset += 1
				# remaining_repos.remove(repo_id)
		return {'data':data,'coords_r':coords_r,'coords_u':coords_u}

	def query_attributes(self):
		if self.db.db_type == 'postgres':
			ans = {
				'start_time':self.start_time,
				'end_time':self.end_time,
				'repo_list':self.repo_list
				}
			if len(self.repo_list) == 0:
				ans['repo_list'] = None
		else:
			ans = {
				'start_time':self.start_time,
				'end_time':self.end_time,
				'repo_list':self.repo_list,
				'repo_list_str':'({})'.format(','.join([':repo_list_{}'.format(i) for i,rl in enumerate(self.repo_list)])),
				**{'repo_list_{}'.format(i):rl for i,rl in enumerate(self.repo_list)}
				}
			if len(self.repo_list) == 0:
				ans['repo_list_str'] = None
		return ans
		
class DevToRepoAddDailyCommits(DevToRepoAddMax):
	def __init__(self,daily_commits,**kwargs):
		DevToRepoAddMax.__init__(self,**kwargs)
		self.daily_commits = daily_commits

	def query(self): 
		if self.db.db_type == 'postgres': 
			return '''
				SELECT main_q.user_id,
					user_q.user_rank,
					main_q.repo_id,
					repo_q.repo_rank,
					(GREATEST(0,main_q.cnt-CASE WHEN main_q.repo_id IN %(repo_list)s
									THEN GREATEST(DATE_PART('day',
										%(end_time)s::timestamp- GREATEST(COALESCE(repo_q.repo_created_at,%(start_time)s::timestamp),%(start_time)s::timestamp)
										),1.)::DOUBLE PRECISION *%(daily_commits)s
									ELSE 0
									END)*1./COALESCE(
								SUM(main_q.cnt) OVER (PARTITION BY main_q.repo_id)

							,1.))::DOUBLE PRECISION AS norm_value,
					GREATEST(0,main_q.cnt-CASE WHEN main_q.repo_id IN %(repo_list)s
									THEN GREATEST(DATE_PART('day',
										%(end_time)s::timestamp- GREATEST(COALESCE(repo_q.repo_created_at,%(start_time)s::timestamp),%(start_time)s::timestamp)
										),1.)::DOUBLE PRECISION *%(daily_commits)s
									ELSE 0
									END)::DOUBLE PRECISION AS abs_value,
					(CASE WHEN main_q.repo_id IN %(repo_list)s
									THEN GREATEST(DATE_PART('day',
											%(end_time)s::timestamp- GREATEST(COALESCE(repo_q.repo_created_at,%(start_time)s::timestamp),%(start_time)s::timestamp)
										),1.)::DOUBLE PRECISION *%(daily_commits)s
										*1./(SUM(main_q.cnt) OVER (PARTITION BY main_q.repo_id)
											)
									ELSE 0
									END)::DOUBLE PRECISION AS inserted_value
				FROM
					(SELECT i.user_id,c.repo_id,count(*) AS cnt FROM commits c
					INNER JOIN identities i
					ON c.author_id=i.id AND c.created_at>%(start_time)s AND c.created_at<=%(end_time)s AND NOT i.is_bot
					GROUP BY i.user_id ,c.repo_id
					--ORDER BY count(*) DESC
					) AS main_q
				INNER JOIN (
					SELECT id AS user_id,
					RANK() OVER
						(ORDER BY id) AS user_rank,
					is_bot
					FROM users uu
					) AS user_q
				ON main_q.user_id=user_q.user_id
				INNER JOIN (
					SELECT id AS repo_id,
					RANK() OVER
						(ORDER BY id) AS repo_rank,
					created_at AS repo_created_at
					FROM repositories rr
					) AS repo_q
				ON main_q.repo_id=repo_q.repo_id
			;'''
		else:
			return '''
				SELECT main_q.user_id,
					user_q.user_rank,
					main_q.repo_id,
					repo_q.repo_rank,
					(MAX(0,main_q.cnt-CASE WHEN main_q.repo_id IN {repo_list_str}
									THEN MAX(CAST ((JulianDay(:end_time) - JulianDay(MAX(COALESCE(repo_q.repo_created_at,:start_time),:start_time))) AS INTEGER),1.)*:daily_commits
									ELSE 0
									END)*1./COALESCE(
								SUM(main_q.cnt) OVER (PARTITION BY main_q.repo_id)

							,1.)) AS norm_value,
					MAX(0,main_q.cnt-CASE WHEN main_q.repo_id IN {repo_list_str}
									THEN MAX(CAST ((JulianDay(:end_time) - JulianDay(MAX(COALESCE(repo_q.repo_created_at,:start_time),:start_time))) AS INTEGER),1.)*:daily_commits
									ELSE 0
									END) AS abs_value,
					CASE WHEN main_q.repo_id IN {repo_list_str}
									THEN MAX(CAST ((JulianDay(:end_time) - JulianDay(MAX(COALESCE(repo_q.repo_created_at,:start_time),:start_time))) AS INTEGER),1.)*:daily_commits
										*1./(SUM(main_q.cnt) OVER (PARTITION BY main_q.repo_id)
											)
									ELSE 0
									END AS max_value
				FROM
					(SELECT i.user_id,c.repo_id,count(*) AS cnt FROM commits c
					INNER JOIN identities i
					ON c.author_id=i.id AND c.created_at>:start_time AND c.created_at<=:end_time AND NOT i.is_bot
					GROUP BY i.user_id ,c.repo_id
					--ORDER BY count(*) DESC
					) AS main_q
				INNER JOIN (
					SELECT id AS user_id,
					RANK() OVER
						(ORDER BY id) AS user_rank,
					is_bot
					FROM users uu
					) AS user_q
				ON main_q.user_id=user_q.user_id
				INNER JOIN (
					SELECT id AS repo_id,
					RANK() OVER
						(ORDER BY id) AS repo_rank,
					created_at AS repo_created_at
					FROM repositories rr
					) AS repo_q
				ON main_q.repo_id=repo_q.repo_id
			;'''.format(**self.query_attributes())


	def query_attributes(self):
		ans = DevToRepoAddMax.query_attributes(self)
		ans['daily_commits'] = self.daily_commits
		return ans




class RepoToRepoDeps(Getter):
	def __init__(self,db,ref_time=datetime.datetime.now(),filter_deps=True,**kwargs):
		self.ref_time = ref_time
		self.filter_deps = filter_deps
		Getter.__init__(self,db=db,**kwargs)

	def query(self):
		if self.db.db_type == 'postgres':
			return '''
				SELECT DISTINCT
					dep_q.repo_id AS depending_repo_id,
					repo_q1.repo_rank AS depending_repo_rank,
					dep_q.do_repo_id AS depending_on_repo_id,
					repo_q2.repo_rank AS depending_on_repo_rank,
					(1./COALESCE(COUNT(*) OVER (PARTITION BY dep_q.repo_id),1))::DOUBLE PRECISION
				FROM
					(SELECT p.repo_id,p_do.repo_id AS do_repo_id,pv.id AS version_id
						FROM package_dependencies pd
						INNER JOIN package_versions pv
						ON pd.depending_version =pv.id
						AND pv.created_at <= %(ref_time)s
						INNER JOIN packages p
						ON pv.package_id=p.id AND p.repo_id IS NOT NULL
						INNER JOIN packages p_do
						ON pd.depending_on_package=p_do.id AND p_do.repo_id IS NOT NULL
						AND p_do.repo_id != p.repo_id
						AND (NOT %(filter_deps)s OR pv.package_id NOT IN (SELECT package_id FROM filtered_deps_package))
						AND (NOT %(filter_deps)s OR p_do.repo_id NOT IN (SELECT repo_id FROM filtered_deps_repo))
						LEFT OUTER JOIN filtered_deps_packageedges fdpe
						ON (NOT %(filter_deps)s OR (pv.package_id=fdpe.package_source_id AND p_do.id=fdpe.package_dest_id))
						WHERE (NOT %(filter_deps)s OR fdpe.package_dest_id IS NULL)
					) AS dep_q
				INNER JOIN (
					SELECT DISTINCT FIRST_VALUE(id) OVER (PARTITION BY package_id ORDER BY created_at DESC,version_str DESC) AS last_v_id
					FROM package_versions pv
					WHERE pv.created_at <= %(ref_time)s
					) AS lastv_q
				ON dep_q.version_id=lastv_q.last_v_id
				INNER JOIN (
					SELECT id AS repo_id,
					RANK() OVER
						(ORDER BY id) AS repo_rank
					FROM repositories rr
					) AS repo_q1
				ON dep_q.repo_id=repo_q1.repo_id
				INNER JOIN (
					SELECT id AS repo_id,
					RANK() OVER
						(ORDER BY id) AS repo_rank
					FROM repositories rr
					) AS repo_q2
				ON dep_q.do_repo_id=repo_q2.repo_id
				LEFT JOIN filtered_deps_repoedges fdre
				ON fdre.repo_dest_id=dep_q.do_repo_id
				AND fdre.repo_source_id=dep_q.repo_id
				WHERE (NOT %(filter_deps)s OR fdre.repo_source_id IS NULL)
			;'''
		else:
			return '''
				SELECT DISTINCT
					dep_q.repo_id AS depending_repo_id,
					repo_q1.repo_rank AS depending_repo_rank,
					dep_q.do_repo_id AS depending_on_repo_id,
					repo_q2.repo_rank AS depending_on_repo_rank,
					1./COALESCE(COUNT(*) OVER (PARTITION BY dep_q.repo_id),1)
				FROM
					(SELECT p.repo_id,p_do.repo_id AS do_repo_id,pv.id AS version_id
						FROM package_dependencies pd
						INNER JOIN package_versions pv
						ON pd.depending_version =pv.id
						AND pv.created_at <= :ref_time
						INNER JOIN packages p
						ON pv.package_id=p.id AND p.repo_id IS NOT NULL
						INNER JOIN packages p_do
						ON pd.depending_on_package=p_do.id AND p_do.repo_id IS NOT NULL
						AND p_do.repo_id != p.repo_id
						AND (NOT :filter_deps OR pv.package_id NOT IN (SELECT package_id FROM filtered_deps_package))
						AND (NOT :filter_deps OR p_do.repo_id NOT IN (SELECT repo_id FROM filtered_deps_repo))
						LEFT OUTER JOIN filtered_deps_packageedges fdpe
						ON (NOT :filter_deps OR (pv.package_id=fdpe.package_source_id AND p_do.id=fdpe.package_dest_id))
						WHERE (NOT :filter_deps OR fdpe.package_dest_id IS NULL)
					) AS dep_q
				INNER JOIN (
					SELECT DISTINCT FIRST_VALUE(id) OVER (PARTITION BY package_id ORDER BY created_at DESC,version_str DESC) AS last_v_id
					FROM package_versions pv
					WHERE pv.created_at <= :ref_time
					) AS lastv_q
				ON dep_q.version_id=lastv_q.last_v_id
				INNER JOIN (
					SELECT id AS repo_id,
					RANK() OVER
						(ORDER BY id) AS repo_rank
					FROM repositories rr
					) AS repo_q1
				ON dep_q.repo_id=repo_q1.repo_id
				INNER JOIN (
					SELECT id AS repo_id,
					RANK() OVER
						(ORDER BY id) AS repo_rank
					FROM repositories rr
					) AS repo_q2
				ON dep_q.do_repo_id=repo_q2.repo_id
				LEFT JOIN filtered_deps_repoedges fdre
				ON fdre.repo_dest_id=dep_q.do_repo_id
				AND fdre.repo_source_id=dep_q.repo_id
				WHERE (NOT :filter_deps OR fdre.repo_source_id IS NULL)
			;'''


	def query_attributes(self):
		return {
		'ref_time':self.ref_time,
		'filter_deps':self.filter_deps,
		}

	def parse_results(self,query_result):
		data = []
		coords_r = []
		coords_r_do = []
		for (repo_id,repo_rank,do_repo_id,do_repo_rank,val) in query_result:
			data.append(val)
			coords_r.append(repo_rank-1)
			coords_r_do.append(do_repo_rank-1)
		return {'data':data,'coords_r':coords_r,'coords_r_do':coords_r_do}


	def get(self,db,raw_result=False,**kwargs):
		db.cursor.execute('SELECT COUNT(*) FROM repositories r;')
		r_max = db.cursor.fetchone()[0]
		db.cursor.execute(self.query(),self.query_attributes())
		# query_result = list(db.cursor.fetchall())
		# self.parse_results(query_result=query_result)
		if raw_result:
			return ({'repo_id':rid,'repo_rank':rrk,'dep_id':did,'dep_rank':drk,'value':val} for (rid,rrk,did,drk,val) in db.cursor.fetchall())
		else:
			parsed_results = self.parse_results(query_result=db.cursor.fetchall())
			ans_mat = sparse.csr_matrix((parsed_results['data'],(parsed_results['coords_r'],parsed_results['coords_r_do'])),shape=(r_max,r_max))
			return ans_mat

