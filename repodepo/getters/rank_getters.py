from . import Getter
from .edge_getters import RepoToRepoDeps

from scipy import sparse
import copy
import datetime
import numpy as np
import pandas as pd

class RepoRankGetter(Getter):
	values_dtype = np.float64

	def query(self):
		return '''SELECT id AS repo_id,
					RANK() OVER
						(ORDER BY id) AS repo_rank,
					0. AS value
					FROM repositories rr
			;'''


	def query_attributes(self):
		return {
		}

	def parse_results(self,query_result):
		return [(r_id,rk-1,val) for (r_id,rk,val) in query_result]

	def get_size_max(self,db):
		db.cursor.execute('SELECT COUNT(*) FROM repositories r;')
		r_max = db.cursor.fetchone()[0]
		return r_max

	def get(self,db,orig_id_rank=False,**kwargs):
		r_max = self.get_size_max(db=db)
		db.cursor.execute(self.query(),self.query_attributes())
		parsed_results = self.parse_results(query_result=db.cursor.fetchall())
		ans_direct = np.zeros(shape=(r_max,),dtype=np.int64)
		ans_values = np.zeros(shape=(r_max,),dtype=self.values_dtype)
		ans_indirect = {}
		for r_id,rk,val in parsed_results:
			ans_indirect[r_id] = rk
			ans_direct[rk] = r_id
			ans_values[rk] = val
		if not orig_id_rank:
			return ans_direct,ans_indirect,ans_values
		else:
			return self.reorder(prev_direct=ans_direct,prev_indirect=ans_indirect,prev_values=ans_values,db=db,**kwargs)

	def reorder(self,prev_direct,prev_indirect,prev_values,db,**kwargs):
		orig_direct,orig_indirect,orig_values = RepoRankGetter(db=db).get(db=db,orig_id_rank=False,**kwargs)
		reord_values = orig_values.copy()
		for i,v in enumerate(prev_values):
			reord_values[orig_indirect[prev_direct[i]]] = v
		return orig_direct,orig_indirect,reord_values

class RepoRankNameGetter(RepoRankGetter):
	values_dtype = np.object_

	def query(self):
		if self.db.db_type == 'postgres':
			return '''SELECT rr.id AS repo_id,
					RANK() OVER
						(ORDER BY rr.id) AS repo_rank,
					CONCAT(s.name,'/',rr.owner,'/',rr.name) AS value
					FROM repositories rr
					INNER JOIN sources s
					ON s.id=rr.source
			;'''
		else:			
			return '''SELECT rr.id AS repo_id,
					RANK() OVER
						(ORDER BY rr.id) AS repo_rank,
					s.name || '/' || rr.owner || '/' || rr.name AS value
					FROM repositories rr
					INNER JOIN sources s
					ON s.id=rr.source
			;'''

class UserRankGetter(RepoRankGetter):
	def __init__(self,no_bots=False,**kwargs):
		self.no_bots = no_bots
		RepoRankGetter.__init__(self,**kwargs)

	def query(self):
		if self.db.db_type == 'postgres':
			return '''SELECT id AS user_id,
					RANK() OVER
						(ORDER BY id) AS user_rank,
					0. AS value
					FROM users
					WHERE NOT (%(no_bots)s AND is_bot)
			;'''
		else:
			return '''SELECT id AS user_id,
					RANK() OVER
						(ORDER BY id) AS user_rank,
					0. AS value
					FROM users
					WHERE NOT (:no_bots AND is_bot)
			;'''

	def query_attributes(self):
		return {'no_bots':self.no_bots}

	def get_size_max(self,db):
		db.cursor.execute('SELECT COUNT(*) FROM users u;')
		r_max = db.cursor.fetchone()[0]
		return r_max

	def reorder(self,prev_direct,prev_indirect,prev_values,db,**kwargs):
		orig_direct,orig_indirect,orig_values = UserRankGetter(db=db).get(db=db,orig_id_rank=False,**kwargs)
		reord_values = orig_values.copy()
		for i,v in enumerate(prev_values):
			reord_values[orig_indirect[prev_direct[i]]] = v
		return orig_direct,orig_indirect,reord_values

class RepoStarRank(RepoRankGetter):
	def query(self):
		return '''SELECT id AS repo_id,
					RANK() OVER
						(ORDER BY starcount DESC,id ASC) AS repo_rank,
					starcount AS value
					FROM
						(SELECT r.id, COUNT(s.repo_id) AS starcount
							FROM repositories r
							LEFT OUTER JOIN stars s
							ON s.repo_id=r.id
							GROUP BY r.id
						) AS repo_stars
			;'''


class RepoDepRank(RepoRankGetter):
	def __init__(self,ref_time=datetime.datetime.now(),**kwargs):
		RepoRankGetter.__init__(self,**kwargs)
		self.ref_time = ref_time

	def query_attributes(self):
		return {'ref_time':self.ref_time}

	def query(self):
		if self.db.db_type == 'postgres':
			return '''
				SELECT dep_count_q.repo_id AS repo_id,
					RANK() OVER
						(ORDER BY dep_count_q.depcount DESC,dep_count_q.repo_id ASC) AS repo_rank,
						dep_count_q.depcount AS value
						FROM
						(SELECT COUNT(*) AS depcount,r.id AS repo_id
							FROM repositories r
							LEFT OUTER JOIN
								(SELECT DISTINCT
									dep_q.repo_id AS depending_repo_id,
									dep_q.do_repo_id AS depending_on_repo_id
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
										) AS dep_q
									INNER JOIN (
										SELECT DISTINCT FIRST_VALUE(id) OVER (PARTITION BY package_id ORDER BY created_at DESC,version_str DESC) AS last_v_id
										FROM package_versions pv
										WHERE pv.created_at <= %(ref_time)s
										) AS lastv_q
									ON dep_q.version_id=lastv_q.last_v_id
								) AS deps_mat_q
						ON r.id=deps_mat_q.depending_on_repo_id
						GROUP BY r.id) AS dep_count_q
			;'''
		else:
			return '''
				SELECT dep_count_q.repo_id AS repo_id,
					RANK() OVER
						(ORDER BY dep_count_q.depcount DESC,dep_count_q.repo_id ASC) AS repo_rank,
						dep_count_q.depcount AS value
						FROM
						(SELECT COUNT(*) AS depcount,r.id AS repo_id
							FROM repositories r
							LEFT OUTER JOIN
								(SELECT DISTINCT
									dep_q.repo_id AS depending_repo_id,
									dep_q.do_repo_id AS depending_on_repo_id
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
										) AS dep_q
									INNER JOIN (
										SELECT DISTINCT FIRST_VALUE(id) OVER (PARTITION BY package_id ORDER BY created_at DESC,version_str DESC) AS last_v_id
										FROM package_versions pv
										WHERE pv.created_at <= :ref_time
										) AS lastv_q
									ON dep_q.version_id=lastv_q.last_v_id
								) AS deps_mat_q
						ON r.id=deps_mat_q.depending_on_repo_id
						GROUP BY r.id) AS dep_count_q
			;'''


class RepoTransitiveDepRank(RepoDepRank):

	def query(self):
		if self.db.db_type == 'postgres':
			return '''
					WITH RECURSIVE deps_mat_q (depending,depended)
							AS (SELECT DISTINCT
									dep_q.repo_id AS depending,
									dep_q.do_repo_id AS depended
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
										) AS dep_q
									INNER JOIN (
										SELECT DISTINCT FIRST_VALUE(id) OVER (PARTITION BY package_id ORDER BY created_at DESC,version_str DESC) AS last_v_id
										FROM package_versions pv
										WHERE pv.created_at <= %(ref_time)s
										) AS lastv_q
									ON dep_q.version_id=lastv_q.last_v_id
								)
					, trans_deps_mat_q (depending,depended)
							AS (SELECT depending,depended FROM deps_mat_q dm
									UNION
								SELECT tdm.depending,dm.depended
								FROM deps_mat_q dm
								INNER JOIN trans_deps_mat_q tdm
								ON dm.depending=tdm.depended
								)
					SELECT dep_count_q.repo_id AS repo_id,
					RANK() OVER
						(ORDER BY dep_count_q.depcount DESC,dep_count_q.repo_id ASC) AS repo_rank,
						dep_count_q.depcount AS value
						FROM (SELECT COUNT(*) AS depcount,r.id AS repo_id
								FROM repositories r
								LEFT OUTER JOIN trans_deps_mat_q tdm
								ON r.id=tdm.depended
								GROUP BY r.id
							) AS dep_count_q
			;'''
		else:
			return '''
					WITH RECURSIVE deps_mat_q (depending,depended)
							AS (SELECT DISTINCT
									dep_q.repo_id AS depending,
									dep_q.do_repo_id AS depended
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
										) AS dep_q
									INNER JOIN (
										SELECT DISTINCT FIRST_VALUE(id) OVER (PARTITION BY package_id ORDER BY created_at DESC,version_str DESC) AS last_v_id
										FROM package_versions pv
										WHERE pv.created_at <= :ref_time
										) AS lastv_q
									ON dep_q.version_id=lastv_q.last_v_id
								ORDER BY depending,depended
								)
					, trans_deps_mat_q (depending,depended)
							AS (SELECT depending,depended FROM deps_mat_q dm
									UNION
								SELECT tdm.depending,dm.depended
								FROM deps_mat_q dm
								INNER JOIN trans_deps_mat_q tdm
								ON dm.depending=tdm.depended
								)
					SELECT dep_count_q.repo_id AS repo_id,
					RANK() OVER
						(ORDER BY dep_count_q.depcount DESC,dep_count_q.repo_id ASC) AS repo_rank,
						dep_count_q.depcount AS value
						FROM (SELECT COUNT(*) AS depcount,r.id AS repo_id
								FROM repositories r
								LEFT OUTER JOIN trans_deps_mat_q tdm
								ON r.id=tdm.depended
								GROUP BY r.id
							) AS dep_count_q
			;'''

class RepoDLRank(RepoDepRank):

	def __init__(self,start_time=datetime.datetime(2000,1,1),end_time=datetime.datetime.now(),ref_time=None,**kwargs):
		RepoRankGetter.__init__(self,**kwargs)
		self.start_time = start_time
		self.end_time = end_time
		if ref_time is None:
			self.ref_time = self.end_time
		else:
			self.ref_time = ref_time

	def query_attributes(self):
		return {'start_time':self.start_time,
			'end_time':self.end_time,
			'ref_time':self.ref_time,
			}

	def query(self):
		if self.db.db_type == 'postgres':
			return '''
				SELECT r.id,
				RANK() OVER
						(ORDER BY COALESCE(main_q.dl_cnt,0) DESC,r.id ASC) AS repo_rank,
					COALESCE(main_q.dl_cnt,0) AS value
				FROM
					(SELECT p.repo_id,SUM(pvd.downloads) AS dl_cnt
					FROM package_version_downloads pvd
					INNER JOIN package_versions pv
					ON pvd.downloaded_at>%(start_time)s AND pvd.downloaded_at<=%(end_time)s
					AND pv.id=pvd.package_version
					INNER JOIN packages p
					ON p.id=pv.package_id
					GROUP BY p.repo_id
					) AS main_q
				RIGHT OUTER JOIN repositories r
				ON main_q.repo_id=r.id
				ORDER BY r.id
			;'''
		else:
			return '''
				SELECT r.id,
				RANK() OVER
						(ORDER BY COALESCE(main_q.dl_cnt,0) DESC,r.id ASC) AS repo_rank,
					COALESCE(main_q.dl_cnt,0) AS value
				FROM repositories r
				LEFT OUTER JOIN
					(SELECT p.repo_id,SUM(pvd.downloads) AS dl_cnt
					FROM package_version_downloads pvd
					INNER JOIN package_versions pv
					ON pvd.downloaded_at>:start_time AND pvd.downloaded_at<=:end_time
					AND pv.id=pvd.package_version
					INNER JOIN packages p
					ON p.id=pv.package_id
					GROUP BY p.repo_id
					) AS main_q
				ON main_q.repo_id=r.id
				ORDER BY r.id
			;'''


class RepoDepDLRank(RepoDLRank):
	def query(self):
		if self.db.db_type == 'postgres':
			return '''
			SELECT dep_count_q.repo_id AS repo_id,
					RANK() OVER
						(ORDER BY dep_count_q.depcount DESC,dep_count_q.repo_id ASC) AS repo_rank,
						dep_count_q.depcount AS value
						FROM
						(SELECT SUM(dl_q.dl_cnt) AS depcount,r.id AS repo_id
							FROM repositories r
							LEFT OUTER JOIN
								(SELECT DISTINCT
									dep_q.repo_id AS depending_repo_id,
									dep_q.do_repo_id AS depending_on_repo_id
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
										) AS dep_q
									INNER JOIN (
										SELECT DISTINCT FIRST_VALUE(id) OVER (PARTITION BY package_id ORDER BY created_at DESC,version_str DESC) AS last_v_id
										FROM package_versions pv
										WHERE pv.created_at <= %(ref_time)s
										) AS lastv_q
									ON dep_q.version_id=lastv_q.last_v_id
								) AS deps_mat_q
						ON r.id=deps_mat_q.depending_on_repo_id
						INNER JOIN (SELECT p.repo_id,SUM(pvd.downloads) AS dl_cnt
								FROM package_version_downloads pvd
								INNER JOIN package_versions pv
								ON pvd.downloaded_at> %(start_time)s AND pvd.downloaded_at<= %(end_time)s
								AND pv.id=pvd.package_version
								INNER JOIN packages p
								ON p.id=pv.package_id
								GROUP BY p.repo_id
								) AS dl_q
						ON dl_q.repo_id=r.id OR dl_q.repo_id=deps_mat_q.depending_repo_id
						GROUP BY r.id) AS dep_count_q
			;'''
		else:
			return '''
			SELECT dep_count_q.repo_id AS repo_id,
					RANK() OVER
						(ORDER BY dep_count_q.depcount DESC,dep_count_q.repo_id ASC) AS repo_rank,
						dep_count_q.depcount AS value
						FROM
						(SELECT SUM(dl_q.dl_cnt) AS depcount,r.id AS repo_id
							FROM repositories r
							LEFT OUTER JOIN
								(SELECT DISTINCT
									dep_q.repo_id AS depending_repo_id,
									dep_q.do_repo_id AS depending_on_repo_id
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
										) AS dep_q
									INNER JOIN (
										SELECT DISTINCT FIRST_VALUE(id) OVER (PARTITION BY package_id ORDER BY created_at DESC,version_str DESC) AS last_v_id
										FROM package_versions pv
										WHERE pv.created_at <= :ref_time
										) AS lastv_q
									ON dep_q.version_id=lastv_q.last_v_id
								) AS deps_mat_q
						ON r.id=deps_mat_q.depending_on_repo_id
						INNER JOIN (SELECT p.repo_id,SUM(pvd.downloads) AS dl_cnt
								FROM package_version_downloads pvd
								INNER JOIN package_versions pv
								ON pvd.downloaded_at> :start_time AND pvd.downloaded_at<= :end_time
								AND pv.id=pvd.package_version
								INNER JOIN packages p
								ON p.id=pv.package_id
								GROUP BY p.repo_id
								) AS dl_q
						ON dl_q.repo_id=r.id OR dl_q.repo_id=deps_mat_q.depending_repo_id
						GROUP BY r.id) AS dep_count_q
			;'''


class RepoTransDepDLRank(RepoDLRank):
	def query(self):
		if self.db.db_type == 'postgres':
			return '''
				WITH RECURSIVE deps_mat_q (depending,depended)
							AS (SELECT DISTINCT
									dep_q.repo_id AS depending,
									dep_q.do_repo_id AS depended
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
										) AS dep_q
									INNER JOIN (
										SELECT DISTINCT FIRST_VALUE(id) OVER (PARTITION BY package_id ORDER BY created_at DESC,version_str DESC) AS last_v_id
										FROM package_versions pv
										WHERE pv.created_at <= %(ref_time)s
										) AS lastv_q
									ON dep_q.version_id=lastv_q.last_v_id
								ORDER BY depending,depended
								)
					, trans_deps_mat_q (depending,depended)
							AS (SELECT depending,depended FROM deps_mat_q dm
									UNION
								SELECT tdm.depending,dm.depended
								FROM deps_mat_q dm
								INNER JOIN trans_deps_mat_q tdm
								ON dm.depending=tdm.depended
								)
			SELECT dep_count_q.repo_id AS repo_id,
					RANK() OVER
						(ORDER BY dep_count_q.depcount DESC,dep_count_q.repo_id ASC) AS repo_rank,
						dep_count_q.depcount AS value
						FROM
						(SELECT SUM(dl_q.dl_cnt) AS depcount,r.id AS repo_id
							FROM repositories r
							LEFT OUTER JOIN trans_deps_mat_q
						ON r.id=trans_deps_mat_q.depended
						INNER JOIN (SELECT p.repo_id,SUM(pvd.downloads) AS dl_cnt
								FROM package_version_downloads pvd
								INNER JOIN package_versions pv
								ON pvd.downloaded_at> %(start_time)s AND pvd.downloaded_at<= %(end_time)s
								AND pv.id=pvd.package_version
								INNER JOIN packages p
								ON p.id=pv.package_id
								GROUP BY p.repo_id
								) AS dl_q
						ON dl_q.repo_id=r.id OR dl_q.repo_id=trans_deps_mat_q.depending
						GROUP BY r.id) AS dep_count_q
			;'''


		else:
			return '''
				WITH RECURSIVE deps_mat_q (depending,depended)
							AS (SELECT DISTINCT
									dep_q.repo_id AS depending,
									dep_q.do_repo_id AS depended
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
										) AS dep_q
									INNER JOIN (
										SELECT DISTINCT FIRST_VALUE(id) OVER (PARTITION BY package_id ORDER BY created_at DESC,version_str DESC) AS last_v_id
										FROM package_versions pv
										WHERE pv.created_at <= :ref_time
										) AS lastv_q
									ON dep_q.version_id=lastv_q.last_v_id
								ORDER BY depending,depended
								)
					, trans_deps_mat_q (depending,depended)
							AS (SELECT depending,depended FROM deps_mat_q dm
									UNION
								SELECT tdm.depending,dm.depended
								FROM deps_mat_q dm
								INNER JOIN trans_deps_mat_q tdm
								ON dm.depending=tdm.depended
								)
			SELECT dep_count_q.repo_id AS repo_id,
					RANK() OVER
						(ORDER BY dep_count_q.depcount DESC,dep_count_q.repo_id ASC) AS repo_rank,
						dep_count_q.depcount AS value
						FROM
						(SELECT SUM(dl_q.dl_cnt) AS depcount,r.id AS repo_id
							FROM repositories r
							LEFT OUTER JOIN trans_deps_mat_q
						ON r.id=trans_deps_mat_q.depended
						INNER JOIN (SELECT p.repo_id,SUM(pvd.downloads) AS dl_cnt
								FROM package_version_downloads pvd
								INNER JOIN package_versions pv
								ON pvd.downloaded_at> :start_time AND pvd.downloaded_at<= :end_time
								AND pv.id=pvd.package_version
								INNER JOIN packages p
								ON p.id=pv.package_id
								GROUP BY p.repo_id
								) AS dl_q
						ON dl_q.repo_id=r.id OR dl_q.repo_id=trans_deps_mat_q.depending
						GROUP BY r.id) AS dep_count_q
			;'''



# class RepoDLs(RepoDLRank):
	'''
	Wrapper around RepoDLRank to output only the ans_values (dl_vec). Artifact of previous design
	'''

	# def __init__(self,db,start_time=datetime.datetime(2010,1,1),end_time=datetime.datetime.now(),dl_correction=False,**kwargs):
	# 	Getter.__init__(self,db=db,**kwargs)
	# 	self.start_time = start_time
	# 	self.end_time = end_time
	# 	self.dl_correction = dl_correction

	# def query(self):
	# 	if self.db.db_type == 'postgres':
	# 		return '''
	# 			SELECT main_q.repo_id,repo_q.repo_rank,main_q.dl_cnt
	# 			FROM
	# 				(SELECT p.repo_id,SUM(pvd.downloads) AS dl_cnt
	# 				FROM package_version_downloads pvd
	# 				INNER JOIN package_versions pv
	# 				ON pvd.downloaded_at>%(start_time)s AND pvd.downloaded_at<=%(end_time)s
	# 				AND pv.id=pvd.package_version
	# 				INNER JOIN packages p
	# 				ON p.id=pv.package_id
	# 				GROUP BY p.repo_id
	# 				) AS main_q
	# 			INNER JOIN (
	# 				SELECT id AS repo_id,
	# 				RANK() OVER
	# 					(ORDER BY id) AS repo_rank
	# 				FROM repositories rr
	# 				) AS repo_q
	# 			ON main_q.repo_id=repo_q.repo_id
	# 		;'''
	# 	else:
	# 		return '''
	# 			SELECT main_q.repo_id,repo_q.repo_rank,main_q.dl_cnt
	# 			FROM
	# 				(SELECT p.repo_id,SUM(pvd.downloads) AS dl_cnt
	# 				FROM package_version_downloads pvd
	# 				INNER JOIN package_versions pv
	# 				ON pvd.downloaded_at>:start_time AND pvd.downloaded_at<=:end_time
	# 				AND pv.id=pvd.package_version
	# 				INNER JOIN packages p
	# 				ON p.id=pv.package_id
	# 				GROUP BY p.repo_id
	# 				) AS main_q
	# 			INNER JOIN (
	# 				SELECT id AS repo_id,
	# 				RANK() OVER
	# 					(ORDER BY id) AS repo_rank
	# 				FROM repositories rr
	# 				) AS repo_q
	# 			ON main_q.repo_id=repo_q.repo_id
	# 		;'''

	# def query_attributes(self):
	# 	return {
	# 	'start_time':self.start_time,
	# 	'end_time':self.end_time,
	# 	}

	# def parse_results(self,query_result):
	# 	return [(rk-1,val) for (r_id,rk,val) in query_result]


	# def get(self,db,deps_mat=None,**kwargs):
	# 	db.cursor.execute('SELECT COUNT(*) FROM repositories r;')
	# 	r_max = db.cursor.fetchone()[0]
	# 	db.cursor.execute(self.query(),self.query_attributes())
	# 	parsed_results = self.parse_results(query_result=db.cursor.fetchall())
	# 	ans = np.zeros(shape=(r_max,))
	# 	for rk,val in parsed_results:
	# 		ans[rk] = val
	# 	if self.dl_correction:
	# 		return self.correct_dls(prevec=ans,deps_mat=deps_mat)
	# 	else:
	# 		return ans

	# def get(self,db,**kwargs):
	# 	ans_direct,ans_indirect,ans_values = RepoDLRank.get(self,db=db,**kwargs)
	# 	return ans_values


class RepoDLCorrectionRank(RepoDLRank):
	'''
	Ranking (ans_d,ans_i,ans_v) with DL correction. Ranking is corrected in python
	'''

	def get(self,db,dl_correction=True,deps_mat=None,orig_id_rank=False,**kwargs):
		orig_direct,orig_indirect,dl_vec = RepoDLRank.get(self,db=db,**kwargs)

		ans_indirect = {}
		ans_direct = np.zeros(dl_vec.shape)
		ans_values = np.zeros(dl_vec.shape)

		if dl_correction:
			deps_mat = self.get_deps_mat(deps_mat=deps_mat)
			dl_vec = self.correct_dls(prevec=dl_vec,deps_mat=deps_mat)

		rk_val_list = sorted(list(zip(orig_direct,dl_vec)),key=lambda x: -x[1])

		for i,rdata in enumerate(rk_val_list):
			rk,val = rdata
			ans_direct[i] = rk
			ans_values[i] = val
			ans_indirect[rk] = i

		if not orig_id_rank:
			return ans_direct,ans_indirect,ans_values
		else:
			return self.reorder(prev_direct=ans_direct,prev_indirect=ans_indirect,prev_values=ans_values,db=db,**kwargs)

	def get_deps_mat(self,deps_mat):
		if deps_mat is None:
			deps_mat = RepoToRepoDeps(db=self.db,ref_time=self.ref_time).get_result()
		deps_mat = deps_mat.astype(np.bool_)
		return deps_mat

	def correct_dls(self,prevec,deps_mat=None):
		deps_mat = self.get_deps_mat(deps_mat=deps_mat)
		remaining_dls = prevec
		for seed_vec in self.get_list_seedvecs(deps_mat=deps_mat):
			concerned_packages = self.get_concerned_packages(deps_mat=deps_mat,init_vec=seed_vec)
			# check one seedvec element
			# check concerned packages for the given package
			#
			# print(
			# 	(concerned_packages.multiply(np.ones((seed_vec.size,)))*seed_vec>0).sum(),
			# 	(concerned_packages.multiply(seed_vec)*np.ones((seed_vec.size,))>0).sum(),
			# 	(seed_vec>0).sum())
			# remaining_dls = remaining_dls - concerned_packages.multiply(seed_vec) * remaining_dls ####
			remaining_dls = remaining_dls - concerned_packages.multiply(seed_vec) * remaining_dls ####
			remaining_dls[remaining_dls<0] = 0
		return remaining_dls

	def get_list_seedvecs(self,deps_mat):
		'''
		gets a list of binary vectors corresponding to branching order:
		first vector is roots of the deps network
		second vector is roots of the deps network when removing first vec
		n th vector is roots of deps net when removing all vectors up to (n-1)th
		stopping iterator when all net covered (last item still has non zero values)
		'''
		ndeps_mat = deps_mat.astype(np.bool_).transpose()
		cols = np.asarray(ndeps_mat.sum(axis=0)).flatten()
		seed_vec = (cols==0)
		cumul_seed_vec = seed_vec
		while seed_vec.any():
			yield seed_vec
			cols = ndeps_mat*(1-cumul_seed_vec) #####
			new_cumul_seed_vec = (cols==0)
			seed_vec = np.logical_and(new_cumul_seed_vec,1-cumul_seed_vec)
			cumul_seed_vec = new_cumul_seed_vec


	def get_concerned_packages(self,init_vec,deps_mat):
		'''
		returns a bool matrix source_packages,concerned_packages.
		concerned packages are all upstream dependencies of a given package.
		'''
		ans = np.zeros((init_vec.size,deps_mat.shape[0],),dtype=np.bool_)
		p = sparse.diags(init_vec.astype(np.int64)).tocsr().astype(np.bool_)
		ndeps_mat = deps_mat.astype(np.bool_).transpose()
		old_size = p.data.size
		init = True
		while init or p.data.size != old_size:
			init = False
			old_size = p.data.size
			p = ndeps_mat*p + p
		return p


class RepoRandomRank(RepoRankGetter):
	def __init__(self,seed=None,**kwargs):
		RepoRankGetter.__init__(self,**kwargs)
		if seed is None:
			self.seed = np.random.random()
		else:
			self.seed = seed
		#Seed usage not implemented

	def query(self):
		return '''SELECT id AS repo_id,
					RANK() OVER
						(ORDER BY RANDOM(),id) AS repo_rank,
					0. AS value
					FROM repositories rr
			;'''


	def query_attributes(self):
		return {'seed':self.seed,
		}


class PackageAgeRank(RepoRankGetter):

	def __init__(self,ref_time=datetime.datetime.now(),**kwargs):
		RepoRankGetter.__init__(self,**kwargs)
		self.ref_time = ref_time

	def query(self):
		if self.db.db_type == 'postgres':
			return '''SELECT r.id AS repo_id,
					RANK() OVER
						(ORDER BY pp.created_at,r.id) AS repo_rank,
					DATE_PART('day',%(ref_time)s-pp.created_at) AS value
					FROM repositories r
					LEFT OUTER JOIN (SELECT p.repo_id,MIN(p.created_at) AS created_at FROM packages p GROUP BY p.repo_id) AS pp
					ON pp.repo_id=r.id
				;'''
		else:
			return '''SELECT r.id AS repo_id,
					RANK() OVER
						(ORDER BY pp.created_at,r.id) AS repo_rank,
					julianday(:ref_time)-julianday(pp.created_at) AS value
					FROM repositories r
					LEFT OUTER JOIN (SELECT p.repo_id,MIN(p.created_at) AS created_at FROM packages p GROUP BY p.repo_id) AS pp
					ON pp.repo_id=r.id
				;'''


	def query_attributes(self):
		return {'ref_time':self.ref_time,
		}



class RepoDLCommitRank(RepoDLRank):
	'''
	Ranking by DLs/Commits on the specified period. Coalescing 0 commits to 1.
	'''
	def query(self):
		if self.db.db_type == 'postgres':
			return '''
				SELECT r.id,
				RANK() OVER
						(ORDER BY COALESCE(main_q.dl_cnt,0)*1./COALESCE(commit_q.commit_cnt,1) DESC,r.id ASC) AS repo_rank,
					COALESCE(main_q.dl_cnt,0)*1./COALESCE(commit_q.commit_cnt,1) AS value
				FROM repositories r
				LEFT OUTER JOIN
					(SELECT p.repo_id,SUM(pvd.downloads) AS dl_cnt
					FROM package_version_downloads pvd
					INNER JOIN package_versions pv
					ON pvd.downloaded_at>%(start_time)s AND pvd.downloaded_at<=%(end_time)s
					AND pv.id=pvd.package_version
					INNER JOIN packages p
					ON p.id=pv.package_id
					GROUP BY p.repo_id
					) AS main_q
				ON main_q.repo_id=r.id
				LEFT OUTER JOIN
					(SELECT c.repo_id,COUNT(*) AS commit_cnt
					FROM commits c
					WHERE c.created_at>%(start_time)s AND c.created_at<=%(end_time)s
					GROUP BY c.repo_id
					) AS commit_q
				ON commit_q.repo_id=r.id
			;'''
		else:
			return '''
				SELECT r.id,
				RANK() OVER
						(ORDER BY COALESCE(main_q.dl_cnt,0)*1./COALESCE(commit_q.commit_cnt,1) DESC,r.id ASC) AS repo_rank,
					COALESCE(main_q.dl_cnt,0)*1./COALESCE(commit_q.commit_cnt,1) AS value
				FROM repositories r
				LEFT OUTER JOIN
					(SELECT p.repo_id,SUM(pvd.downloads) AS dl_cnt
					FROM package_version_downloads pvd
					INNER JOIN package_versions pv
					ON pvd.downloaded_at>:start_time AND pvd.downloaded_at<=:end_time
					AND pv.id=pvd.package_version
					INNER JOIN packages p
					ON p.id=pv.package_id
					GROUP BY p.repo_id
					) AS main_q
				ON main_q.repo_id=r.id
				LEFT OUTER JOIN
					(SELECT c.repo_id,COUNT(*) AS commit_cnt
					FROM commits c
					WHERE c.created_at>:start_time AND c.created_at<=:end_time
					GROUP BY c.repo_id
					) AS commit_q
				ON commit_q.repo_id=r.id
			;'''

class RepoCommitRank(RepoDLRank):
	def query(self):
		if self.db.db_type == 'postgres':
			return '''
				SELECT r.id,
				RANK() OVER
						(ORDER BY COALESCE(commit_q.commit_cnt,0) DESC,r.id ASC) AS repo_rank,
					COALESCE(commit_q.commit_cnt,0) AS value
				FROM repositories r
				LEFT OUTER JOIN
					(SELECT c.repo_id,COUNT(*) AS commit_cnt
					FROM commits c
					WHERE c.created_at>%(start_time)s AND c.created_at<=%(end_time)s
					GROUP BY c.repo_id
					) AS commit_q
				ON commit_q.repo_id=r.id
			;'''
		else:
			return '''
				SELECT r.id,
				RANK() OVER
						(ORDER BY COALESCE(commit_q.commit_cnt,0) DESC,r.id ASC) AS repo_rank,
					COALESCE(commit_q.commit_cnt,0) AS value
				FROM repositories r
				LEFT OUTER JOIN
					(SELECT c.repo_id,COUNT(*) AS commit_cnt
					FROM commits c
					WHERE c.created_at>:start_time AND c.created_at<=:end_time
					GROUP BY c.repo_id
					) AS commit_q
				ON commit_q.repo_id=r.id
			;'''

class RepoMaxCommitRank(RepoCommitRank):
	def query(self):
		if self.db.db_type == 'postgres':
			return '''
				SELECT r.id,
				RANK() OVER
						(ORDER BY COALESCE(commit_q.commit_cnt,0) DESC,r.id ASC) AS repo_rank,
					COALESCE(commit_q.commit_cnt,0) AS value
				FROM repositories r
				LEFT OUTER JOIN
					(SELECT repo_id,MAX(commit_cnt) AS commit_cnt FROM
						(SELECT c.repo_id,i.user_id,COUNT(*) AS commit_cnt
						FROM commits c
						INNER JOIN identities i
						ON c.created_at>%(start_time)s AND c.created_at<=%(end_time)s AND i.id=c.author_id AND NOT i.is_bot
						GROUP BY c.repo_id,i.user_id
						) AS inner_commit_q
						GROUP BY repo_id
					) AS commit_q
				ON commit_q.repo_id=r.id
			;'''
		else:
			return '''
				SELECT r.id,
				RANK() OVER
						(ORDER BY COALESCE(commit_q.commit_cnt,0) DESC,r.id ASC) AS repo_rank,
					COALESCE(commit_q.commit_cnt,0) AS value
				FROM repositories r
				LEFT OUTER JOIN
					(SELECT repo_id,MAX(commit_cnt) AS commit_cnt FROM
						(SELECT c.repo_id,i.user_id,COUNT(*) AS commit_cnt
						FROM commits c
						INNER JOIN identities i
						ON c.created_at>:start_time AND c.created_at<=:end_time AND i.id=c.author_id AND NOT i.is_bot
						GROUP BY c.repo_id,i.user_id
						) AS inner_commit_q
						GROUP BY repo_id
					) AS commit_q
				ON commit_q.repo_id=r.id
			;'''


# class RepoImpactRank(RepoRankGetter):
# 	def __init__(self,sr_getter_class=None,srgetter_kwargs=None,start_time=None,end_time=None,**kwargs):
# 		if sr_getter_class is None: # cannot top-level import SR_getters, because there would be cyclical imports
# 			from .SR_getters import SRGetter
# 			sr_getter_class = SRGetter
# 		if srgetter_kwargs is None:
# 			srgetter_kwargs = {}
# 		RepoRankGetter.__init__(self,**kwargs)
# 		srgetter_kwargs = copy.deepcopy(srgetter_kwargs)
# 		if start_time is None:
# 			if not 'start_time' in srgetter_kwargs.keys():
# 				srgetter_kwargs['start_time'] = datetime.datetime(2000,1,1)
# 		else:
# 			if srgetter_kwargs['start_time'] != start_time:
# 				self.logger.info('In {}, start_time provided 2 times. Priority to explicit arg over key in srgetter_kwargs.'.format(self.__class__.__name__.split('.')[-1]))
# 			srgetter_kwargs['start_time'] = start_time
# 		self.start_time = srgetter_kwargs['start_time']
# 		if end_time is None:
# 			if not 'end_time' in srgetter_kwargs.keys():
# 				srgetter_kwargs['end_time'] = datetime.datetime.now()
# 		else:
# 			if srgetter_kwargs['end_time'] != end_time:
# 				self.logger.info('In {}, end_time provided 2 times. Priority to explicit arg over key in srgetter_kwargs.'.format(self.__class__.__name__.split('.')[-1]))
# 			srgetter_kwargs['end_time'] = end_time
# 		self.end_time = srgetter_kwargs['end_time']


# 		self.sr_getter = sr_getter_class(db=self.db,**srgetter_kwargs)

# 	# def query(self):
# 	# 	return '''SELECT id AS repo_id,
# 	# 				RANK() OVER
# 	# 					(ORDER BY id) AS repo_rank,
# 	# 				0. AS value
# 	# 				FROM repositories rr
# 	# 		;'''


# 	# def query_attributes(self):
# 	# 	return {
# 	# 	}

# 	# def parse_results(self,query_result):
# 	# 	return [(r_id,rk-1,val) for (r_id,rk,val) in query_result]

# 	# def get_size_max(self,db):
# 	# 	db.cursor.execute('SELECT COUNT(*) FROM repositories r;')
# 	# 	r_max = db.cursor.fetchone()[0]
# 	# 	return r_max

# 	def preprocess_result(self):
# 		return self.sr_getter.get_result().sum(axis=1)

# 	def get(self,db,orig_id_rank=False,**kwargs):
# 		# db.cursor.execute(self.query(),self.query_attributes())
# 		# parsed_results = self.parse_results(query_result=db.cursor.fetchall())
# 		results = self.preprocess_result()
# 		r_max = results.size
# 		ans_direct = np.zeros(shape=(r_max,),dtype=np.int64)
# 		ans_values = np.zeros(shape=(r_max,),dtype=np.float64)
# 		ans_indirect = {}
# 		sorted_results = sorted(enumerate(results),key=lambda x:-x[1])
# 		r_direct,r_indirect,r_values = RepoRankGetter(db=db).get(db=db,orig_id_rank=False,**kwargs)
# 		for rk,res in enumerate(sorted_results):
# 			id_rk,val = res
# 			r_id = r_direct[id_rk]

# 			ans_indirect[r_id] = rk
# 			ans_direct[rk] = r_id
# 			ans_values[rk] = val

# 		if not orig_id_rank:
# 			return ans_direct,ans_indirect,ans_values
# 		else:
# 			return self.reorder(prev_direct=ans_direct,prev_indirect=ans_indirect,prev_values=ans_values,db=db,**kwargs)

# class RepoImpactCommitRank(RepoImpactRank):
# 	def __init__(self,commit_min=1,**kwargs):
# 		RepoImpactRank.__init__(self,**kwargs)
# 		self.commit_min = commit_min

# 	def preprocess_result(self):
# 		impact = RepoImpactRank.preprocess_result(self)
# 		if self.db.db_type == 'postgres':
# 			self.db.cursor.execute('''
# 				SELECT COUNT(*),r.id,r.rank-1 FROM
# 				(SELECT id, RANK() OVER (ORDER BY id) AS rank FROM repositories) AS r
# 				INNER JOIN commits c
# 				ON c.repo_id=r.id
# 				AND %(start_time)s <= c.created_at AND c.created_at < %(end_time)s
# 				AND c.repo_id IS NOT NULL
# 				GROUP BY r.id,r.rank
# 				''',{'start_time':self.start_time,'end_time':self.end_time})
# 		else:
# 			self.db.cursor.execute('''
# 				SELECT COUNT(*),r.id,r.rank-1 FROM
# 				(SELECT id, RANK() OVER (ORDER BY id) AS rank FROM repositories) AS r
# 				INNER JOIN commits c
# 				ON c.repo_id=r.id
# 				AND :start_time <= c.created_at AND c.created_at < :end_time
# 				AND c.repo_id IS NOT NULL
# 				GROUP BY r.id,r.rank
# 				''',{'start_time':self.start_time,'end_time':self.end_time})
# 		commits = np.zeros(impact.shape,dtype=np.float64)
# 		for val,rid,rk in self.db.cursor.fetchall():
# 			commits[rk] = val
# 		commits[commits<self.commit_min] = self.commit_min
# 		return impact/commits



# class RepoNoDevsModeRank(RepoImpactRank):
# 	'''
# 	The scenarios in SR are 'a repo fails' and not 'a dev fails'
# 	'''
# 	def __init__(self,srgetter_kwargs=None,**kwargs):
# 		if srgetter_kwargs is None:
# 			srgetter_kwargs = {}
# 		else:
# 			srgetter_kwargs = copy.deepcopy(srgetter_kwargs)
# 		srgetter_kwargs['dev_mode'] = False
# 		RepoImpactRank.__init__(self,srgetter_kwargs=srgetter_kwargs,**kwargs)

# 	def preprocess_result(self):
# 		ans = np.asarray(self.sr_getter.get_result().sum(axis=0)).flatten()
# 		return ans
