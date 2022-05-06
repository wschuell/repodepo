from . import Getter
from . import rank_getters
from . import SR_getters

from scipy import sparse
import datetime
import copy
import numpy as np
import json
import sqlite3
import pandas as pd

import multiprocessing as mp
import psutil

class VaccinationRankGetter(rank_getters.RepoRankGetter):

	def __init__(self,sr_getter_class=SR_getters.SRGetter,limit_repos=None,offset_repos=0,srgetter_kwargs={},computation_results_db=None,**kwargs):
		rank_getters.RepoRankGetter.__init__(self,**kwargs)
		self.sr_getter_class = sr_getter_class
		self.sr_getter = self.sr_getter_class(db=self.db,**srgetter_kwargs)
		self.limit_repos = limit_repos
		self.offset_repos = offset_repos
		if computation_results_db is None:
			self.computation_results_db = None
			self.cr_db_connection = None
			self.cr_db_cursor = None
		else:
			self.computation_results_db = computation_results_db
			self.connect_cr_db()

	def connect_cr_db(self):
		if self.computation_results_db is not None:
			self.cr_db_connection = sqlite3.connect(self.computation_results_db)
			self.cr_db_cursor = self.cr_db_connection.cursor()
			self.cr_db_cursor.execute('''CREATE TABLE IF NOT EXISTS vacc_values(
										repo_rank INTEGER PRIMARY KEY,
										values_dict TEXT,
										inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);''')
			self.cr_db_connection.commit()

	def update_cr_db_results(self):
		if self.computation_results_db is None:
			self.cr_db_results = {}
		else:
			self.cr_db_cursor.execute('SELECT repo_rank,values_dict FROM vacc_values;')
			self.cr_db_results = {rk:json.loads(val) for rk,val in self.cr_db_cursor.fetchall()}

	def submit_cr_db_result(self,rk,val):
		if self.computation_results_db is not None:
			self.cr_db_cursor.execute('INSERT OR IGNORE INTO vacc_values(repo_rank,values_dict) VALUES(:rk,:val);',{'rk':rk,'val':json.dumps(val)})
			self.cr_db_connection.commit()

	def get(self,db,**kwargs):
		ranks_direct,ranks_indirect,ranks_values = rank_getters.RepoRankGetter.get(self,db=db,**kwargs)
		# self.results = []
		self.baseline_results = self.process_results(self.sr_getter.get_result(),baseline=True)
		self.update_cr_db_results()
		if self.limit_repos is None:
			repo_rank_gen = range(self.offset_repos,ranks_direct.size-self.offset_repos)
		else:
			repo_rank_gen = range(self.offset_repos,min(self.offset_repos+self.limit_repos,ranks_direct.size))
		repo_rank_list = []
		results = []
		for v_rk in repo_rank_gen:
			if v_rk not in self.cr_db_results.keys():
				repo_rank_list.append(v_rk)
			else:
				results.append((v_rk,self.cr_db_results[v_rk]))
		self.results = results + self.repo_iterations(repo_rank_list=repo_rank_list)
		# for v_rk in range(ranks_direct.size):
		# 	self.logger.info('Vaccination repository {}/{}'.format(v_rk+1,ranks_direct.size))
		# 	self.sr_getter.set_vaccinated_repos(vaccinated_repo_ranks=[v_rk])
		# 	sr_res = self.sr_getter.get_result()
		# 	self.results.append((v_rk,self.process_results(sr_res)))
		sorted_vrk = sorted(self.results,key=lambda vr: (self.results_sort_function(vr[1]),vr[0]))
		ans_direct = np.zeros(ranks_direct.shape)
		ans_values = np.zeros(ranks_direct.shape)
		ans_indirect = {}
		for i,sv in enumerate(sorted_vrk):
			v_id = ranks_direct[sv[0]]
			ans_direct[i] = v_id
			ans_indirect[v_id] = i
			ans_values[i] = self.results_sort_function(sv[1])
		return ans_direct,ans_indirect,ans_values

	def repo_iterations(self,repo_rank_list,prefix='',sr_getter=None):
		if sr_getter is None:
			sr_getter = self.sr_getter
		results = []

		for i,v_rk in enumerate(repo_rank_list):
			if v_rk in self.cr_db_results.keys():
				val = self.cr_db_results[v_rk]
			else:
				self.logger.info('{}{} repository {}/{} {}'.format(prefix,self.__class__.__name__.split('.')[-1],i+1,len(repo_rank_list),v_rk+1,))
				self.modify_srgetter(sr_getter=sr_getter,v_rk=v_rk)
				sr_res = sr_getter.get_result()
				val = self.process_results(sr_res)
				self.submit_cr_db_result(rk=v_rk,val=val)
			results.append((v_rk,val))
		return results

	def modify_srgetter(self,sr_getter,v_rk):
		sr_getter.set_vaccinated_repos(vaccinated_repo_ranks=[v_rk])

	def process_results(self,sr_res,baseline=False):
		'''
		What measures are to be extracted from the sparse matrix sr_res (dim scenario*repos)
		The baseline (without any vaccination) results can also be used (self.baseline_results)
		The baseline (without any vaccination) matrix can also be used (self.baseline_matrix)
		'''
		sum_ax1 = sr_res.sum(axis=1)
		ans = {'umax':sum_ax1.max(),
				'umean':sum_ax1.mean(),
				'globalsum':sum_ax1.sum(),
				}
		if not baseline:
			ans.update({'umax_diff':self.baseline_results['umax']-ans['umax'],
				'umean_diff':self.baseline_results['umean']-ans['umean'],
				'globalsum_diff':self.baseline_results['globalsum']-ans['globalsum'],
				})
		else:
			ans.update({'umax_diff':0.,
				'umean_diff':0.,
				'globalsum_diff':0.,
				})

		return ans

	def results_sort_function(self,res):
		'''
		Getting an order on res_dict items
		'''
		return -res['globalsum_diff']
		# return -res['globalsum']


class ParallelVaccRankGetter(VaccinationRankGetter):
	'''
	Instead of for loop on each repo, parallelizing in nb_workers processes, each with the same amount of repos
	'''
	def __init__(self,workers=None,**kwargs):
		VaccinationRankGetter.__init__(self,**kwargs)
		if workers is None:
			self.workers = len(psutil.Process().cpu_affinity())
		else:
			self.workers = workers

	def split_rk_list(self,repo_rank_list):
		if self.workers >= len(repo_rank_list):
			return [[r] for r in repo_rank_list ]
		else:
			ans = []
			base_nb = int(len(repo_rank_list)/self.workers)
			remaining = len(repo_rank_list) - self.workers * base_nb
			current_idx = 0
			for i in range(self.workers):
				if i<remaining:
					ans.append(repo_rank_list[current_idx:current_idx + base_nb + 1])
					current_idx = current_idx + base_nb + 1
				else:
					ans.append(repo_rank_list[current_idx:current_idx + base_nb])
					current_idx = current_idx + base_nb
			return ans

	def repo_iterations(self,repo_rank_list,prefix='',sr_getter=None):
		if sr_getter is None:
			sr_getter = self.sr_getter
		results = []
		manager = mp.Manager()
		ans_dict = manager.dict()
		rk_list = self.split_rk_list(repo_rank_list)
		# copy sr_getter
		# sr_getter_l = [self.sr_getter.copy() for _ in range(len(rk_list))]
		self.logger.info('Copying SR getters')
		self.sr_getter.get_all()
		sr_getter_l = [copy.deepcopy(self.sr_getter) for _ in range(len(rk_list))]
		self.logger.info('Copied SR getters')
		# define launch processes on repo_iter func
		def f(i,ans_d):
			ans_d[i] = VaccinationRankGetter.repo_iterations(self,repo_rank_list=rk_list[i],prefix='Process {}{}/{}:'.format(' '*( len(str(len(rk_list)))-len(str(i+1)) ),i+1,len(rk_list)),sr_getter=sr_getter_l[i])
		# make processes
		process_pool = [mp.Process(target=f,args=(i,ans_dict)) for i in range(len(rk_list))]
		for p in process_pool:
			p.start()
		for p in process_pool:
			p.join()
		# collect and combine results
		for i in range(len(rk_list)):
			results += ans_dict[i]

		return results

class EfficientVaccRankGetter(VaccinationRankGetter):
	'''
	Instead of for loop on each repo, combining all cascades in one
	'''
	def __init__(self,grouping_size=100,**kwargs):
		VaccinationRankGetter.__init__(self,**kwargs)
		self.grouping_size = grouping_size
		if self.computation_results_db is not None:
			raise NotImplementedError('Saving results to SQLite for EfficientVaccRankGetter is not implemented yet')

	def get(self,db,**kwargs):
		

		ranks_direct,ranks_indirect,ranks_values = rank_getters.RepoRankGetter.get(self,db=db,**kwargs)
		self.results = []
		self.baseline_matrix = self.sr_getter.get_result()
		self.baseline_results = self.process_results(self.baseline_matrix,baseline=True)
		
		group_boundaries = []
		current_rk = 0
		while current_rk<=ranks_direct.size-1:
			new_upper = min(current_rk+self.grouping_size-1,ranks_direct.size-1)
			group_boundaries.append((current_rk,new_upper))
			current_rk = new_upper+1

		self.sr_getter.parallelize_repo_space(factor=self.grouping_size)

		for v_rk_min,v_rk_max in group_boundaries:
			# print(v_rk_min,v_rk_max)
			# if v_rk_max>200:
			# 	raise ValueError()
			self.sr_getter.set_vaccinated_repos(vaccinated_repo_ranks=[v_rk+i*ranks_direct.size for i,v_rk in enumerate(range(v_rk_min,v_rk_max+1))])
			sr_res = self.sr_getter.get_result().tocsc()
			for i,v_rk in enumerate(range(v_rk_min,v_rk_max+1)):
				sr_res_extract = self.extract_sub_sr_res(sr_res=sr_res,i=i,n=ranks_direct.size)
				self.results.append((v_rk,self.process_results(sr_res_extract)))

		
		sorted_vrk = sorted(self.results,key=lambda vr: (self.results_sort_function(vr[1]),vr[0]))
		ans_direct = np.zeros(ranks_direct.shape)
		ans_values = np.zeros(ranks_direct.shape)
		ans_indirect = {}
		for i,sv in enumerate(sorted_vrk):
			v_id = ranks_direct[sv[0]]
			ans_direct[i] = v_id
			ans_values[i] = self.results_sort_function(sv[1])
			ans_indirect[v_id] = i
		return ans_direct,ans_indirect,ans_values

	def extract_sub_sr_res(self,sr_res,i,n):
		'''
		extract i-th slice of sr_res when parallelized
		'''
		return sr_res[:,n*i:n*(i+1)]


class SemiGreedyEffectRank(ParallelVaccRankGetter):
	def __init__(self,daily_commits=5./7.,**kwargs):
		self.daily_commits = daily_commits
		ParallelVaccRankGetter.__init__(self,**kwargs)

	def modify_srgetter(self,sr_getter,v_rk):
		if not hasattr(self,'complete_devs_mat'):
			self.complete_devs_mat = copy.deepcopy(sr_getter.devs_mat)
		sr_getter.devs_mat = self.complete_devs_mat.copy()
		nb_commits = self.daily_commits*(sr_getter.end_time-sr_getter.start_time).days
		val = nb_commits*1./max(sr_getter.repo_commits[v_rk],1)
		nz = sr_getter.devs_mat.nonzero()
		for i in nz[1][nz[0]==v_rk]:
			sr_getter.devs_mat[v_rk,i] = max(sr_getter.devs_mat[v_rk,i] - val,0.)
		sr_getter.devs_mat.eliminate_zeros()
		del sr_getter.dev_contrib


	def connect_cr_db(self):
		if self.computation_results_db is not None:
			self.cr_db_connection = sqlite3.connect(self.computation_results_db)
			self.cr_db_cursor = self.cr_db_connection.cursor()
			self.cr_db_cursor.execute('''CREATE TABLE IF NOT EXISTS semigreedy_values(
										repo_rank INTEGER PRIMARY KEY,
										values_dict TEXT,
										inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);''')
			self.cr_db_connection.commit()

	def update_cr_db_results(self):
		if self.computation_results_db is None:
			self.cr_db_results = {}
		else:
			self.cr_db_cursor.execute('SELECT repo_rank,values_dict FROM semigreedy_values;')
			self.cr_db_results = {rk:json.loads(val) for rk,val in self.cr_db_cursor.fetchall()}

	def submit_cr_db_result(self,rk,val):
		if self.computation_results_db is not None:
			self.cr_db_cursor.execute('INSERT OR IGNORE INTO semigreedy_values(repo_rank,values_dict) VALUES(:rk,:val);',{'rk':rk,'val':json.dumps(val)})
			self.cr_db_connection.commit()
