from . import Getter
from . import edge_getters
from . import SR_getters

from scipy import sparse
import datetime
import numpy as np
import pandas as pd
import sqlite3
import json
import copy

import multiprocessing as mp
import psutil

class PolicyGetter(Getter):
	def __init__(self,db,ranks,nb_devs=10,no_checks=False,sr_getter_class=SR_getters.SRGetter,start_time=datetime.datetime(2010,1,1),end_time=datetime.datetime.now(),**kwargs):
		Getter.__init__(self,db=db,**kwargs)
		if len(ranks) == 3 and isinstance(ranks[1],dict):
			self.ranks = ranks[0]
		else:
			self.ranks = ranks
		self.no_checks = no_checks
		if not self.no_checks:
			self.check_ranks()
		self.nb_devs = nb_devs
		self.sr_getter_class = sr_getter_class
		self.start_time = start_time
		self.end_time = end_time
		self.sr_getter = self.sr_getter_class(db=db,start_time=self.start_time,end_time=self.end_time,**kwargs)

	def check_ranks(self,db=None):
		if db is None:
			db = self.db
		db.cursor.execute('SELECT COUNT(*) FROM repositories;')
		nb_repos = self.db.cursor.fetchone()[0]
		assert len(self.ranks) ==  nb_repos, 'Error: Ranks ({}) not matching number of repositories ({})'.format(len(self.ranks),nb_repos)

	def set_new_ranks(self,ranks):
		self.ranks = ranks
		del self.sr_getters.devs_mat
		if not self.no_checks:
			self.check_ranks()

	def set_modified_devs_mat(self):
		if not hasattr(self.sr_getter,'devs_mat'):
			self.sr_getter.devs_getter = edge_getters.DevToRepoAddMax(db=self.db,repo_list=self.ranks[:self.nb_devs],start_time=self.start_time,end_time=self.end_time)

	def get(self,db,**kwargs):
		if not self.no_checks:
			self.check_ranks(db=db)
		if self.nb_devs > 0:
			self.set_modified_devs_mat()
		self.result = self.sr_getter.get(db=db,**kwargs)
		return self.result

class CommitRatePolicyGetter(PolicyGetter):
	def __init__(self,daily_commits=5./7.,**kwargs):
		self.daily_commits = daily_commits
		PolicyGetter.__init__(self,**kwargs)


	def set_modified_devs_mat(self):
		if not hasattr(self.sr_getter,'devs_mat'):
			self.sr_getter.devs_getter = edge_getters.DevToRepoAddDailyCommits(db=self.db,repo_list=self.ranks[:self.nb_devs],daily_commits=self.daily_commits,start_time=self.start_time,end_time=self.end_time)


class BatchPolicyGetter(Getter):
	'''
	Wrapper around Policy Getters to batch compute for several nb_devs values and save/load results
	Some methods are really similar to VaccRankGetter, one could consider an implementation where they are shared
	'''
	def __init__(self,nb_devs_list,policy_getter_class=CommitRatePolicyGetter,policy_getter_args=None,computation_results_db=None,rank_label='(no rank label)',**kwargs):
		Getter.__init__(self,**kwargs)
		self.policy_getter_args = copy.deepcopy(policy_getter_args)
		if 'nb_devs' in self.policy_getter_args.keys():
			del self.policy_getter_args['nb_devs']
		self.policy_getter_class = policy_getter_class
		self.nb_devs_list = list(nb_devs_list)
		self.rank_label = rank_label

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
			self.cr_db_cursor.execute('''CREATE TABLE IF NOT EXISTS policy_values(
										nb_devs INTEGER PRIMARY KEY,
										values_dict TEXT,
										inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);''')
			self.cr_db_connection.commit()

	def update_cr_db_results(self):
		if self.computation_results_db is None:
			self.cr_db_results = {}
		else:
			self.cr_db_cursor.execute('SELECT nb_devs,values_dict FROM policy_values;')
			self.cr_db_results = {ndev:json.loads(val) for ndev,val in self.cr_db_cursor.fetchall()}

	def submit_cr_db_result(self,ndev,val):
		if self.computation_results_db is not None:
			self.cr_db_cursor.execute('INSERT OR IGNORE INTO policy_values(nb_devs,values_dict) VALUES(:ndev,:val);',{'ndev':ndev,'val':json.dumps(val)})
			self.cr_db_connection.commit()

	def process_results(self,sr_res,baseline=False):
		sum_ax1 = sr_res.sum(axis=1)
		return {'umax':sum_ax1.max(),
				'umean':sum_ax1.mean(),
				'globalsum':sum_ax1.sum(),
				}

	# def results_sort_function(self,res):
	# 	'''
	# 	Getting an order on res_dict items
	# 	'''
	# 	return res['globalsum']

	def get(self,db,**kwargs):
		'''
		returns a dict of nbdev:processedresult_dict
		'''
		self.update_cr_db_results()
		return self.ndev_iterations(db=db,nb_devs_list=self.nb_devs_list)

	def ndev_iterations(self,db,nb_devs_list=None,prefix=''):
		results = {}
		if nb_devs_list is None:
			nb_devs_list = self.nb_devs_list
		for i,nb_devs in enumerate(nb_devs_list):
			try:
				results[nb_devs] = copy.deepcopy(self.cr_db_results[nb_devs])
			except KeyError:
				self.logger.info('{}Computing {} for {} with nb_devs={} ({}/{})'.format(prefix,self.policy_getter_class.__name__.split('.')[-1],self.rank_label,nb_devs,i+1,len(nb_devs_list)))
				pg = self.policy_getter_class(db=db,nb_devs=nb_devs,**self.policy_getter_args)
				val = self.process_results(pg.get_result())
				self.submit_cr_db_result(ndev=nb_devs,val=val)
				results[nb_devs] = copy.deepcopy(val)

		return results


class ParallelBatchPolicyGetter(BatchPolicyGetter):
	def __init__(self,workers=None,**kwargs):
		if workers is None:
			self.workers = len(psutil.Process().cpu_affinity())
		else:
			self.workers = workers
		BatchPolicyGetter.__init__(self,**kwargs)

	def split_ndev_list(self,ndev_list):
		if self.workers >= len(ndev_list):
			return [[r] for r in ndev_list ]
		else:
			ans = []
			base_nb = int(len(ndev_list)/self.workers)
			remaining = len(ndev_list) - self.workers * base_nb
			current_idx = 0
			for i in range(self.workers):
				if i<remaining:
					ans.append(ndev_list[current_idx:current_idx + base_nb + 1])
					current_idx = current_idx + base_nb + 1
				else:
					ans.append(ndev_list[current_idx:current_idx + base_nb])
					current_idx = current_idx + base_nb
			return ans

	def ndev_iterations(self,db,nb_devs_list=None,prefix=''):
		results = {}
		manager = mp.Manager()
		ans_dict = manager.dict()
		ndev_list = []
		for n in self.nb_devs_list:
			if n in self.cr_db_results.keys():
				results[n] = self.cr_db_results[n]
			else:
				ndev_list.append(n)
		batch_list = self.split_ndev_list(ndev_list)

		def f(i,ans_d):
			# temp_db = self.db # connection duplication on pickling handled in Database class
			temp_db = self.db.__class__(**self.db.db_conninfo) # connection duplication
			try:
				ans_d[i] = BatchPolicyGetter.ndev_iterations(self,nb_devs_list=batch_list[i],prefix='{} Process {}{}/{}:'.format(prefix,' '*( len(str(len(batch_list)))-len(str(i+1)) ),i+1,len(batch_list)),db=temp_db)
			finally:
				temp_db.connection.close()
		# make processes
		process_pool = [mp.Process(target=f,args=(i,ans_dict)) for i in range(len(batch_list))]
		for p in process_pool:
			p.start()
		for p in process_pool:
			p.join()
		# collect and combine results
		for i in range(len(batch_list)):
			results.update(ans_dict[i])

		return results
