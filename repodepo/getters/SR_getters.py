from . import Getter
from . import edge_getters
from . import rank_getters

from scipy import sparse
import datetime
import copy
import scipy
import numpy as np
import pandas as pd


class SRGetter(Getter):
	'''
	output of get_result: sparse matrix dev to repo, with impact value
	dev_mode by default (cascades in developer space; if set to False in repo_space)
	'''
	def __init__(self,start_time=datetime.datetime(2010,1,1),
				end_time=datetime.datetime.now(),
				dl_weights=True,
				norm_dl=False,
				dl_correction=False,
				iter_max=10**4,
				stationary_devcontrib=True,
				dev_cd_power=0.5,
				deps_cd_power=0.5,
				dev_mode=True,
				**kwargs):
		Getter.__init__(self,**kwargs)
		self.dev_mode = dev_mode
		self.stationary_devcontrib = stationary_devcontrib
		self.start_time = start_time
		self.end_time = end_time
		self.dev_cd_power = dev_cd_power
		self.deps_cd_power = deps_cd_power
		self.iter_max = iter_max
		self.norm_dl = norm_dl
		self.dl_weights = dl_weights
		if dl_correction:
			self.dl_getter = rank_getters.RepoDLCorrectionRank(db=self.db,start_time=self.start_time,end_time=self.end_time)#,dl_correction=dl_correction)
		else:
			self.dl_getter = rank_getters.RepoDLRank(db=self.db,start_time=self.start_time,end_time=self.end_time)#,dl_correction=dl_correction)
		self.deps_getter = edge_getters.RepoToRepoDeps(db=self.db,ref_time=self.end_time)
		self.devs_getter = edge_getters.DevToRepo(db=self.db,start_time=self.start_time,end_time=self.end_time)
		self.vaccinated_repo_ranks = None
		self.vaccinate_matrix = None

	def get_all(self):
		self.get_repo_ranks()
		self.get_deps_mat()
		self.get_devs_mat()
		self.get_dl_vec()
		self.get_init_vaccmat()
		self.get_repo_commits()

	def get_repo_ranks(self):
		if not hasattr(self,'repo_ranks'):
			self.repo_ranks = rank_getters.RepoRankGetter(db=self.db).get_result()[1]
		return self.repo_ranks

	def get_init_vaccmat(self):
		if not hasattr(self,'init_vaccmat'):
			repo_ranks = self.get_repo_ranks()
			self.init_vaccmat = sparse.eye(len(repo_ranks)).tolil()
		return self.init_vaccmat.tolil()

	def set_vaccinated_repos(self,vaccinated_repos=None,vaccinated_repo_ranks=None):
		if vaccinated_repo_ranks is not None:
			# self.vaccinated_repo_ranks = copy.deepcopy(vaccinated_repo_ranks)
			self.vaccinated_repo_ranks = vaccinated_repo_ranks
		elif vaccinated_repos is not None:
			repo_ranks = self.get_repo_ranks()
			self.vaccinated_repo_ranks = [repo_ranks[v_id] for v_id in vaccinated_repos]
		else:
			raise SyntaxError('Either vaccinated_repos or vaccinated_repo_ranks should be provided')
		
		vaccinate_matrix = self.get_init_vaccmat().copy()
		for v_nb in self.vaccinated_repo_ranks:
			vaccinate_matrix[v_nb,v_nb] = 0
		self.vaccinate_matrix = vaccinate_matrix.todia()

	def get_devs_mat(self,**kwargs):
		if not hasattr(self,'devs_mat'):
			self.devs_mat = self.devs_getter.get_result(**kwargs)

	def get_repo_commits(self,**kwargs):
		if not hasattr(self,'repo_commits'):
			mat = self.devs_getter.get_result(abs_value=True,**kwargs)
			self.repo_commits = np.asarray(mat.sum(axis=1)).flatten()

	def get_deps_mat(self,**kwargs):
		if not hasattr(self,'deps_mat'):
			self.deps_mat = self.deps_getter.get_result(**kwargs)#.transpose()

	def get_dl_vec(self):
		if self.dl_weights:
			if not hasattr(self,'dl_vec'):
				dl_direct,dl_indirect,dl_values = self.dl_getter.get_result(deps_mat=self.get_deps_mat(),orig_id_rank=True)
				self.dl_vec = dl_values

	def vaccinate(self,repo_status):
		if self.vaccinate_matrix is not None:
			repo_status = self.vaccinate_matrix * repo_status
		return repo_status

	def get(self,db,**kwargs):
		self.get_devs_mat()
		self.get_deps_mat()

		self.get_dl_vec()

		self.set_init_conditions()

		dev_status = self.init_dev_status
		repo_status = self.init_repo_status#.copy()

		repo_status_cond = self.init_repo_status#.copy()
		dev_status_cond = self.init_dev_status#.copy()

		count = 0
		while repo_status_cond.nnz or (count == 0):
		# while repo_status_cond.nnz or dev_status_cond.nnz:
		# while count<5:
			repo_status = self.propagate(dev_status=dev_status,
								repo_status=repo_status,
								) + self.init_repo_status
			repo_status_cond = self.propagate(dev_status=dev_status_cond,
									repo_status = repo_status_cond,force_dev_mat=True,info='cond')

			repo_status = self.vaccinate(repo_status)
			repo_status_cond = self.vaccinate(repo_status_cond)

			dev_status_cond = sparse.csc_matrix(dev_status_cond.shape)
			count += 1
			self.logger.debug('iteration {}'.format(count))
			if count >= self.iter_max:
				self.logger.info('Reached {} iterations, returning with current state'.format(self.iter_max))
				break

		self.logger.info('Ended after {} iterations'.format(count))
		if self.dl_weights:
			ans = (repo_status.transpose().multiply(self.dl_vec)).transpose()
			if self.norm_dl:
				ans = ans/self.dl_vec.sum()
		else:
			ans = repo_status

		self.result = ans
		return ans

	def propagate(self,dev_status,repo_status,force_dev_mat=False,info=None):
		'''
		returns repo_status_new. Cobb Douglas with given exponents.
		'''
		# dev_contrib = self.devs_mat * dev_status
		dev_contrib = self.get_dev_contrib(dev_status=dev_status,force=force_dev_mat)
		deps_contrib = self.deps_mat * repo_status
		# repo_status_new = dev_contrib + deps_contrib - dev_contrib.multiply(deps_contrib) # = 1- (1-dev_c)(1-deps_c); Cobb Douglas with both exponents equal to 1 but adapted to sparse mat

		# change values of 1 to NaN
		dev_contrib = dev_contrib.copy()
		dev_contrib[dev_contrib==1] = np.nan
		deps_contrib[deps_contrib==1] = np.nan

		# transform remaining values to (1-x)**p (becoming production value = 1 - impact value)
		dev_contrib.data = 1.-dev_contrib.data
		deps_contrib.data = 1.-deps_contrib.data

		dev_contrib = dev_contrib.power(self.dev_cd_power)
		deps_contrib = deps_contrib.power(self.deps_cd_power)

		# retransform to impact values

		dev_contrib.data = 1.-dev_contrib.data
		dev_contrib.data[np.isnan(dev_contrib.data)] = 1.

		deps_contrib.data = 1.-deps_contrib.data
		deps_contrib.data[np.isnan(deps_contrib.data)] = 1.

		# multiply point wise
		repo_status_new = dev_contrib + deps_contrib - dev_contrib.multiply(deps_contrib)

		repo_status_new = self.postprocess_repostatus(repo_status=repo_status_new,info=info)

		# repo_status_new.data = 1.-repo_status_new.data
		# repo_status_new.data[np.isnan(repo_status_new.data)] = 1.
		return repo_status_new

	def postprocess_repostatus(self,repo_status,info=None):
		if self.dev_mode or info == 'cond':
			return repo_status
		else:
			repo_status += self.init_repo_status
			repo_status.data[repo_status.data>1] = 1.
			return repo_status

	def get_dev_contrib(self,dev_status,force=False):
		if not force and self.stationary_devcontrib:
			if not hasattr(self,'dev_contrib'):
				self.dev_contrib = self.devs_mat * dev_status
			return self.dev_contrib
		else:
			return self.devs_mat * dev_status

	def set_init_conditions(self):
		u_max = self.devs_mat.shape[1]
		r_max = self.devs_mat.shape[0]
		if self.dev_mode:
			self.init_repo_status = sparse.csc_matrix((r_max,u_max))
			self.init_dev_status = sparse.eye(u_max)
		else:
			self.init_repo_status = sparse.eye(r_max)
			self.init_dev_status = sparse.csc_matrix((u_max,r_max))

	def parallelize_repo_space(self,factor):
		'''
		to parallelize computation for several repo spaces (eg when vaccinating)
		adding factor-1 times the matrices to themselves on the repo space dimension (nb_repos -> factor*nb_repos)
		instead of going to 3rd dimension
		'''
		# self.get_repo_ranks()
		self.get_devs_mat()
		self.get_deps_mat()
		self.get_dl_vec()
		self.set_init_conditions()
		self.get_init_vaccmat()

		# new_repo_ranks = self.repo_ranks
		# for i in range(factor-1):
		# 	new_repo_ranks.update({r_id+i*factor:r_rk+i*factor for r_id,r_rk in self.repo_ranks.items()})

		new_dl_vec = self.dl_vec
		for i in range(factor-1):
			new_dl_vec = np.concatenate([new_dl_vec,self.dl_vec])

		new_devs_mat = self.devs_mat
		for i in range(factor-1):
			new_devs_mat = scipy.sparse.vstack([new_devs_mat,self.devs_mat])
		
		new_deps_mat = self.deps_mat
		for i in range(factor-1):
			new_deps_mat = scipy.sparse.bmat([[self.deps_mat,None],[None,new_deps_mat]])
			
		# new_init_dev_status = self.init_dev_status
		# for i in range(factor-1):
		# 	new_init_dev_status = 

		new_init_repo_status = self.init_repo_status
		for i in range(factor-1):
			new_init_repo_status = scipy.sparse.vstack([new_init_repo_status,self.init_repo_status])

		new_init_vaccmat = self.init_vaccmat
		new_init_vaccmat = sparse.eye(self.init_vaccmat.shape[0]*factor).tolil()

		# self.repo_ranks = new_repo_ranks
		self.devs_mat = new_devs_mat
		self.deps_mat = new_deps_mat
		self.dl_vec = new_dl_vec
		self.init_repo_status = new_init_repo_status
		# self.init_dev_status = new_init_dev_status
		self.init_vaccmat = new_init_vaccmat

		if hasattr(self,'dev_contrib'):
			del self.dev_contrib

class SRLeontief(SRGetter):
	def propagate(self,dev_status,repo_status,force_dev_mat=False,info=None):
		'''
		returns repo_status_new
		'''

		# dev_contrib = self.devs_mat * dev_status
		dev_contrib = self.get_dev_contrib(dev_status=dev_status,force=force_dev_mat)
		deps_contrib = self.deps_mat * repo_status
		repo_status_new = (abs(dev_contrib - deps_contrib) + dev_contrib + deps_contrib)/2
		repo_status_new = self.postprocess_repostatus(repo_status=repo_status_new,info=info)
		return repo_status_new

class SRLinear(SRGetter):
	def propagate(self,dev_status,repo_status,force_dev_mat=False,info=None):
		'''
		returns repo_status_new
		'''

		# dev_contrib = self.devs_mat * dev_status
		dev_contrib = self.get_dev_contrib(dev_status=dev_status,force=force_dev_mat)
		deps_contrib = self.deps_mat * repo_status
		repo_status_new = (dev_contrib + deps_contrib)/2.
		repo_status_new = self.postprocess_repostatus(repo_status=repo_status_new,info=info)
		return repo_status_new

class OldSRCobbDouglas(SRGetter):
	def propagate(self,dev_status,repo_status,force_dev_mat=False,info=None):
		'''
		returns repo_status_new
		'''

		# dev_contrib = self.devs_mat * dev_status
		dev_contrib = self.get_dev_contrib(dev_status=dev_status,force=force_dev_mat)
		deps_contrib = self.deps_mat * repo_status
		repo_status_new = dev_contrib + deps_contrib - dev_contrib.multiply(deps_contrib) # = 1- (1-dev_c)(1-deps_c); Cobb Douglas with both exponents equal to 1 but adapted to sparse mat
		# assert (repo_status_new.data <=1).all()
		repo_status_new = self.postprocess_repostatus(repo_status=repo_status_new,info=info)
		return repo_status_new

