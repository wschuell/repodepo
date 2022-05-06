
import repodepo
from repodepo.fillers import generic,meta_fillers
import pytest
import datetime
import time
import os
import inspect
import numpy as np
import json
from scipy import sparse

from repodepo.getters import SR_getters,policy_getters,effect_rank_getters
from repodepo.getters import edge_getters,rank_getters


#### Parameters
dbtype_list = [
	'sqlite',
	'postgres'
	]
@pytest.fixture(params=dbtype_list)
def dbtype(request):
	return request.param

tw_list_nonone = [
	'month',
	'day',
	'year'
	]
@pytest.fixture(params=tw_list_nonone)
def time_window_nonone(request):
	return request.param

tw_list = tw_list_nonone + [None]
@pytest.fixture(params=tw_list)
def time_window(request):
	return request.param


cumul_list = [
	True,
	False
	]
@pytest.fixture(params=cumul_list)
def cumulative(request):
	return request.param

agg_list = [
	True,
	False
	]
@pytest.fixture(params=agg_list)
def aggregated(request):
	return request.param

proj_list = [
	'pubs/pubs',
	'pubs/pubs',
	('pubs/pubs','GitHub'),
	1,
	]
@pytest.fixture(params=proj_list)
def proj_id(request):
	return request.param

identity_list = [
	'wschuell',
	'wschuell@users.noreply.github.com',
	'benureau',
	('wschuell','github_login'),
	1,
	(1,'github_login'),
	]
@pytest.fixture(params=identity_list)
def identity_id(request):
	return request.param

subclasses_ranks = inspect.getmembers(rank_getters, lambda elt:(inspect.isclass(elt) and issubclass(elt,rank_getters.RepoRankGetter) ))
@pytest.fixture(params=subclasses_ranks)
def rank_class(request):
	return request.param[1]

@pytest.fixture(params=dbtype_list)
def testdb(request):
	db = repodepo.repo_database.Database(db_name='travis_ci_test_repo_tools',db_type=request.param,data_folder=os.path.join(os.path.dirname(__file__),'dummy_data'))
	db.init_db()
	db.cursor.execute('''SELECT info_content FROM _dbinfo WHERE info_type='DB_INIT';''')
	ans = db.cursor.fetchone()
	db.cursor.execute('''SELECT COUNT(*) FROM commits;''')
	count = db.cursor.fetchone()[0]
	if count == 0 or ans is None or ans[0] != db.DB_INIT:
		db.clean_db()
		db.init_db()
		db.add_filler(meta_fillers.DummyMetaFiller(fail_on_wait=True))
		db.fill_db()
	yield db
	db.connection.close()


SRGetter_params = {'cobbdouglas':(SR_getters.SRGetter,{}),
					'cobbdouglas1_1':(SR_getters.SRGetter,dict(dev_cd_power=1,deps_cd_power=1)),
					'leontief':(SR_getters.SRLeontief,{}),
					'linear':(SR_getters.SRLinear,{}),
					'old_cd':(SR_getters.OldSRCobbDouglas,{}),
					}

SRGetter_types = sorted(SRGetter_params.keys())

@pytest.fixture(params=SRGetter_types)
def testsrgetter(request):
	ans = SRGetter_params[request.param][0](db='dummyDB',**SRGetter_params[request.param][1])
	ans.testsrgetter_name = request.param

	ans.repo_ranks = (np.asarray([1,4,6,11]),
						{1:0,4:1,6:2,11:3})
	# ans.devs_mat = sparse.csr_matrix(np.asarray([[0.2,1.,0,0],
	# 											[0.8,0,1,0],
	# 											[0,0,0,0],
	# 											[0,0,0,1],
	# 											[0,0,0,0],
	# 											])).transpose()
	ans.devs_mat = sparse.csr_matrix(np.asarray([
												[0.2,0.8,0,0,0],
												[1,0,0,0,0],
												[0,1,0,0,0],
												[0,0,0,1,0],
												])) # [repo1[dev1,dev2,...],repo2[dev1,dev2,...],...]
	ans.deps_mat = sparse.csr_matrix(np.asarray([[0,0,0,0],
												[1,0,0,0],
												[0.5,0.5,0,0],
												[0,0,0,0],
												]))#.transpose()

	ans.dl_vec = np.asarray([100,10,20,5])
	return ans

checked_results = {
	'linear':np.asarray([
		[10,40,0,0,0],
		[5.5,2,0,0,0],
		[3.25,13,0,0,0],
		[0,0,0,2.5,0],
		]),
	'leontief':np.asarray([
		[20,80,0,0,0],
		[10,8,0,0,0],
		[12,20,0,0,0],
		[0,0,0,5,0],
		]),
	'cobbdouglas':np.asarray([
		[(1-np.sqrt(0.8))*100,(1-np.sqrt(0.2))*100,0,0,0],
		[10,(1-np.sqrt(np.sqrt(0.2)))*10,0,0,0],
		[(1-np.sqrt(np.sqrt(0.8)/2))*20,20,0,0,0],
		[0,0,0,5,0],
		]),
	'old_cd':np.asarray([
		[20,80,0,0,0],
		[10,8,0,0,0],
		[12,20,0,0,0],
		[0,0,0,5,0],
		]),
	'cobbdouglas1_1':np.asarray([
		[20,80,0,0,0],
		[10,8,0,0,0],
		[12,20,0,0,0],
		[0,0,0,5,0],
		]),
}

##############

#### Tests

def test_setdb(testdb):
	testdb.init_db()

def test_dev_to_repo(testdb):
	edge_getters.DevToRepo(db=testdb).get_result()


def test_rank(testdb,rank_class):
	ranks = rank_class(db=testdb).get_result()
	assert len(ranks) == 3
	assert ranks[0].shape == ranks[2].shape

def test_rank_reorder(testdb,rank_class):
	ranks = rank_class(db=testdb).get_result(orig_rank_id=True)
	assert len(ranks) == 3
	assert ranks[0].shape == ranks[2].shape

def test_dev_to_repoaddmax(testdb):
	ranks = rank_getters.RepoRankGetter(db=testdb).get_result()[0]
	repo_list = tuple(ranks[:10])
	edge_getters.DevToRepoAddMax(db=testdb,repo_list=repo_list).get_result()

def test_dev_to_repodailycommits(testdb):
	ranks = rank_getters.RepoRankGetter(db=testdb).get_result()[0]
	repo_list = tuple(ranks[:10])
	edge_getters.DevToRepoAddDailyCommits(db=testdb,repo_list=repo_list,daily_commits=5/7).get_result()

#### SRGetter .get_result() gives a sparse mat devs_to_repo

def test_srgetter(testdb):
	devs_to_repo = SR_getters.SRGetter(db=testdb,start_time='2010-07-01',end_time='2020-07-01',iter_max=10,dl_weights=False).get_result()

def test_srgetter_dl(testdb):
	devs_to_repo = SR_getters.SRGetter(db=testdb,start_time='2010-07-01',end_time='2020-07-01',iter_max=10,dl_weights=True).get_result()

def test_srgetter_depsmode(testdb):
	devs_to_repo = SR_getters.SRGetter(db=testdb,start_time='2010-07-01',end_time='2020-07-01',iter_max=10,dl_weights=True,dev_mode=False).get_result()


def test_srlinear(testdb):
	devs_to_repo = SR_getters.SRLinear(db=testdb,start_time='2010-07-01',end_time='2020-07-01',iter_max=10,dl_weights=False).get_result()

def test_srleontief(testdb):
	devs_to_repo = SR_getters.SRLeontief(db=testdb,start_time='2010-07-01',end_time='2020-07-01',iter_max=10,dl_weights=False).get_result()


## Policies add a number of devs (using a DevToRepo process: either maxval or dailycommits) to the first repos of the ranking and result is the SRGetter matrix result
def test_policy(testdb):
	ranks = rank_getters.RepoRankGetter(db=testdb).get_result()
	ans = policy_getters.PolicyGetter(db=testdb,ranks=ranks,nb_devs=10,dl_weights=False).get_result()

def test_batchpolicy(testdb):
	ranks = rank_getters.RepoRankGetter(db=testdb).get_result()
	pg = policy_getters.BatchPolicyGetter(db=testdb,nb_devs_list=[0,1,2,3,5,10],policy_getter_args=dict(ranks=ranks,dl_weights=False))
	ans = pg.get_result()

def test_batchpolicy_save(testdb):
	ranks = rank_getters.RepoRankGetter(db=testdb).get_result()
	cr_db_path = os.path.join(os.path.dirname(__file__),'cr_db_tests.db')
	if os.path.exists(cr_db_path):
		os.remove(cr_db_path)
	pg = policy_getters.BatchPolicyGetter(db=testdb,nb_devs_list=[0,1,2,3,5,10],policy_getter_args=dict(ranks=ranks,dl_weights=False),computation_results_db=cr_db_path)
	ans = pg.get_result()
	pg2 = policy_getters.BatchPolicyGetter(db=testdb,nb_devs_list=[0,1,2,3,5,10],policy_getter_args=dict(ranks=ranks,dl_weights=False),computation_results_db=cr_db_path)
	ans2 = pg2.get_result()
	assert json.dumps(ans,sort_keys=True) == json.dumps(ans2,sort_keys=True)

def test_batchpolicy_parallel(testdb):
	ranks = rank_getters.RepoRankGetter(db=testdb).get_result()
	pg = policy_getters.ParallelBatchPolicyGetter(db=testdb,nb_devs_list=[0,1,2,3,5,10],policy_getter_args=dict(ranks=ranks,dl_weights=False))
	ans = pg.get_result()

def test_batchpolicy_parallel_save(testdb):
	ranks = rank_getters.RepoRankGetter(db=testdb).get_result()
	cr_db_path = os.path.join(os.path.dirname(__file__),'cr_db_tests.db')
	if os.path.exists(cr_db_path):
		os.remove(cr_db_path)
	pg = policy_getters.ParallelBatchPolicyGetter(db=testdb,nb_devs_list=[0,1,2,3,5,10],policy_getter_args=dict(ranks=ranks,dl_weights=False),computation_results_db=cr_db_path)
	ans = pg.get_result()
	pg2 = policy_getters.ParallelBatchPolicyGetter(db=testdb,nb_devs_list=[0,1,2,3,5,10],policy_getter_args=dict(ranks=ranks,dl_weights=False),computation_results_db=cr_db_path)
	ans2 = pg2.get_result()
	assert json.dumps(ans,sort_keys=True) == json.dumps(ans2,sort_keys=True)

### 'Vaccination' to get a ranking of repos based on impact if vaccinated (usable as input for PolicyGetter):
def test_vacc(testdb):
	ranks = effect_rank_getters.VaccinationRankGetter(db=testdb).get_result()

def test_vacc_save(testdb):
	cr_db_path = os.path.join(os.path.dirname(__file__),'cr_db_tests.db')
	if os.path.exists(cr_db_path):
		os.remove(cr_db_path)
	ranks = effect_rank_getters.VaccinationRankGetter(db=testdb,computation_results_db=cr_db_path).get_result()
	time.sleep(0.1)
	ranks2 = effect_rank_getters.VaccinationRankGetter(db=testdb,computation_results_db=cr_db_path).get_result()
	assert (ranks[0]==ranks2[0]).all()
	assert (ranks[2]==ranks2[2]).all()

def test_parallel_vacc(testdb):
	ranks = effect_rank_getters.ParallelVaccRankGetter(db=testdb).get_result()

def test_parallel_vacc_save(testdb):
	cr_db_path = os.path.join(os.path.dirname(__file__),'cr_db_tests.db')
	if os.path.exists(cr_db_path):
		os.remove(cr_db_path)
	ranks = effect_rank_getters.ParallelVaccRankGetter(db=testdb,computation_results_db=cr_db_path).get_result()
	time.sleep(0.1)
	ranks2 = effect_rank_getters.ParallelVaccRankGetter(db=testdb,computation_results_db=cr_db_path).get_result()
	assert (ranks[0]==ranks2[0]).all()
	assert (ranks[2]==ranks2[2]).all()

def test_parallel_semigreedy_save(testdb):
	cr_db_path = os.path.join(os.path.dirname(__file__),'cr_db_tests.db')
	if os.path.exists(cr_db_path):
		os.remove(cr_db_path)
	ranks = effect_rank_getters.SemiGreedyEffectRank(db=testdb,computation_results_db=cr_db_path).get_result()
	time.sleep(0.1)
	ranks2 = effect_rank_getters.SemiGreedyEffectRank(db=testdb,computation_results_db=cr_db_path).get_result()
	assert (ranks[0]==ranks2[0]).all()
	assert (ranks[2]==ranks2[2]).all()

def test_effvacc(testdb):
	ranks = effect_rank_getters.EfficientVaccRankGetter(db=testdb,grouping_size=9).get_result()

# def test_eff_vacc_save(testdb):
# 	cr_db_path = os.path.join(os.path.dirname(__file__),'cr_db_tests.db')
# 	if os.path.exists(cr_db_path):
# 		os.remove(cr_db_path)
# 	ranks = effect_rank_getters.EfficientVaccRankGetter(db=testdb,computation_results_db=cr_db_path).get_result()
# 	ranks2 = effect_rank_getters.EfficientVaccRankGetter(db=testdb,computation_results_db=cr_db_path).get_result()
# 	assert (ranks[0]==ranks2[0]).all()
# 	assert (ranks[2]==ranks2[2]).all()

### NB: to assess health, use SRGetter result, to assess efficiency of policy compare Policy result with SRGetter result 

def test_dlcorrect_rank(testdb):
	ranks = rank_getters.RepoDLCorrectionRank(db=testdb).get_result()[0]

def test_sr_propag(testsrgetter):
	results = testsrgetter.get_result()
	dense_results = results.todense()
	ref_results = checked_results[testsrgetter.testsrgetter_name]
	# assert (dense_results == ref_results).all() , dense_results
	assert np.abs(dense_results - ref_results).sum()<10**-5 , sparse.csr_matrix(dense_results - ref_results).__str__()

def test_sr_propag_dlnorm(testsrgetter):
	testsrgetter.norm_dl = True
	results = testsrgetter.get_result()
	dense_results = results.todense()
	ref_results = checked_results[testsrgetter.testsrgetter_name]/135.
	# assert (dense_results == ref_results).all() , dense_results
	assert np.abs(dense_results - ref_results).sum()<10**-5 , sparse.csr_matrix(dense_results - ref_results).__str__()
