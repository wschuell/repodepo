
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


from repodepo.getters import edge_getters,rank_getters


#### Parameters
dbtype_list = [
	'sqlite',
	'postgres'
	]
@pytest.fixture(params=dbtype_list)
def dbtype(request):
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
