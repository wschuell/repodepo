
import repodepo
from repodepo import extras
from repodepo.extras import exports
from repodepo.fillers import generic,meta_fillers
from repodepo.getters import project_getters,user_getters,generic_getters,combined_getters,edge_getters
import pytest
import datetime
import time
import os
import inspect

import numpy as np
from scipy import sparse

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

geng_list = [
	generic_getters.RepoIDs,
	generic_getters.RepoNames,
	generic_getters.RepoCreatedAt,
	generic_getters.UserIDs,
	generic_getters.UserLogins,
	]
@pytest.fixture(params=geng_list)
def generic_g(request):
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

subclasses_Pgetters = inspect.getmembers(project_getters, lambda elt:(inspect.isclass(elt) and issubclass(elt,project_getters.ProjectGetter) and elt!=project_getters.ProjectGetter ))
@pytest.fixture(params=subclasses_Pgetters)
def Pgetter(request):
	return request.param[1]

subclasses_Ugetters = inspect.getmembers(user_getters, lambda elt:(inspect.isclass(elt) and issubclass(elt,user_getters.UserGetter) and elt!=user_getters.UserGetter ))
@pytest.fixture(params=subclasses_Ugetters)
def Ugetter(request):
	return request.param[1]

subclasses_combined_g = inspect.getmembers(combined_getters, lambda elt:(inspect.isclass(elt) and issubclass(elt,combined_getters.CombinedGetter) and elt!=combined_getters.CombinedGetter ))
@pytest.fixture(params=subclasses_combined_g)
def combined_g(request):
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
		db.add_filler(meta_fillers.DummyMetaFiller())
		db.fill_db()
	yield db
	db.connection.close()
	del db


@pytest.fixture(params=['postgres'])
def pdb(request):
	db = repodepo.repo_database.Database(db_name='travis_ci_test_repo_tools',db_type=request.param,data_folder=os.path.join(os.path.dirname(__file__),'dummy_data'))
	yield db
	db.connection.close()
	del db


@pytest.fixture(params=['sqlite'])
def sdb(request):
	orig_db = repodepo.repo_database.Database(db_name='travis_ci_test_repo_tools',db_type='postgres',data_folder=os.path.join(os.path.dirname(__file__),'dummy_data'))
	db = repodepo.repo_database.Database(db_name='travis_ci_test_repo_tools_comparison',db_type=request.param,data_folder=os.path.join(os.path.dirname(__file__),'dummy_data'))
	extras.exports.export(orig_db=orig_db,dest_db=db)
	yield db
	db.connection.close()
	del db

epsilon = 0 # 10**-15

##############

#### Tests

def test_setdb(testdb):
	testdb.init_db()

def test_generic_getters(testdb,generic_g):
	generic_g(db=testdb).get_result()

def test_combined_getters(testdb,combined_g):
	combined_g(db=testdb).get_result()

def test_gettersPproj(testdb,time_window,Pgetter,proj_id,cumulative):
	testdb.init_db()
	df = Pgetter().get_result(db=testdb,aggregated=True,time_window=time_window,cumulative=cumulative,project_id=proj_id)

def test_gettersP(testdb,time_window,Pgetter,aggregated,cumulative):
	testdb.init_db()
	df = Pgetter().get_result(db=testdb,aggregated=aggregated,time_window=time_window,cumulative=cumulative)

def test_gettersUuser(testdb,time_window,Ugetter,identity_id,cumulative):
	testdb.init_db()
	df = Ugetter().get_result(db=testdb,aggregated=True,time_window=time_window,cumulative=cumulative,identity_id=identity_id)

def test_gettersU(testdb,time_window,Ugetter,cumulative,aggregated):
	testdb.init_db()
	df = Ugetter().get_result(db=testdb,aggregated=aggregated,time_window=time_window,cumulative=cumulative)


#### test equal postgres vs sqlite

# def test_value_generic_getters(pdb,sdb,generic_g):
# 	p = generic_g(db=pdb).get_result()
# 	s = generic_g(db=pdb).get_result()
# 	assert p.equals(s)

# def test_value_combined_getters(pdb,sdb,combined_g):
# 	p = combined_g(db=pdb).get_result()
# 	s = combined_g(db=sdb).get_result()
# 	assert p.equals(s)

# def test_value_gettersPproj(pdb,sdb,time_window,Pgetter,proj_id,cumulative):
# 	pdb.init_db()
# 	sdb.init_db()
# 	p = Pgetter().get_result(db=pdb,aggregated=True,time_window=time_window,cumulative=cumulative,project_id=proj_id)
# 	s = Pgetter().get_result(db=sdb,aggregated=True,time_window=time_window,cumulative=cumulative,project_id=proj_id)
# 	assert p.equals(s)

# def test_value_gettersP(pdb,sdb,time_window,Pgetter,aggregated,cumulative):
# 	pdb.init_db()
# 	sdb.init_db()
# 	p = Pgetter().get_result(db=pdb,aggregated=aggregated,time_window=time_window,cumulative=cumulative)
# 	s = Pgetter().get_result(db=sdb,aggregated=aggregated,time_window=time_window,cumulative=cumulative)
# 	assert p.equals(s)

# def test_value_gettersUuser(pdb,sdb,time_window,Ugetter,identity_id,cumulative):
# 	pdb.init_db()
# 	sdb.init_db()
# 	p = Ugetter().get_result(db=pdb,aggregated=True,time_window=time_window,cumulative=cumulative,identity_id=identity_id)
# 	s = Ugetter().get_result(db=sdb,aggregated=True,time_window=time_window,cumulative=cumulative,identity_id=identity_id)
# 	assert p.equals(s)

# def test_value_gettersU(pdb,sdb,time_window,Ugetter,cumulative,aggregated):
# 	pdb.init_db()
# 	sdb.init_db()
# 	p = Ugetter().get_result(db=pdb,aggregated=aggregated,time_window=time_window,cumulative=cumulative)
# 	s = Ugetter().get_result(db=sdb,aggregated=aggregated,time_window=time_window,cumulative=cumulative)
# 	assert p.equals(s)

def test_value_edge_getters(pdb,sdb):
	p = edge_getters.DevToRepo(db=pdb).get_result()
	s = edge_getters.DevToRepo(db=sdb).get_result()

	p.eliminate_zeros()
	s.eliminate_zeros()

	assert p.nonzero()[0].shape == s.nonzero()[0].shape and p.nonzero()[1].shape == s.nonzero()[1].shape and (p.nonzero()[0] == s.nonzero()[0]).all() and (p.nonzero()[1] == s.nonzero()[1]).all(), 'Different sparsity structure'
	assert (np.abs(p.data-s.data)*2./(p.data+s.data)<= epsilon).all()

def test_value_edge_addmax(pdb,sdb):
	p = edge_getters.DevToRepoAddMax(db=pdb,repo_list=list(range(5))).get_result()
	s = edge_getters.DevToRepoAddMax(db=sdb,repo_list=list(range(5))).get_result()

	p.eliminate_zeros()
	s.eliminate_zeros()

	assert p.nonzero()[0].shape == s.nonzero()[0].shape and p.nonzero()[1].shape == s.nonzero()[1].shape and (p.nonzero()[0] == s.nonzero()[0]).all() and (p.nonzero()[1] == s.nonzero()[1]).all(), 'Different sparsity structure'
	assert (np.abs(p.data-s.data)*2./(p.data+s.data)<= epsilon).all()

def test_value_edge_cr(pdb,sdb):
	p = edge_getters.DevToRepoAddDailyCommits(db=pdb,repo_list=list(range(5)),daily_commits=0.005/7.).get_result()
	s = edge_getters.DevToRepoAddDailyCommits(db=sdb,repo_list=list(range(5)),daily_commits=0.005/7.).get_result()

	p.eliminate_zeros()
	s.eliminate_zeros()

	assert p.nonzero()[0].shape == s.nonzero()[0].shape and p.nonzero()[1].shape == s.nonzero()[1].shape and (p.nonzero()[0] == s.nonzero()[0]).all() and (p.nonzero()[1] == s.nonzero()[1]).all(), 'Different sparsity structure'
	assert (np.abs(p.data-s.data)*2./(p.data+s.data)<= epsilon).all()

def test_value_edge_deps(pdb,sdb):
	p = edge_getters.RepoToRepoDeps(db=pdb).get_result()
	s = edge_getters.RepoToRepoDeps(db=sdb).get_result()

	p.eliminate_zeros()
	s.eliminate_zeros()

	assert p.nonzero()[0].shape == s.nonzero()[0].shape and p.nonzero()[1].shape == s.nonzero()[1].shape and (p.nonzero()[0] == s.nonzero()[0]).all() and (p.nonzero()[1] == s.nonzero()[1]).all(), 'Different sparsity structure'
	assert (np.abs(p.data-s.data)*2./(p.data+s.data)<= epsilon).all()
