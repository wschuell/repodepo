
import repo_tools
from repo_tools.fillers import generic,meta_fillers
from repo_tools.getters import project_getters,user_getters
import pytest
import datetime
import time
import os
import inspect




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

subclasses_Pgetters = inspect.getmembers(project_getters, lambda elt:(inspect.isclass(elt) and issubclass(elt,project_getters.ProjectGetter) and elt!=project_getters.ProjectGetter ))
@pytest.fixture(params=subclasses_Pgetters)
def Pgetter(request):
	return request.param[1]

subclasses_Ugetters = inspect.getmembers(user_getters, lambda elt:(inspect.isclass(elt) and issubclass(elt,user_getters.UserGetter) and elt!=user_getters.UserGetter ))
@pytest.fixture(params=subclasses_Ugetters)
def Ugetter(request):
	return request.param[1]


@pytest.fixture(params=dbtype_list)
def testdb(request):
	db = repo_tools.repo_database.Database(db_name='travis_ci_test_repo_tools',db_type=request.param,data_folder=os.path.join(os.path.dirname(__file__),'dummy_data'))
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


##############

#### Tests

def test_setdb(testdb):
	testdb.init_db()

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
