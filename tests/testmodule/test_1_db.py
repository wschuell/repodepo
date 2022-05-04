
import repodepo
import pytest
import datetime
import time
import os

#### Parameters
dbtype_list = [
	'sqlite',
	'postgres'
	]
@pytest.fixture(params=dbtype_list)
def dbtype(request):
	return request.param


@pytest.fixture(params=dbtype_list)
def testdb(request):
	db = repodepo.repo_database.Database(db_name='travis_ci_test_repo_tools',db_type=request.param)
	db.clean_db()
	db.init_db()
	yield db
	db.connection.close()
	del db

@pytest.fixture(params=dbtype_list)
def otherdb(request):
	db = repodepo.repo_database.Database(db_name='travis_ci_test_repo_tools_dumptest',db_type=request.param)
	db.clean_db()
	db.init_db()
	yield db
	db.connection.close()
	del db

##############

#### Tests

def test_createdb(dbtype):
	db = repodepo.repo_database.Database(db_name='travis_ci_test_repo_tools',db_type=dbtype,clean_first=True)

def test_cleandb(testdb):
	testdb.clean_db()

def test_doublecleandb(testdb):
	testdb.clean_db()
	testdb.clean_db()

def test_source(testdb):
	testdb.register_source(source='GitHub',source_urlroot='github.com')

def test_url(testdb):
	testdb.register_source(source='GitHub',source_urlroot='github.com')
	testdb.register_url(source='GitHub',repo_url='https://github.com/test/test')

def test_repo(testdb):
	testdb.register_source(source='GitHub',source_urlroot='github.com')
	testdb.register_url(source='GitHub',repo_url='https://github.com/test/test')
	testdb.register_repo(source='GitHub',repo='test',owner='test')

def test_merge_repos(testdb):
	testdb.register_source(source='GitHub',source_urlroot='github.com')
	testdb.register_url(source='GitHub',repo_url='https://github.com/test/test')
	testdb.register_repo(source='GitHub',repo='test',owner='test')
	testdb.register_url(source='GitHub',repo_url='https://github.com/test1/test2')
	testdb.register_repo(source='GitHub',repo='test2',owner='test1')
	testdb.merge_repos(obsolete_source='GitHub',obsolete_owner='test',obsolete_name='test',new_owner='test1',new_name='test2')
	testdb.merge_repos(obsolete_source='GitHub',obsolete_owner='test1',obsolete_name='test2',new_owner='test3',new_name='test2')


def test_dl(testdb):
	testdb.register_source(source='GitHub',source_urlroot='github.com')
	testdb.register_url(source='GitHub',repo_url='https://github.com/test/test')
	testdb.register_repo(source='GitHub',repo='test',owner='test')
	testdb.submit_download_attempt(source='GitHub',owner='test',repo='test',success=False)
	testdb.submit_download_attempt(source='GitHub',owner='test',repo='test',success=True)

def test_ram(testdb):
	testdb.register_source(source='GitHub',source_urlroot='github.com')
	testdb.register_url(source='GitHub',repo_url='https://github.com/test/test')
	testdb.register_repo(source='GitHub',repo='test',owner='test')
	testdb.submit_download_attempt(source='GitHub',owner='test',repo='test',success=False)
	testdb.submit_download_attempt(source='GitHub',owner='test',repo='test',success=True)
	testdb.move_to_RAM()
