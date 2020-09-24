
import repo_tools
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
	db = repo_tools.repo_database.Database(db_name='travis_ci_test_repo_tools',db_type=request.param)
	db.clean_db()
	db.init_db()
	return db

##############

#### Tests

def test_createdb(dbtype):
	db = repo_tools.repo_database.Database(db_name='travis_ci_test_repo_tools',db_type=dbtype,clean_first=True)

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


def test_dl(testdb):
	testdb.register_source(source='GitHub',source_urlroot='github.com')
	testdb.register_url(source='GitHub',repo_url='https://github.com/test/test')
	testdb.register_repo(source='GitHub',repo='test',owner='test')
	testdb.submit_download_attempt(source='GitHub',owner='test',repo='test',success=False)
	testdb.submit_download_attempt(source='GitHub',owner='test',repo='test',success=True)