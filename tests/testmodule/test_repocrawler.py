
import repo_tools
import pytest
import datetime
import time
import os
import shutil

#### Parameters
dbtype_list = [
	'sqlite',
	# 'postgres'
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

@pytest.fixture(params=dbtype_list)
def testrc(request):
	db = repo_tools.repo_database.Database(db_name='travis_ci_test_repo_tools',db_type=request.param)
	db.clean_db()
	db.init_db()
	try:
		shutil.rmtree('test_clones/testrc')
	except:
		pass
	return repo_tools.repo_crawler.RepoCrawler(db=db,folder='test_clones/testrc')

##############

#### Tests

def test_rc(testdb):
	rc = repo_tools.repo_crawler.RepoCrawler(db=testdb)

def test_addlist(testrc):
	testrc.add_list(['wschuell/experiment_manager','flowersteam/naminggamesal'],source='GitHub',source_urlroot='github.com')

def test_clone(testrc):
	testrc.add_list(['wschuell/experiment_manager','flowersteam/naminggamesal'],source='GitHub',source_urlroot='github.com')
	testrc.clone_all()

def test_clone_inexistant(testrc):
	testrc.add_list(['wschuell/experiment_manager','flowersteam/naminggamesal'],source='GitHub',source_urlroot='github.com')
	testrc.add_list(['wschuell/inexistantrepo'],source='GitHub',source_urlroot='github.com')
	testrc.clone_all()

def test_gitfetch(testrc):
	testrc.add_list(['wschuell/experiment_manager','flowersteam/naminggamesal'],source='GitHub',source_urlroot='github.com')
	testrc.clone_all()
	testrc.clone_all(update=True)

def test_fillcommits(testrc):
	testrc.add_list(['wschuell/experiment_manager','flowersteam/naminggamesal'],source='GitHub',source_urlroot='github.com')
	testrc.clone_all()
	testrc.fill_commit_info()
