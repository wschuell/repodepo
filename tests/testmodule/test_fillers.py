
import repo_tools
from repo_tools.fillers import generic,commit_info,github
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
	db = repo_tools.repo_database.Database(db_name='travis_ci_test_repo_tools',db_type=request.param,data_folder=os.path.join(os.path.dirname(__file__),'dummy_data'))
	db.clean_db()
	db.init_db()
	return db

##############

#### Tests

def test_packages(testdb):
	testdb.add_filler(generic.PackageFiller(package_list_file='packages.csv'))
	testdb.fill_db()

def test_sources(testdb):
	testdb.add_filler(generic.SourcesFiller(source='GitHub',source_urlroot='github.com'))
	testdb.fill_db()

def test_sources2(testdb):
	testdb.add_filler(generic.SourcesFiller(source=['GitHub',],source_urlroot=['github.com',]))
	testdb.fill_db()

def test_sources3(testdb):
	testdb.add_filler(generic.SourcesFiller(source=['GitHub','blah'],source_urlroot=['github.com',None]))
	testdb.fill_db()

def test_repositories(testdb):
	testdb.add_filler(generic.SourcesFiller(source=['GitHub',],source_urlroot=['github.com',]))
	testdb.add_filler(generic.PackageFiller(package_list_file='packages.csv'))
	testdb.add_filler(generic.RepositoriesFiller())
	testdb.fill_db()

def test_clones_https(testdb):
	testdb.add_filler(generic.SourcesFiller(source=['GitHub',],source_urlroot=['github.com',]))
	testdb.add_filler(generic.PackageFiller(package_list_file='packages.csv'))
	testdb.add_filler(generic.RepositoriesFiller())
	testdb.add_filler(generic.ClonesFiller(data_folder='dummy_clones'))
	# testdb.add_filler(generic.ClonesFiller(data_folder='dummy_clones',rm_first=True))
	testdb.fill_db()

# def test_clones_ssh(testdb):
# 	testdb.add_filler(generic.SourcesFiller(source=['GitHub',],source_urlroot=['github.com',]))
# 	testdb.add_filler(generic.PackageFiller(package_list_file='packages.csv'))
# 	testdb.add_filler(generic.RepositoriesFiller())
# 	testdb.add_filler(generic.ClonesFiller(data_folder='dummy_clones',rm_first=True,ssh_sources={'GitHub':os.path.join(os.environ['HOME'],'.ssh','github','id_rsa')}))
# 	testdb.fill_db()

def test_commits(testdb):
	testdb.add_filler(generic.SourcesFiller(source=['GitHub',],source_urlroot=['github.com',]))
	testdb.add_filler(generic.PackageFiller(package_list_file='packages.csv'))
	testdb.add_filler(generic.RepositoriesFiller())
	testdb.add_filler(generic.ClonesFiller(data_folder='dummy_clones'))
	testdb.add_filler(commit_info.CommitsFiller(data_folder='dummy_clones'))
	testdb.fill_db()

def test_github(testdb):
	testdb.add_filler(generic.SourcesFiller(source=['GitHub',],source_urlroot=['github.com',]))
	testdb.add_filler(generic.PackageFiller(package_list_file='packages.csv'))
	testdb.add_filler(generic.RepositoriesFiller())
	testdb.add_filler(generic.ClonesFiller(data_folder='dummy_clones'))
	testdb.add_filler(commit_info.CommitsFiller(data_folder='dummy_clones'))
	testdb.add_filler(github.StarsFiller(fail_on_wait=True))
	testdb.add_filler(github.GHLoginsFiller(fail_on_wait=True))
	testdb.add_filler(github.FollowersFiller(fail_on_wait=True))
	testdb.fill_db()
