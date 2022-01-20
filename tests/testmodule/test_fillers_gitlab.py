
import repo_tools
from repo_tools.fillers import generic,commit_info,gitlab_gql,meta_fillers
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
	db = repo_tools.repo_database.Database(db_name='travis_ci_test_repo_tools',db_type=request.param,data_folder='dummy_clones')
	db.clean_db()
	db.init_db()
	return db

workers = 1 #5

##############

#### Tests
@pytest.mark.timeout(100)
def test_gitlab(testdb):
	testdb.add_filler(generic.SourcesFiller(source=['Gitlab',],source_urlroot=['gitlab.com',]))
	testdb.add_filler(generic.PackageFiller(package_list_file='packages_gitlab.csv',data_folder=os.path.join(os.path.dirname(__file__),'dummy_data')))
	testdb.add_filler(generic.RepositoriesFiller())
	testdb.add_filler(generic.ClonesFiller(data_folder='dummy_clones')) # Clones after forks to have up-to-date repo URLS (detect redirects)
	testdb.add_filler(commit_info.CommitsFiller(data_folder='dummy_clones')) # Commits after forks because fork info needed for repo commit ownership
	# testdb.add_filler(gitlab_gql.LoginsFiller(fail_on_wait=True,workers=workers))
	# testdb.add_filler(github_rest.StarsFiller(fail_on_wait=True,workers=workers,no_unauth=True))
	# testdb.add_filler(github_rest.FollowersFiller(fail_on_wait=True,workers=workers,no_unauth=True))
	testdb.fill_db()
