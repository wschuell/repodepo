
import repo_tools
from repo_tools.fillers import generic,commit_info,github_rest,meta_fillers
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

workers = 5

##############

#### Tests

@pytest.mark.timeout(10)
def test_packages(testdb):
	testdb.add_filler(generic.PackageFiller(package_list_file='packages.csv',data_folder=os.path.join(os.path.dirname(__file__),'dummy_data')))
	testdb.fill_db()

@pytest.mark.timeout(10)
def test_sources(testdb):
	testdb.add_filler(generic.SourcesFiller(source='GitHub',source_urlroot='github.com'))
	testdb.fill_db()

@pytest.mark.timeout(10)
def test_sources2(testdb):
	testdb.add_filler(generic.SourcesFiller(source=['GitHub',],source_urlroot=['github.com',]))
	testdb.fill_db()

@pytest.mark.timeout(10)
def test_sources3(testdb):
	testdb.add_filler(generic.SourcesFiller(source=['GitHub','blah'],source_urlroot=['github.com',None]))
	testdb.fill_db()

@pytest.mark.timeout(30)
def test_repositories(testdb):
	testdb.add_filler(generic.SourcesFiller(source=['GitHub',],source_urlroot=['github.com',]))
	testdb.add_filler(generic.PackageFiller(package_list_file='packages.csv',data_folder=os.path.join(os.path.dirname(__file__),'dummy_data')))
	testdb.add_filler(generic.RepositoriesFiller())
	testdb.fill_db()

# @pytest.mark.timeout(100)
# def test_clones_https(testdb):
# 	testdb.add_filler(generic.SourcesFiller(source=['GitHub',],source_urlroot=['github.com',]))
# 	testdb.add_filler(generic.PackageFiller(package_list_file='packages.csv',data_folder=os.path.join(os.path.dirname(__file__),'dummy_data')))
# 	testdb.add_filler(generic.RepositoriesFiller())
# 	testdb.add_filler(generic.ClonesFiller(data_folder='dummy_clones',update=True))
# 	# testdb.add_filler(generic.ClonesFiller(data_folder='dummy_clones',rm_first=True))
# 	testdb.fill_db()

# @pytest.mark.timeout(100)
# def test_commits(testdb):
# 	testdb.add_filler(generic.SourcesFiller(source=['GitHub',],source_urlroot=['github.com',]))
# 	testdb.add_filler(generic.PackageFiller(package_list_file='packages.csv',data_folder=os.path.join(os.path.dirname(__file__),'dummy_data')))
# 	testdb.add_filler(generic.RepositoriesFiller())
# 	testdb.add_filler(generic.ClonesFiller(data_folder='dummy_clones'))
# 	testdb.add_filler(commit_info.CommitsFiller(data_folder='dummy_clones'))
# 	testdb.fill_db()

# @pytest.mark.timeout(100)
# def test_merge_repositories(testdb):
# 	testdb.add_filler(generic.SourcesFiller(source=['GitHub',],source_urlroot=['github.com',]))
# 	testdb.add_filler(generic.PackageFiller(package_list_file='packages.csv',data_folder=os.path.join(os.path.dirname(__file__),'dummy_data')))
# 	testdb.add_filler(generic.RepositoriesFiller())
# 	testdb.add_filler(generic.ClonesFiller(data_folder='dummy_clones'))
# 	testdb.add_filler(commit_info.CommitsFiller(data_folder='dummy_clones'))
# 	testdb.fill_db()
# 	testdb.plan_repo_merge(new_id=2,obsolete_id=1,obsolete_source='GitHub')
# 	testdb.batch_merge_repos()
# 	testdb.plan_repo_merge(new_id=None,new_owner='blah',new_name='blih',obsolete_id=2,obsolete_source='GitHub')
# 	testdb.batch_merge_repos()

# @pytest.mark.timeout(100)
# def test_github(testdb):
# 	testdb.add_filler(generic.SourcesFiller(source=['GitHub',],source_urlroot=['github.com',]))
# 	testdb.add_filler(generic.PackageFiller(package_list_file='packages.csv',data_folder=os.path.join(os.path.dirname(__file__),'dummy_data')))
# 	testdb.add_filler(generic.RepositoriesFiller())
# 	testdb.add_filler(github_rest.ForksFiller(fail_on_wait=True,workers=workers))
# 	testdb.add_filler(generic.ClonesFiller(data_folder='dummy_clones')) # Clones after forks to have up-to-date repo URLS (detect redirects)
# 	testdb.add_filler(commit_info.CommitsFiller(data_folder='dummy_clones')) # Commits after forks because fork info needed for repo commit ownership
# 	testdb.add_filler(github_rest.GHLoginsFiller(fail_on_wait=True,workers=workers))
# 	testdb.add_filler(github_rest.StarsFiller(fail_on_wait=True,workers=workers))
# 	testdb.add_filler(github_rest.FollowersFiller(fail_on_wait=True,workers=workers))
# 	testdb.fill_db()

# @pytest.mark.timeout(100)
# def test_reset_merged_identities(testdb):
# 	testdb.add_filler(generic.SourcesFiller(source=['GitHub',],source_urlroot=['github.com',]))
# 	testdb.add_filler(generic.PackageFiller(package_list_file='packages.csv',data_folder=os.path.join(os.path.dirname(__file__),'dummy_data')))
# 	testdb.add_filler(generic.RepositoriesFiller())
# 	testdb.add_filler(github_rest.ForksFiller(fail_on_wait=True,workers=workers))
# 	testdb.add_filler(generic.ClonesFiller(data_folder='dummy_clones')) # Clones after forks to have up-to-date repo URLS (detect redirects)
# 	testdb.add_filler(commit_info.CommitsFiller(data_folder='dummy_clones')) # Commits after forks because fork info needed for repo commit ownership
# 	testdb.add_filler(github_rest.GHLoginsFiller(fail_on_wait=True,workers=workers))
# 	testdb.fill_db()
# 	count = testdb.count_users()
# 	testdb.reset_merged_identities()
# 	assert testdb.count_users() == testdb.count_identities(), 'There should be as many users as identities'
# 	testdb.fillers = []
# 	testdb.add_filler(github_rest.GHLoginsFiller(fail_on_wait=True,workers=workers,force=True))
# 	testdb.fill_db()
# 	assert testdb.count_users() == count

# @pytest.mark.timeout(100)
# def test_metafiller(testdb):
# 	testdb.add_filler(meta_fillers.DummyMetaFiller(fail_on_wait=True))
# 	testdb.fill_db()
