
import repodepo
from repodepo.fillers import generic,commit_info,github_gql,meta_fillers
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
	db = repodepo.repo_database.Database(db_name='travis_ci_test_repo_tools',db_type=request.param,data_folder='dummy_clones')
	db.clean_db()
	db.init_db()
	yield db
	db.connection.close()
	del db

workers = 5

##############

#### Tests


@pytest.mark.timeout(10)
def test_urls(testdb):
	testdb.add_filler(generic.SourcesFiller(source='GitHub',source_urlroot='github.com'))
	testdb.add_filler(generic.URLFiller(url_list=['github.com/Wschuell/FabSub'],data_folder=os.path.join(os.path.dirname(__file__),'dummy_data')))
	testdb.add_filler(generic.RepositoriesFiller())
	testdb.add_filler(generic.ClonesFiller(data_folder='dummy_clones'))
	testdb.add_filler(commit_info.CommitsFiller(data_folder='dummy_clones'))
	testdb.add_filler(github_gql.StarsGQLFiller(fail_on_wait=True,workers=workers))
	testdb.fill_db()
