
import repo_tools
from repo_tools.fillers import generic,commit_info,github_gql,meta_fillers
from repo_tools.extras import pseudonymize
import pytest
import datetime
import time
import os
import re

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
	yield db
	db.connection.close()
	del db

workers = 5

##############

#### Tests


@pytest.mark.timeout(20)
def test_pseudonymize(testdb):
	pseudonymize(db=testdb)
	testdb.cursor.execute('''SELECT i.identity FROM identities i
						INNER JOIN identity_types it
						ON it.id=i.identity_type_id AND it.name='email'
						AND NOT i.is_bot;''')
	res = list(testdb.cursor.fetchall())
	ans = []
	for r in res:
		prefix = r.split('@')[0]
		if re.match((r'^[a-fA-F\d]{32}$',prefix)) is None:
			ans.append(('identity',r))


	testdb.cursor.execute('''SELECT u.creation_identity FROM users u
						INNER JOIN identity_types it
						ON it.id=u.creation_identity_type_id AND it.name='email'
						AND NOT u.is_bot;''')
	res = list(testdb.cursor.fetchall())
	for r in res:
		prefix = r.split('@')[0]
		if re.match((r'^[a-fA-F\d]{32}$',prefix)) is None or not r.endswith('_HASHED'):
			ans.append(('user',r))

	assert ans == []