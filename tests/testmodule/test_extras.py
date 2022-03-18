
import repo_tools
from repo_tools.fillers import generic,commit_info,github_gql,meta_fillers
from repo_tools.extras import pseudonymize,exports,errors
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
	# db.clean_db()
	db.init_db()
	yield db
	db.connection.close()
	del db

@pytest.fixture(params=dbtype_list)
def dest_db(request):
	db = repo_tools.repo_database.Database(db_name='travis_ci_test_repo_tools_export',db_type=request.param,data_folder='dummy_clones')
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
	res = [r[0] for r in testdb.cursor.fetchall()]
	ans = []
	for r in res:
		prefix = r.split('@')[0]
		if re.match(r'^[a-fA-F\d]{32}$',prefix) is None:
			ans.append(('identity',r))


	testdb.cursor.execute('''SELECT u.creation_identity FROM users u
						INNER JOIN identity_types it
						ON it.id=u.creation_identity_type_id AND it.name='email'
						AND NOT u.is_bot;''')
	res = [r[0] for r in testdb.cursor.fetchall()]
	for r in res:
		prefix = r.split('@')[0]
		if re.match(r'^[a-fA-F\d]{32}$',prefix) is None or not r.endswith('_HASHED'):
			ans.append(('user',r))

	assert ans == []


@pytest.mark.timeout(20)
def test_check_db_equal(testdb,dest_db):
	assert exports.check_db_equal(db=testdb,other_db=testdb)
	assert not exports.check_db_equal(db=testdb,other_db=dest_db)


@pytest.mark.timeout(30)
def test_tables_info(testdb):
	exports.get_tables_info(testdb)

@pytest.mark.timeout(30)
def test_autoexport(testdb):
	try:
		exports.export(orig_db=testdb,dest_db=testdb)
		raise ValueError('Should have raised an error while trying to export a db to itself')
	except errors.RepoToolsExportSameDBError:
		pass

@pytest.mark.timeout(30)
def test_export(testdb,dest_db):
	exports.export(orig_db=testdb,dest_db=dest_db)

@pytest.mark.timeout(30)
def test_dump(testdb):
	try:
		exports.dump_pg_csv(db=testdb,output_folder=os.path.join(os.path.dirname(__file__),'dump_pg'))
	except errors.RepoToolsDumpSQLiteError:
		if testdb.db_type == 'sqlite':
			return
		else:
			raise
	if testdb.db_type == 'sqlite':
		raise ValueError('Should have raised an error for dumping a SQLite DB')