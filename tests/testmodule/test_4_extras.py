
import repodepo
from repodepo.fillers import generic,commit_info,github_gql,meta_fillers,bot_fillers
from repodepo.extras import anonymize,exports,errors,stats,anonymization
import pytest
import datetime
import time
import os
import re
import sqlite3

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
	# db.clean_db()
	db.init_db()
	db.add_filler(bot_fillers.BotFiller(identity_type='email',pattern='%@gmail.com'))
	db.add_filler(bot_fillers.BotUserFiller())
	db.fill_db()
	yield db
	db.connection.close()
	del db

@pytest.fixture(params=dbtype_list)
def dest_db(request):
	db = repodepo.repo_database.Database(db_name='travis_ci_test_repo_tools_export',db_type=request.param,data_folder='dummy_clones')
	db.clean_db()
	db.init_db()
	yield db
	db.connection.close()
	del db

@pytest.fixture(params=dbtype_list)
def dest_db_exported(request):
	orig_db = repodepo.repo_database.Database(db_name='travis_ci_test_repo_tools',db_type=request.param,data_folder='dummy_clones')
	db = repodepo.repo_database.Database(db_name='travis_ci_test_repo_tools_export',db_type=request.param,data_folder='dummy_clones')
	db.clean_db()
	db.init_db()
	exports.export(orig_db=orig_db,dest_db=db)
	yield db
	db.connection.close()
	orig_db.connection.close()
	del db
	del orig_db

@pytest.fixture(params=dbtype_list)
def dest_db_anon(request):
	orig_db = repodepo.repo_database.Database(db_name='travis_ci_test_repo_tools',db_type=request.param,data_folder='dummy_clones')
	db = repodepo.repo_database.Database(db_name='travis_ci_test_repo_tools_export',db_type=request.param,data_folder='dummy_clones')
	db.clean_db()
	db.init_db()
	exports.export(orig_db=orig_db,dest_db=db)
	anonymize(db=db)
	yield db
	db.connection.close()
	orig_db.connection.close()
	del db
	del orig_db

workers = 5

##############

#### Tests



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
		exports.dump_pg_csv(db=testdb,output_folder=os.path.join(os.path.dirname(__file__),'dump_pg'),force=True)
	except errors.RepoToolsDumpSQLiteError:
		if testdb.db_type == 'sqlite':
			return
		else:
			raise
	if testdb.db_type == 'sqlite':
		raise ValueError('Should have raised an error for dumping a SQLite DB')


@pytest.mark.timeout(30)
def test_dump_nopsql(testdb):
	try:
		exports.dump_pg_csv(db=testdb,output_folder=os.path.join(os.path.dirname(__file__),'dump_pg_nopsql'),csv_psql=False,force=True)
	except errors.RepoToolsDumpSQLiteError:
		if testdb.db_type == 'sqlite':
			return
		else:
			raise
	if testdb.db_type == 'sqlite':
		raise ValueError('Should have raised an error for dumping a SQLite DB')


@pytest.mark.timeout(30)
def test_dump_error(testdb):
	try:
		exports.dump_pg_csv(db=testdb,output_folder=os.path.join(os.path.dirname(__file__),'dump_pg'),force=False,quiet_error=False)
	except errors.RepoToolsDumpSQLiteError:
		if testdb.db_type == 'sqlite':
			return
		else:
			raise
	except errors.RepoToolsDumpPGError:
		return
	if testdb.db_type == 'sqlite':
		raise ValueError('Should have raised an error for dumping a SQLite DB')
	else:
		raise ValueError('Should have raised an error for dumping in a folder already containing dumped files')

def test_stats(testdb):
	stats.GlobalStats(db=testdb)

@pytest.mark.timeout(20)
def test_anonymize_emails(dest_db_exported):
	anonymization.anonymize_emails(db=dest_db_exported)
	dest_db_exported.cursor.execute('''SELECT i.identity FROM identities i
						INNER JOIN identity_types it
						ON it.id=i.identity_type_id AND it.name='email'
						AND NOT i.is_bot;''')
	res = [r[0] for r in dest_db_exported.cursor.fetchall()]
	ans = []
	for r in res:
		prefix = r.split('@')[0]
		if re.match(r'^[a-fA-F\d]{32}$',prefix) is None:
			ans.append(('identity',r))


	dest_db_exported.cursor.execute('''SELECT u.creation_identity FROM users u
						INNER JOIN identity_types it
						ON it.id=u.creation_identity_type_id AND it.name='email'
						AND NOT u.is_bot;''')
	res = [r[0] for r in dest_db_exported.cursor.fetchall()]
	for r in res:
		prefix = r.split('@')[0]
		if re.match(r'^[a-fA-F\d]{32}$',prefix) is None:# or not r.endswith('_HASHED'):
			ans.append(('user',r))

	assert ans == []

@pytest.mark.timeout(20)
def test_anonymize(dest_db_exported):
	anonymize(db=dest_db_exported)

@pytest.mark.timeout(20)
def test_clean_table(dest_db_anon):
	exports.clean_table(db=dest_db_anon,table='stars')

@pytest.mark.timeout(20)
def test_clean_attr(dest_db_anon):
	exports.clean_attr(db=dest_db_anon,table='stars',attr='inserted_at')

@pytest.mark.timeout(20)
def test_stats(dest_db_anon):
	stats.GlobalStats(db=dest_db_anon).get_result()

@pytest.mark.timeout(20)
def test_export_filters(dest_db_anon):
	exports.export_filters(db=dest_db_anon)

@pytest.mark.timeout(20)
def test_export_bots(dest_db_anon):
	exports.export_bots(db=dest_db_anon)
