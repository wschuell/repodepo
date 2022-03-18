import string
import random
import sqlite3
import psycopg2
import psycopg2.extras
from . import errors

def check_sqlname_safe(s):
	assert s == ''.join( c for c in s if c.isalnum() or c in ('_',) )

def check_db_equal(db,other_db):
	'''
	Checks whether the databases are the same or not by inserting a random value in a new table
	True: they are the same
	False: they are not
	'''
	value = ''.join(random.choice(string.ascii_letters) for i in range(20))

	db.cursor.execute('CREATE TABLE _temp_check_{}(value TEXT);'.format(value))
	db.connection.commit()

	try:
		other_db.cursor.execute('CREATE TABLE _temp_check_{}(value TEXT);'.format(value))
	except psycopg2.errors.DuplicateTable:
		ans = True
	except sqlite3.OperationalError:
		ans = True
	else:
		ans = False
	finally:
		db.connection.commit()
		other_db.connection.commit()
		db.cursor.execute('DROP TABLE IF EXISTS _temp_check_{};'.format(value))
		other_db.cursor.execute('DROP TABLE IF EXISTS _temp_check_{};'.format(value))
		db.connection.commit()
		other_db.connection.commit()

	return ans

def get_tables_info(db):
	'''
	Returns a dict {tablename:[attr_list]} from either postgres or sqlite DB
	'''
	ans = {}
	if db.db_type == 'postgres':
		db.cursor.execute('''SELECT c.table_name,c.column_name FROM information_schema.columns c
							WHERE c.table_schema = (SELECT current_schema())
							ORDER BY c.table_name,c.ordinal_position ;''')
		for tab,col in db.cursor.fetchall():
			try:
				ans[tab].append(col)
			except KeyError:
				ans[tab] = [col]
	else:
		db.cursor.execute('''SELECT name FROM sqlite_master
			WHERE type='table';
			''')
		ans = {t[0]:[] for t in db.cursor.fetchall()}
		for t in ans.keys():
			check_sqlname_safe(t)
			db.cursor.execute('''PRAGMA table_info({table_name});'''.format(table_name=t))
			ans[t] = [r[1] for r in db.cursor.fetchall()]

	if '_dbinfo' in ans.keys():
		del ans['_dbinfo']
	return ans

def get_table_data(table,columns,db):
	'''
	gets a generator outputing the rows of the table
	'''
	check_sqlname_safe(table)
	for c in columns:
		check_sqlname_safe(c)
	db.cursor.execute('''
			SELECT {columns} FROM {table}
			;'''.format(columns=','.join(columns),table=table))
	return db.cursor.fetchall()

def insert_table_data(table,columns,db,table_data):
	check_sqlname_safe(table)
	for c in columns:
		check_sqlname_safe(c)
	if db.db_type == 'postgres':
		psycopg2.extras.execute_batch(db.cursor,'''
			INSERT INTO {table}({columns}) VALUES ({separators})
			;'''.format(columns=','.join(columns),table=table,separators=','.join(['%s' for _ in columns]))
			,table_data)
	else:
		db.cursor.executemany('''
			INSERT INTO {table}({columns}) VALUES ({separators})
			;'''.format(columns=','.join(columns),table=table,separators=','.join(['?' for _ in columns]))
			,table_data)

def fix_sequences(db):
	if db.db_type == 'postgres':
		db.logger.info('Fixing sequences')
		db.cursor.execute('''
				SELECT 'SELECT SETVAL(' ||
						quote_literal(quote_ident(PGT.schemaname) || '.' || quote_ident(S.relname)) ||
						', COALESCE(MAX(' ||quote_ident(C.attname)|| '), 1) ) FROM ' ||
						quote_ident(PGT.schemaname)|| '.'||quote_ident(T.relname)|| ';'
				FROM pg_class AS S,
					pg_depend AS D,
					pg_class AS T,
					pg_attribute AS C,
					pg_tables AS PGT
				WHERE S.relkind = 'S'
					AND S.oid = D.objid
					AND D.refobjid = T.oid
					AND D.refobjid = C.attrelid
					AND D.refobjsubid = C.attnum
					AND T.relname = PGT.tablename
				ORDER BY S.relname;
				--from https://wiki.postgresql.org/wiki/Fixing_Sequences
			''')
		commands = [r[0] for r in db.cursor.fetchall()]
		for c in commands:
			db.cursor.execute(c)
		db.connection.commit()

def export(orig_db,dest_db):
	'''
	Exporting data from one database to another, being SQLite or PostgreSQL for both
	'''
	if check_db_equal(orig_db,dest_db):
		# orig_db.logger.info('Cannot export to self, skipping')
		raise errors.RepoToolsExportSameDBError
	else:
		if len(get_tables_info(dest_db)) == 0:
			dest_db.init_db()

		orig_db.cursor.execute('''SELECT info_content FROM _dbinfo WHERE info_type='uuid';''')
		orig_uuid = orig_db.cursor.fetchone()[0]

		dest_db.cursor.execute('''SELECT info_content FROM _dbinfo WHERE info_type='exported_from';''')
		exportedfrom_uuid = orig_db.cursor.fetchone()
		if exportedfrom_uuid is not None:
			exportedfrom_uuid = exportedfrom_uuid[0]

		if orig_uuid is not None and exportedfrom_uuid == orig_uuid:
			orig_db.logger.info('Export already done, skipping')
		else:
			if dest_db.db_type == 'postgres':
				dest_db.cursor.execute('''INSERT INTO _dbinfo(info_type,info_content) VALUES ('exported_from',%(orig_uuid)s);''',{'orig_uuid':orig_uuid})
			else:
				dest_db.cursor.execute('''INSERT INTO _dbinfo(info_type,info_content) VALUES ('exported_from',:orig_uuid);''',{'orig_uuid':orig_uuid})
			tables_info = get_tables_info(db=orig_db)
			if dest_db.db_type == 'postgres':
				db.cursor.execute(disable_triggers_cmd(db=dest_db,tables_info=tables_info))
			for t,columns in tables_info.items():
				dest_db.logger.info('Exporting table {}'.format(t))
				table_data = get_table_data(table=t,columns=columns,db=orig_db)
				insert_table_data(table=t,columns=columns,db=dest_db,table_data=table_data)
			if dest_db.db_type == 'postgres':
				db.cursor.execute(enable_triggers_cmd(db=dest_db,tables_info=tables_info))
				fix_sequences(db=dest_db)
			dest_db.connection.commit()

def disable_triggers_cmd(db,tables_info=None):
	if tables_info is None:
		tables_info = get_tables_info(db=db)
	for t in tables_info.keys():
		check_sqlname_safe(t)
	return '\n'.join(['''ALTER TABLE {table} DISABLE TRIGGER ALL;\n'''.format(table=t) for t in tables_info.keys()])

def enable_triggers_cmd(db,tables_info=None):
	if tables_info is None:
		tables_info = get_tables_info(db=db)
	for t in tables_info.keys():
		check_sqlname_safe(t)
	return '\n'.join(['''ALTER TABLE {table} ENABLE TRIGGER ALL;\n'''.format(table=t) for t in tables_info.keys()])

def clean(db):
	'''
	Certain number of steps to prepare the dataset for release, not including pseudonymization
	'''
	pass


def dump_pg_csv(db,output_folder):
	'''
	Dumping a postgres DB to schema.sql, import.sql and one CSV per table
	'''
	if not os.path.exists(os.path.join(output_folder,'data')):
		os.makedirs(os.path.join(output_folder,'data'))

	'''\\COPY {} TO {} FROM '''

	'''
	\\copy "categories" ("category", "crates_cnt", "created_at", "description", "id", "path", "slug") FROM 'data/categories.csv' WITH CSV HEADER
	'''

	# schema.sql
	with open(os.path.join(output_folder,'schema.sql'),'w') as f:
		f.write(schema_str)
	# import.sql

	import_str = '''
BEGIN;

-- Disabling Triggers
{disable_trig}

-- Inserting data
{copy_tables}

-- Reenabling Triggers
{enable_trig}

COMMIT;
	'''.format(disable_trig=disable_triggers_cmd(db=db,tables_info=tables_info),
				enable_trig=enable_triggers_cmd(db=db,tables_info=tables_info),
				copy_tables=copy_tables_str)

	with open(os.path.join(output_folder,'import.sql'),'w') as f:
		f.write(import_str)

	# CSV
	# header, then each line.
	'''
	COPY { table_name [ ( column [, ...] ) ] | ( query ) }
    TO { 'filename' | STDOUT }
    [ [ WITH ] ( option [, ...] ) ]
	'''

