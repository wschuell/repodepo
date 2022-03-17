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
				for t in tables_info.keys():
					dest_db.cursor.execute('''ALTER TABLE {table} DISABLE TRIGGER ALL;'''.format(table=t))
			for t,columns in tables_info.items():
				dest_db.logger.info('Exporting table {}'.format(t))
				table_data = get_table_data(table=t,columns=columns,db=orig_db)
				insert_table_data(table=t,columns=columns,db=dest_db,table_data=table_data)
			if dest_db.db_type == 'postgres':
				for t in tables_info.keys():
					dest_db.cursor.execute('''ALTER TABLE {table} ENABLE TRIGGER ALL;'''.format(table=t))
				fix_sequences(db=dest_db)
			dest_db.connection.commit()

def clean(db):
	'''
	Certain number of steps to prepare the dataset for release, not including pseudonymization
	'''
	pass



	def dump_pg_to_sqlite(self,other_db):
		'''
		Wrapper to move the db (postgres) to another with sqlite.
		Table names are used as variables, and the (self-made) check against injection expects alphanumeric chars or _
		DB structure has to be the same, and especially attribute order.
		Maybe attributes could be retrieved and specified also as variables to be safer in terms of data integrity.
		'''
		if not (self.db_type == 'postgres' and other_db.db_type == 'sqlite'):
			raise NotImplementedError('Dumping is only available from PostgreSQL to SQLite. Trying to dump from {} to {}.'.format(self.db_type,other_db.db_type))
		else:
			self.logger.info('Dumping PostgreSQL {} into SQLite {}'.format(self.db_conninfo['db_name'],other_db.db_conninfo['db_name']))
			self.cursor.execute('''SELECT table_name FROM information_schema.tables
							WHERE table_schema = (SELECT current_schema());''')
			tab_list = [r[0] for r in self.cursor.fetchall()]
			for i,tab in enumerate(tab_list):
				check_sqlname_safe(tab)
				other_db.cursor.execute(''' SELECT * FROM {} LIMIT 1;'''.format(tab))
				res = list(other_db.cursor.fetchall())
				if len(res) == 0:
					self.logger.info('Dumping table {} ({}/{})'.format(tab,i+1,len(tab_list)))
					self.cursor.execute(''' SELECT * from {} LIMIT 1;'''.format(tab))
					resmain_sample = list(self.cursor.fetchall())
					if len(resmain_sample) > 0:
						nb_attr = len(resmain_sample[0])
						self.cursor.execute(''' SELECT * from {};'''.format(tab))
						other_db.cursor.executemany('''INSERT OR IGNORE INTO {} VALUES({});'''.format(tab,','.join(['?' for _ in range(nb_attr)])),self.cursor.fetchall())
						other_db.connection.commit()
				else:
					self.logger.info('Skipping table {} ({}/{})'.format(tab,i+1,len(tab_list)))
