import string
import random
import sqlite3
import copy
import csv
import os
from collections import OrderedDict
import oyaml as yaml
from sh import pg_dump,psql
import psycopg2
import psycopg2.extras
from . import errors
from . import check_sqlname_safe

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

def get_tables_info(db,as_yml=False,keep_dbinfo=False):
	'''
	Returns a dict {tablename:[attr_list]} from either postgres or sqlite DB
	'''
	ans = OrderedDict()
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

	if not keep_dbinfo and '_dbinfo' in ans.keys():
		del ans['_dbinfo']
	if as_yml:
		return yaml.dump(ans)
	else:
		return ans


def get_table_data(table,columns,db,batch_size=None,use_multiqueries=False,page_size=None):
	'''
	gets a generator outputing the rows of the table
	'''
	check_sqlname_safe(table)

	if (not use_multiqueries) and db.db_type == 'postgres':
		cursor = db.connection.cursor(name='cursor_{}'.format(table))
		if page_size is not None:
			cursor.itersize = page_size
	else:
		cursor = db.cursor
		if db.db_type == 'postgres':
			orig_itersize = cursor.itersize
			if page_size is not None:
				cursor.itersize = page_size

	for c in columns:
		check_sqlname_safe(c)

	if batch_size is None:
		cursor.execute('''
			SELECT {columns} FROM {table}
			;'''.format(columns=','.join(columns),table=table))
		return cursor.fetchall()
	else:
		def ans_gen():
			counter = 0
			if not use_multiqueries:
				cursor.execute('''
						SELECT {columns} FROM {table}
					;'''.format(columns=','.join(columns),table=table))
			while True:
				if use_multiqueries:
					cursor.execute('''
							SELECT {columns} FROM {table}
							LIMIT {limit} OFFSET {offset}
						;'''.format(columns=','.join(columns),table=table,limit=batch_size,offset=counter))
					rows = list(cursor.fetchall())
				else:
					rows = cursor.fetchmany(batch_size)
				if not rows:
					break
				else:
					if counter != 0 or len(rows) == batch_size:
						db.logger.info('Fetched {} rows of table {}'.format(counter+len(rows),table))
						counter += len(rows)
					for r in rows:
						yield r
					if len(rows) < batch_size:
						break
			if db.db_type == 'postgres' and use_multiqueries:
				cursor.itersize = orig_itersize
		return ans_gen()

def insert_table_data(table,columns,db,table_data,page_size=10**5):
	check_sqlname_safe(table)
	for c in columns:
		check_sqlname_safe(c)
	if db.db_type == 'postgres':
		psycopg2.extras.execute_batch(db.cursor,'''
			INSERT INTO {table}({columns}) VALUES ({separators}) ON CONFLICT DO NOTHING
			;'''.format(columns=','.join(columns),table=table,separators=','.join(['%s' for _ in columns]))
			,(td for td in table_data),page_size=page_size)
	else:
		db.cursor.executemany('''
			INSERT OR IGNORE INTO {table}({columns}) VALUES ({separators})
			;'''.format(columns=','.join(columns),table=table,separators=','.join(['?' for _ in columns]))
			,(td for td in table_data))

def fix_sequences(db):
	if db.db_type == 'postgres':
		db.logger.info('Fixing sequences')
		db.cursor.execute('''
				SELECT 'SELECT SETVAL(' ||
						quote_literal(quote_ident(PGT.schemaname) || '.' || quote_ident(S.relname)) ||
						', COALESCE(MAX(' ||quote_ident(C.attname)|| '), 1) ) FROM ' ||
						quote_ident(PGT.schemaname)|| '.'||quote_ident(T.relname)|| ';'
				FROM pg_class AS S,
					pg_namespace AS NS,
					pg_depend AS D,
					pg_class AS T,
					pg_attribute AS C,
					pg_tables AS PGT
				WHERE S.relkind = 'S'
					AND S.relnamespace = NS.oid
					AND NS.nspname = (SELECT current_schema())
					AND S.oid = D.objid
					AND D.refobjid = T.oid
					AND D.refobjid = C.attrelid
					AND D.refobjsubid = C.attnum
					AND T.relname = PGT.tablename
					AND PGT.schemaname=(SELECT current_schema())
				ORDER BY S.relname;
				--adapted from https://wiki.postgresql.org/wiki/Fixing_Sequences
			''')
		commands = [r[0] for r in db.cursor.fetchall()]
		for c in commands:
			db.cursor.execute(c)
		db.connection.commit()

def export(orig_db,dest_db,page_size=10**5,ignore_error=False,force=False,batch_size=10**6):
	'''
	Exporting data from one database to another, being SQLite or PostgreSQL for both
	'''
	if check_db_equal(orig_db,dest_db):
		# orig_db.logger.info('Cannot export to self, skipping')
		raise errors.RepoToolsExportSameDBError
	else:
		dest_t_info = get_tables_info(dest_db,keep_dbinfo=True)
		if len(dest_t_info) == 0:
			dest_db.init_db()
		elif '_dbinfo' not in dest_t_info.keys():
			if ignore_error:
				dest_db.logger.info('''Skipping export from {orig_db}({orig_db_type}) to {destdb}({destdb_type}):
destination is not empty but has no _dbinfo table'''.format(orig_db=orig_db.db_name,
															orig_db_type=orig_db.db_type,
															destdb=dest_db.db_name,
															destdb_type=dest_db.db_type))
				return
			else:
				raise errors.RepoToolsDBStructError('The destination database does not have the proper structure.')

		orig_db.cursor.execute('''SELECT info_content FROM _dbinfo WHERE info_type='uuid';''')
		orig_uuid = orig_db.cursor.fetchone()[0]

		dest_db.cursor.execute('''SELECT info_content FROM _dbinfo WHERE info_type='exported_from';''')
		exportedfrom_uuid = dest_db.cursor.fetchone()
		if exportedfrom_uuid is not None:
			exportedfrom_uuid = exportedfrom_uuid[0]

		dest_db.cursor.execute('''SELECT info_content FROM _dbinfo WHERE info_type='finished_exported_from';''')
		finished_exportedfrom_uuid = dest_db.cursor.fetchone()
		if finished_exportedfrom_uuid is not None:
			finished_exportedfrom_uuid = finished_exportedfrom_uuid[0]

		if orig_uuid is None:
			raise errors.RepoToolsError('No UUID for origin database')
		elif exportedfrom_uuid == orig_uuid and finished_exportedfrom_uuid == orig_uuid:
			orig_db.logger.info('Export already done, skipping')
		elif exportedfrom_uuid is not None and exportedfrom_uuid != orig_uuid:
			raise errors.RepoToolsError('Trying to export in a non empty DB, already result of an export but from a different source DB')
		else:
			if dest_db.db_type == 'postgres':
				dest_db.cursor.execute('''INSERT INTO _dbinfo(info_type,info_content) VALUES ('exported_from',%(orig_uuid)s) ON CONFLICT DO NOTHING;''',{'orig_uuid':orig_uuid})
			else:
				dest_db.cursor.execute('''INSERT OR IGNORE INTO _dbinfo(info_type,info_content) VALUES ('exported_from',:orig_uuid);''',{'orig_uuid':orig_uuid})
			tables_info = get_tables_info(db=orig_db)
			tables_info_dest = get_tables_info(db=dest_db)
			if dest_db.db_type == 'postgres':
				dest_db.cursor.execute(disable_triggers_cmd(db=dest_db,tables_info=tables_info_dest))
				if dest_db.connection.server_version >= 90600:
					dest_db.cursor.execute('''SET SESSION idle_in_transaction_session_timeout = 0;''')
				else:
					dest_db.logger.warning('You may experience failure of export due to parameter idle_in_transaction_session_timeout not existing in PostgreSQL<9.6')
			try:
				for t,columns in tables_info.items():
					check_sqlname_safe(t)
					if t in tables_info_dest.keys():
						dest_db.cursor.execute('SELECT 1 FROM {} LIMIT 1;'.format(t))
						if dest_db.cursor.fetchone() == (1,) and not force:
							dest_db.logger.info('Skipping table {}, already exported'.format(t))
							continue
						dest_db.logger.info('Exporting table {}'.format(t))
						table_data = get_table_data(table=t,columns=columns,db=orig_db,batch_size=batch_size,page_size=page_size) # as a generator
						insert_table_data(table=t,columns=columns,db=dest_db,table_data=table_data,page_size=page_size)
						dest_db.connection.commit()
					else:
						dest_db.logger.info('Skipping table {}, not in schema of destination DB'.format(t))
				if dest_db.db_type == 'postgres':
					dest_db.cursor.execute(enable_triggers_cmd(db=dest_db,tables_info=tables_info_dest))
					fix_sequences(db=dest_db)
				if dest_db.db_type == 'postgres':
					dest_db.cursor.execute('''INSERT INTO _dbinfo(info_type,info_content) VALUES ('finished_exported_from',%(orig_uuid)s);''',{'orig_uuid':orig_uuid})
				else:
					dest_db.cursor.execute('''INSERT INTO _dbinfo(info_type,info_content) VALUES ('finished_exported_from',:orig_uuid);''',{'orig_uuid':orig_uuid})
			except:
				# closing connection manually because idle_in_transaction_session_timeout is infinite
				if dest_db.connection.closed == 0:
					dest_db.connection.close()
				raise
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


def clean_table(db,table,autocommit=True):
	check_sqlname_safe(table)
	db.cursor.execute('''DROP TABLE IF EXISTS {table} ;'''.format(table=table))
	if autocommit:
		db.connection.commit()

def attr_script_removal(sql_script,attr):
	if isinstance(attr,str):
		attr_list = [attr]
	else:
		attr_list = attr
	prefix = sql_script.split('(')[0]
	suffix = sql_script.split(')')[-1]
	main = '('.join(')'.join(sql_script.split(')')[:-1]).split('(')[1:])
	lines = main.split(',')
	new_lines = []
	for l in lines:
		flagged = False
		for a in attr_list:
			trimmed_l = l.replace('\n','').replace('\t','')
			while trimmed_l.startswith(' '):
				trimmed_l = trimmed_l[1:]
			if trimmed_l.startswith('{} '.format(a)):
				flagged = True
				break
		if not flagged:
			new_lines.append(l)
	return '{prefix}({middle}){suffix}'.format(prefix=prefix,suffix=suffix,middle=','.join(new_lines))


def clean_attr(db,table,attr,autocommit=True):
	check_sqlname_safe(table)
	if isinstance(attr,str):
		attr_list = [attr]
	else:
		attr_list = attr
	for a in attr_list:
		check_sqlname_safe(a)

	t_info = get_tables_info(db=db)
	to_remove = (set(attr_list) & set(t_info[table]))
	if table in t_info.keys() and to_remove:
		if db.db_type == 'sqlite' and sqlite3.sqlite_version < '3.37':
			# raise NotImplementedError('ALTER TABLE t DROP COLUMN not implemented for this version of SQLite, upgrade to >=3.37.0, drop whole tables, or clean in postgres and export')

			db.cursor.execute('''SELECT sql FROM sqlite_master WHERE tbl_name = '{table}';'''.format(table=table))
			table_sql = db.cursor.fetchone()[0]

			db.cursor.execute('''SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name='{table}' AND sql IS NOT NULL;'''.format(table=table))
			idx_list = [r[0] for r in db.cursor.fetchall()]

			db.cursor.execute('''ALTER TABLE {table} RENAME TO __old__{table};'''.format(table=table))

			clean_table_sql = table_sql
			for a in attr_list:
				clean_table_sql = attr_script_removal(sql_script=clean_table_sql,attr=a) # spot and remove target attribute

			db.cursor.execute(clean_table_sql)

			orig_attr_list = copy.deepcopy(t_info[table])
			remaining_attr_list = [a for a in orig_attr_list if a not in to_remove]
			db.cursor.execute('''INSERT INTO {table}({attr_list}) SELECT {attr_list} FROM __old__{table};'''.format(table=table,attr_list=','.join(remaining_attr_list)))

			db.cursor.execute('''DROP TABLE __old__{table};'''.format(table=table))

			for idx_sql in idx_list:
				cleaned_idx_sql = idx_sql
				to_discard = False
				for a in attr_list:
					cleaned_idx_sql = cleaned_idx_sql.replace('{},'.format(a),'').replace(',{}'.format(a),'')
					if '({})'.format(a) in cleaned_idx_sql:
						to_discard = True
				if to_discard:
					db.logger.info('Skipping index: {}'.format(idx_sql))
				else:
					db.cursor.execute(cleaned_idx_sql)

		else:
			for a in attr_list:
				db.cursor.execute('''ALTER TABLE {table} DROP COLUMN {attr} ;'''.format(table=table,attr=a))
	if autocommit:
		db.connection.commit()

def clean(db,*,inclusion_list=None,exclusion_list=None,autocommit=False,vacuum_sqlite=True):
	'''
	Certain number of steps to prepare the dataset for release, not including pseudonymization
	'''
	if autocommit is False:
		db.cursor.execute('BEGIN TRANSACTION;')
	if exclusion_list is not None and inclusion_list is not None:
		raise SyntaxError('Both exclusion_list and inclusion_list args cannot be provided, pick one method')
	t_info = get_tables_info(db=db,keep_dbinfo=True)

	if exclusion_list is not None:
		for t in exclusion_list.keys():
			if t in t_info.keys():
				if len(set(t_info[t]) - set(exclusion_list[t])) == 0 or len(exclusion_list[t]) == 0:
					clean_table(db=db,table=t,autocommit=autocommit)
				else:
					clean_attr(db=db,table=t,attr=exclusion_list[t],autocommit=autocommit)

	elif inclusion_list is not None:
		for t in set(t_info.keys())-set(inclusion_list.keys()):
			clean_table(db=db,table=t,autocommit=autocommit)
		for t in inclusion_list.keys():
			if t in t_info.keys():
				if len(inclusion_list[t]) != 0:
					clean_attr(db=db,table=t,attr=set(t_info[t])-set(inclusion_list[t]),autocommit=autocommit)

	if not autocommit:
		# db.cursor.execute('COMMIT;')
		db.connection.commit()
	if db.db_type == 'sqlite' and vacuum_sqlite:
		db.cursor.execute('VACUUM;')

def dump_pg_csv(db,output_folder,import_dump=True,schema_dump=True,csv_dump=True,csv_psql=True,force=False,quiet_error=True):
	'''
	Dumping a postgres DB to schema.sql, import.sql and one CSV per table
	'''

	if not db.db_type == 'postgres':
		raise errors.RepoToolsDumpSQLiteError('Trying to dump to schema and CSV from a SQLite DB, should be PostgreSQL')

	if not os.path.exists(os.path.join(output_folder,'data')):
		os.makedirs(os.path.join(output_folder,'data'))

	tables_info = get_tables_info(db=db)

	for filename,bool_var in [('schema.sql',schema_dump),('import.sql',import_dump)]+[('data/{}.csv'.format(t),csv_dump) for t in sorted(tables_info.keys())]:
		filepath = os.path.join(output_folder,filename)
		if os.path.exists(filepath) and bool_var:
			if force:
				db.logger.warning('Removing {} for replacement'.format(filename))
				os.remove(filepath)
			elif quiet_error:
				db.logger.warning('While dumping: {} already exists. Use force=True to replace existing files.'.format(filename))
				return
			else:
				raise errors.RepoToolsDumpPGError('Error while dumping: {} already exists. Use force=True to replace.'.format(filename))

	db.logger.info('Dumping DB to folder')

	###### schema.sql ######
	if schema_dump:
		with open(os.path.join(output_folder,'schema.sql'),'w') as f:
			pg_dump('-h', db.db_conninfo['host'],
					'-U', db.db_conninfo['db_user'],
					db.db_conninfo['db_name'],
					'-p',db.db_conninfo['port'],
					'--schema-only',
					'--no-owner',
					'--no-privileges',
					'--no-security-labels',
					'--no-tablespaces',
					_out=f)



	###### import.sql ######
	if import_dump:
		copy_tables_str = '\n'.join(['''\\copy {table} ({columns}) FROM 'data/{table}.csv' WITH CSV HEADER;'''.format(table=t,columns=','.join(col)) 
				for t,col in sorted(tables_info.items())])

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

	###### CSV files ######
	# header, then each line.
	if csv_dump:
		if csv_psql:
			copy_tables_str = '\n'.join(['''\\copy {table} ({columns}) TO '{folder_table}.csv' WITH CSV HEADER;'''.format(table=t,
																								columns=','.join(col),
																								folder_table=os.path.join(output_folder,'data',t)) 
				for t,col in sorted(tables_info.items())])
			psql('-h', db.db_conninfo['host'], '-U', db.db_conninfo['db_user'], db.db_conninfo['db_name'],'-p',db.db_conninfo['port'],_in=copy_tables_str)
		else:
			for t,col in tables_info.items():
				with open(os.path.join(output_folder,'data','{}.csv'.format(t)),'w') as f:
					writer = csv.writer(f)
					writer.writerow(col)
					db.cursor.execute('SELECT {columns} FROM {table};'.format(table=t,columns=','.join(col)))
					for r in db.cursor.fetchall():
						writer.writerow(r)
	

	####### script.sh
	script_sh_content ='''#!/bin/bash
set -e
echo 'Database name? (default rust_repos)'
read DBNAME
if [ -z "$DBNAME" ]
then
	DBNAME="rust_repos"
fi

echo 'Database host? (default localhost)'
read DBHOST
if [ -z "$DBHOST" ]
then
	DBHOST="localhost"
fi

echo 'Database port? (default 5432)'
read DBPORT
if [ -z "$DBPORT" ]
then
	DBPORT="5432"
fi

echo 'Database user? (default postgres)'
read DBUSER
if [ -z "$DBUSER" ]
then
	DBUSER="postgres"
fi

echo "Create database $DBNAME? (empty=yes)"
read DBCREATE
if [ -z "$DBCREATE" ]
then
	psql -q -h $DBHOST --port=$DBPORT --user=$DBUSER -c "create database $DBNAME;"
fi

psql -q -h $DBHOST --port=$DBPORT --user=$DBUSER $DBNAME < schema.sql
psql -q -h $DBHOST --port=$DBPORT --user=$DBUSER $DBNAME < import.sql

echo "Finishing importing data into $DBNAME"
'''
	with open(os.path.join(output_folder,'script.sh'),'w') as f:
		f.write(script_sh_content)

	db.logger.info('Dumped DB to folder')


def export_filters(db,folder=None,overwrite=False):
	if folder is None:
		folder = db.data_folder

	if not os.path.exists(folder):
		os.makedirs(folder)

	# packages
	db.cursor.execute('''
		SELECT s.name,p.name FROM filtered_deps_package fdp
		INNER JOIN packages p
		ON p.id=fdp.package_id
		INNER JOIN sources s
		ON s.id=p.source_id
		;''')

	res = list(db.cursor.fetchall())
	filepath = os.path.join(folder,'filtered_packages.csv')
	if not overwrite and os.path.exists(filepath):
		with open(filepath,'r') as f:
			reader = csv.reader(f)
			next(reader)
			previous_res = [tuple(r) for r in reader] 
			res = sorted(list(set(previous_res+res)))

		
	with open(filepath,'w') as f:
		f.write('source,package\n')
		for s,p in res:
			f.write('"{}","{}"\n'.format(s,p))

	# repos
	db.cursor.execute('''
		SELECT s.name,r.owner,r.name FROM filtered_deps_repo fdr
		INNER JOIN repositories r
		ON r.id=fdr.repo_id
		INNER JOIN sources s
		ON s.id=r.source
		;''')

	res = list(db.cursor.fetchall())
	filepath = os.path.join(folder,'filtered_repos.csv')
	if not overwrite and os.path.exists(filepath):
		with open(filepath,'r') as f:
			reader = csv.reader(f)
			next(reader)
			previous_res = [(s,r.split('/')[0],'/'.join(r.split('/')[1:])) for s,r in reader] 
			res = sorted(list(set(previous_res+res)))


	with open(filepath,'w') as f:
		f.write('source,repo\n')
		for s,o,n in res:
			f.write('"{}","{}/{}"\n'.format(s,o,n))

	# repo_edges
	db.cursor.execute('''
		SELECT ss.name,rs.owner,rs.name,sd.name,rd.owner,rd.name FROM filtered_deps_repoedges fdre
		INNER JOIN repositories rs
		ON rs.id=fdre.repo_source_id 
		INNER JOIN sources ss
		ON ss.id=rs.source
		INNER JOIN repositories rd
		ON rd.id=fdre.repo_dest_id 
		INNER JOIN sources sd
		ON sd.id=rd.source
		;''')

	res = list(db.cursor.fetchall())
	filepath = os.path.join(folder,'filtered_repoedges.csv')
	if not overwrite and os.path.exists(filepath):
		with open(filepath,'r') as f:
			reader = csv.reader(f)
			next(reader)
			previous_res = [(ss,rs.split('/')[0],'/'.join(rs.split('/')[1:]),sd,rd.split('/')[0],'/'.join(rd.split('/')[1:])) for ss,rs,sd,rd in reader] 
			res = sorted(list(set(previous_res+res)))

		
	with open(filepath,'w') as f:
		f.write('source_source,repo_source,source_dest,repo_dest\n')
		for s,o,n,s2,o2,n2 in res:
			f.write('"{}","{}/{}","{}","{}/{}"\n'.format(s,o,n,s2,o2,n2))

	# package_edges
	db.cursor.execute('''
		SELECT ss.name,ps.name,sd.name,pd.name FROM filtered_deps_packageedges fdpe
		INNER JOIN packages ps
		ON ps.id=fdpe.package_source_id 
		INNER JOIN sources ss
		ON ss.id=ps.source_id
		INNER JOIN packages pd
		ON pd.id=fdpe.package_dest_id 
		INNER JOIN sources sd
		ON sd.id=pd.source_id
		;''')

	res = list(db.cursor.fetchall())
	filepath = os.path.join(folder,'filtered_packageedges.csv')
	if not overwrite and os.path.exists(filepath):
		with open(filepath,'r') as f:
			reader = csv.reader(f)
			next(reader)
			previous_res = [tuple(r) for r in reader]
			res = sorted(list(set(previous_res+res)))
		
	with open(filepath,'w') as f:
		f.write('source_source,package_source,source_dest,package_dest\n')
		for s,n,s2,n2 in res:
			f.write('"{}","{}","{}","{}"\n'.format(s,n,s2,n2))


def export_bots(db,folder=None):
	if folder is None:
		folder = db.data_folder

	if not os.path.exists(folder):
		os.makedirs(folder)

	db.cursor.execute('''
		SELECT it.name,i.identity FROM identities i
		INNER JOIN identity_types it
		ON it.id=i.identity_type_id
		AND i.is_bot
		;''')

	with open(os.path.join(folder,'bots.csv'),'w') as f:
		f.write('identity_type,identity\n')
		for s,p in db.cursor.fetchall():
			f.write('"{}","{}"\n'.format(s,p))
