

import secrets

from .. import fillers

import time
import os
import hashlib

import psycopg2
import psycopg2.sql

from . import check_sqlname_safe
from . import errors

def modify(old_id,salt):
	if old_id is None:
		return None
	parts = old_id.split('@')
	prefix = parts[0]
	suffix = '@'.join(parts[1:])
	if len(suffix) > 0:
		suffix = '@' + suffix
	return hashlib.md5((salt+prefix).encode()).hexdigest()+suffix



def anonymize(db,salt=None,keep_email_suffixes=True,ignore_error=False):
	try:
		db.add_filler(AnonymizationMetaFiller(salt=salt,keep_email_suffixes=keep_email_suffixes))
		db.fill_db()
	except errors.RepoToolsDBStructError:
		if ignore_error:
			db.logger.info('Skipping anonymization step, DB seems to have already be cleaned (_dbinfo table missing)')
		else:
			raise




def anonymize_emails(db,salt=None):

	db.add_filler(EmailAnonFiller(table='identities',field='identity',it_field='identity_type_id',salt=salt))
	db.add_filler(EmailAnonFiller(table='users',field='creation_identity',it_field='creation_identity_type_id',salt=salt))
	db.fill_db()


class AnonymizationMetaFiller(fillers.Filler):
	'''
	Combining all the necessary fillers for anonymization
	'''
	def __init__(self,salt,keep_email_suffixes=True,**kwargs):
		self.salt = salt
		self.keep_email_suffixes = keep_email_suffixes
		fillers.Filler.__init__(self,**kwargs)

	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder

		# Emails
		if self.keep_email_suffixes:
			self.db.add_filler(EmailAnonFiller(table='identities',field='identity',it_field='identity_type_id',salt=self.salt))
			self.db.add_filler(EmailAnonFiller(table='users',field='creation_identity',it_field='creation_identity_type_id',salt=self.salt))
		else:
			self.db.add_filler(AnonymizationFiller(table='identities',field='identity',leave_bots=True,filter_identity_type=True,salt=self.salt))
			self.db.add_filler(AnonymizationFiller(table='users',field='creation_identity',leave_bots=True,filter_identity_type=True,salt=self.salt))

		# Names
		self.db.add_filler(NullifyFiller(table='identities',field='attributes',it_field='identity_type_id',salt=self.salt))

		# Logins in various fields
		for table,field in [('followers','follower_login'),
							('stars','login'),
							('sponsors_user','sponsor_login'),
							('sponsors_user','external_id'),
							('sponsors_listings','login'),
							]:
			self.db.add_filler(AnonymizationFiller(table=table,field=field,leave_bots=False,filter_identity_type=False,salt=self.salt))

		# reason field in merged identities
		self.db.add_filler(MergedIDAnonFiller())

class MergedIDAnonFiller(fillers.Filler):
	def apply(self):
		self.db.cursor.execute(''' UPDATE merged_identities SET reason='Parsed email as github_login' WHERE reason LIKE 'Parsed email % github_login%'; ''')
		self.db.cursor.execute(''' UPDATE merged_identities SET reason='Parsed email as gitlab_login' WHERE reason LIKE 'Parsed email % gitlab_login%'; ''')
		self.db.connection.commit()

class AnonymizationFiller(fillers.Filler):
	'''
	Wrapping anonymization steps as fillers -- base structure hashing one field in one table
	'''
	def __init__(self,table,field='login',force=False,id_field=None,salt=None,leave_bots=True,filter_identity_type=True,it_field='identity_type_id',**kwargs):
		fillers.Filler.__init__(self,**kwargs)
		self.table = table
		self.field = field
		if id_field is None:
			self.id_field = field
		else:
			self.id_field = id_field
		self.force = force
		self.salt = salt

		self.leave_bots = leave_bots
		self.it_field = it_field
		self.filter_identity_type = filter_identity_type

	def set_where_clause(self):
		if self.leave_bots and not self.filter_identity_type:
			self.where_clause = 'WHERE NOT is_bot'
		elif self.filter_identity_type:
			if self.db.db_type == 'postgres':
				self.where_clause = '''FROM identity_types it WHERE it.id={it_field} AND it.name='email' '''
			else:
				self.where_clause = '''INNER JOIN identity_types it ON it.id={it_field} AND it.name='email' '''
			if self.leave_bots:
				self.where_clause += ' AND NOT is_bot'
		else:
			self.where_clause = ''

	def set_salt(self):
		self.db.cursor.execute('''SELECT info_content FROM _dbinfo WHERE info_type='anonymization_salt'; ''')
		db_salt = self.db.cursor.fetchone()
		if db_salt is not None:
			db_salt = db_salt[0]

		if db_salt is None:
			if self.salt is None:
				self.salt = secrets.token_urlsafe(16)
			if self.db.db_type == 'postgres':
				self.db.cursor.execute('''INSERT INTO _dbinfo(info_type,info_content) SELECT 'anonymization_salt',%(salt)s;''',{'salt':self.salt})
			else:
				self.db.cursor.execute('''INSERT INTO _dbinfo(info_type,info_content) SELECT 'anonymization_salt',:salt;''',{'salt':self.salt})
		elif self.salt is None:
			self.salt = db_salt
		elif self.salt != db_salt:
				raise ValueError('Trying to anonymize with a different salt than the one already used for previous anonymization. DB salt:{} filler salt:{}'.format(db_salt,self.salt))

	def get_update_name(self,table=None,field=None):
		if table is None:
			table = self.table
		if field is None:
			field = self.field
		return 'anonymization__{}__{}'.format(table,field)

	def prepare(self):
		self.db.check_structure()
		self.set_salt()
		self.set_where_clause()
		update_name = self.get_update_name()
		if not self.force:
			if self.db.db_type == 'postgres':
				self.db.cursor.execute('''SELECT COUNT(*) FROM full_updates WHERE update_type=%(update_name)s;''',{'update_name':update_name})
			else:
				self.db.cursor.execute('''SELECT COUNT(*) FROM full_updates WHERE update_type=:update_name;''',{'update_name':update_name})
			if self.db.cursor.fetchone()[0] > 0:
				self.done = True


	def register_update(self,table=None,field=None,autocommit=True):
		update_name = self.get_update_name(table=table,field=field)
		if self.db.db_type == 'postgres':
			self.db.cursor.execute('''INSERT INTO full_updates(update_type) SELECT %(update_name)s;''',{'update_name':update_name})
		else:
			self.db.cursor.execute('''INSERT INTO full_updates(update_type) SELECT :update_name;''',{'update_name':update_name})
		if autocommit:
			self.db.connection.commit()


	def apply(self):
		self.hash_field()
		self.register_update()

	def hash_field(self,id_field=None,field=None,table=None):
		if table is None:
			table = self.table
		if field is None:
			field = self.field
		if id_field is None:
			id_field = self.id_field

		update_name = self.get_update_name()
		if self.db.db_type == 'postgres':
			self.db.cursor.execute(psycopg2.sql.SQL('''
				UPDATE {table}
				SET {field} = MD5(%(salt)s||{field})
				;''').format(field=psycopg2.sql.Identifier(field),id_field=psycopg2.sql.Identifier(id_field),table=psycopg2.sql.Identifier(table)),{'salt':self.salt})
		else:
			check_sqlname_safe(table)
			check_sqlname_safe(field)
			check_sqlname_safe(id_field)
			query = '''
						SELECT {id_field},{field}
						FROM {table}
					;'''.format(field=field,id_field=id_field,table=table)

			self.db.cursor.execute(query)

			to_update = [r for r in self.db.cursor.fetchall()]

			updated = [(iid,modify(old_id=old_val,salt=self.salt)) for iid,old_val in to_update]

			query = '''
				UPDATE {table} SET
							{field}= :new_val
						WHERE {id_field} = :iid
					;
				'''.format(field=field,id_field=id_field,table=table)
			self.db.cursor.executemany(query,
						({'salt':self.salt,'iid':iid,'new_val':new_val} for iid,new_val in updated))
			self.db.cursor.execute('''
				INSERT INTO full_updates(update_type)
				SELECT :update_name;
				''',{'update_name':update_name} )


class EmailAnonFiller(AnonymizationFiller):


	def apply(self):
		self.hash_emails()
		self.register_update()

	def hash_emails(self,id_field=None,field=None,it_field=None,table=None):
		if table is None:
			table = self.table
		if field is None:
			field = self.field
		if id_field is None:
			id_field = self.id_field
		if it_field is None:
			it_field = self.it_field

		update_name = self.get_update_name()
		if self.db.db_type == 'postgres':
			self.db.cursor.execute(psycopg2.sql.SQL('''
				UPDATE {table}
				SET {field} = COALESCE(MD5(%(salt)s||SUBSTRING({field} FROM '[^@]+'))||'@'||SUBSTRING({field} FROM '(?<=@).*'),
															MD5(%(salt)s||{field}))
															{where_clause}
				;''').format(field=psycopg2.sql.Identifier(field),
							id_field=psycopg2.sql.Identifier(id_field),
							it_field=psycopg2.sql.Identifier(it_field),
							table=psycopg2.sql.Identifier(table),
							where_clause=psycopg2.sql.SQL(self.where_clause).format(it_field=psycopg2.sql.Identifier(it_field))),{'salt':self.salt})
		else:
			check_sqlname_safe(table)
			check_sqlname_safe(field)
			check_sqlname_safe(id_field)
			check_sqlname_safe(it_field)
			query = '''
						SELECT {id_field},{field}
						FROM {table}
						{where_clause}
					;'''.format(field=field,id_field=id_field,it_field=it_field,table=table,where_clause=self.where_clause.format(it_field=it_field))

			self.db.cursor.execute(query)

			to_update = [r for r in self.db.cursor.fetchall()]

			updated = [(iid,modify(old_id=old_val,salt=self.salt)) for iid,old_val in to_update]

			query = '''
				UPDATE {table} SET
							{field}= :new_val
						WHERE {id_field} = :iid
					;
				'''.format(field=field,id_field=id_field,table=table)
			self.db.cursor.executemany(query,
						({'salt':self.salt,'iid':iid,'new_val':new_val} for iid,new_val in updated))


class EmptyTableFiller(AnonymizationFiller):

	def apply(self):
		self.empty_table()
		self.register_update()

	def get_update_name(self,table=None,field=None):
		if table is None:
			table = self.table
		if field is None:
			field = self.field
		return 'anonymization__{}'.format(table)

	def empty_table(self):
		if self.db.db_type == 'postgres':
			self.db.cursor.execute(psycopg2.sql.SQL('DELETE FROM {table};').format(table=psycopg2.sql.Identifier(self.table)))
		else:
			check_sqlname_safe(self.table)
			self.db.cursor.execute('DELETE FROM {table};'.format(table=self.table))

class NullifyFiller(AnonymizationFiller):


	def apply(self):
		self.nullify()
		self.register_update()

	def nullify(self,id_field=None,field=None,it_field=None,table=None):
		if table is None:
			table = self.table
		if field is None:
			field = self.field
		if id_field is None:
			id_field = self.id_field
		if it_field is None:
			it_field = self.it_field

		update_name = self.get_update_name()
		if self.db.db_type == 'postgres':
			self.db.cursor.execute(psycopg2.sql.SQL('''
				UPDATE {table}
				SET {field} = NULL
															{where_clause}
				;''').format(field=psycopg2.sql.Identifier(field),
							id_field=psycopg2.sql.Identifier(id_field),
							it_field=psycopg2.sql.Identifier(it_field),
							table=psycopg2.sql.Identifier(table),
							where_clause=psycopg2.sql.SQL(self.where_clause).format(it_field=psycopg2.sql.Identifier(it_field))),{'salt':self.salt})
		else:
			check_sqlname_safe(table)
			check_sqlname_safe(field)
			check_sqlname_safe(id_field)
			check_sqlname_safe(it_field)
			query = '''
						SELECT {id_field},{field}
						FROM {table}
						{where_clause}
					;'''.format(field=field,id_field=id_field,it_field=it_field,table=table,where_clause=self.where_clause.format(it_field=it_field))

			self.db.cursor.execute(query)

			to_update = [r for r in self.db.cursor.fetchall()]

			query = '''
				UPDATE {table} SET
							{field}= NULL
						WHERE {id_field} = :iid
					;
				'''.format(field=field,id_field=id_field,table=table)
			self.db.cursor.executemany(query,
						({'salt':self.salt,'iid':iid} for iid,old_val in to_update))

