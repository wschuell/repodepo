

import secrets

import repo_tools as rp
from repo_tools import fillers

import time
import os
import hashlib

import psycopg2
import psycopg2.sql

from . import check_sqlname_safe


def modify(old_id,salt):
	parts = old_id.split('@')
	prefix = parts[0]
	suffix = '@'.join(parts[1:])
	if len(suffix) > 0:
		suffix = '@' + suffix
	return hashlib.md5((salt+prefix).encode()).hexdigest()+suffix



def pseudonymize(db,salt=None):
	db.add_filler(PseudonymizationMetaFiller(salt=salt))
	db.fill_db()



class PseudonymizationMetaFiller(fillers.Filler):
	'''
	Combining all the necessary fillers for pseudonymization
	'''
	def __init__(self,salt,**kwargs):
		self.salt = salt
		fillers.Filler.__init__(self,**kwargs)

	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder

		self.db.add_filler(PseudonymizationFiller(salt=self.salt,table='followers',field='follower_login'))



class PseudonymizationFiller(fillers.Filler):
	'''
	Wrapping pseudonymization steps as fillers -- base structure hashing one field in one table
	'''
	def __init__(self,table,field,force=False,id_field=None,salt=None,**kwargs):
		fillers.Filler.__init__(self,**kwargs)
		self.table = table
		self.field = field
		if id_field is None:
			self.id_field = field
		else:
			self.id_field = id_field
		self.force = force
		self.salt = salt

	def set_salt(self):
		self.db.cursor.execute('''SELECT info_content FROM _dbinfo WHERE info_type='pseudonymization_salt'; ''')
		db_salt = self.db.cursor.fetchone()
		if db_salt is not None:
			db_salt = db_salt[0]

		if db_salt is None:
			if self.salt is None:
				self.salt = secrets.token_urlsafe(16)
			if self.db.db_type == 'postgres':
				self.db.cursor.execute('''INSERT INTO _dbinfo(info_type,info_content) SELECT 'pseudonymization_salt',%(salt)s;''',{'salt':self.salt})
			else:
				self.db.cursor.execute('''INSERT INTO _dbinfo(info_type,info_content) SELECT 'pseudonymization_salt',:salt;''',{'salt':self.salt})
		elif self.salt is None:
			self.salt = db_salt
		elif self.salt != db_salt:
				raise ValueError('Trying to pseudonymize with a different salt than the one already used for previous pseudonymization. DB salt:{} filler salt:{}'.format(db_salt,self.salt))

	def get_update_name(self,table=None,field=None):
		if table is None:
			table = self.table
		if field is None:
			field = self.field
		return 'pseudonymization__{}__{}'.format(table,field)

	def prepare(self):
		self.set_salt()
		update_name = self.get_update_name()
		if not self.force:
			if self.db.db_type == 'postgres':
				self.db.cursor.execute('''SELECT COUNT(*) FROM full_updates WHERE update_type=%(update_name)s;''',{'update_name':update_name})
			else:
				self.db.cursor.execute('''SELECT COUNT(*) FROM full_updates WHERE update_type=:update_name;''',{'update_name':update_name})
			if self.db.cursor.fetchone()[0] > 0:
				self.done = True


	def register_update(self,table=None,field=None):
		update_name = self.get_update_name(table=table,field=field)
		if self.db.db_type == 'postgres':
			self.db.cursor.execute('''INSERT INTO full_updates(update_type) SELECT %(update_name)s;''',{'update_name':update_name})
		else:
			self.db.cursor.execute('''INSERT INTO full_updates(update_type) SELECT :update_name;''',{'update_name':update_name})

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


def pseudonymize_emails(db,salt=None):
	'''
	# Steps for pseudonymizing
	 - WHAT:
	  - emails: hash prefix
	  - names: nullify
	 - WHERE:
	  - emails to be found in:
	   - identities with identity type email
	   - users with origin identity
	  - names to be found in:
	   - identities with identity type email
	 - HOW:
	  - identities table:
	   - email identities not having a 'hashed_identities' update in table_updates get their prefix hashed, attr is set to NULL.
	  - users table:
	   - where creation_identity corresponds to an email and doesn't end with _HASHED, email prefix hashed and prefix _HASHED added.

	Bot accounts are discarded from hashing.
	Hash is done with md5, and prefixing the string with a salt (provided to the function or randomly chosen if None)
	'''
	if salt is None:
		salt = secrets.token_urlsafe(16)

	if db.db_type == 'postgres':
		db.cursor.execute('''
			WITH to_be_updated AS (
					SELECT i.id FROM identities i
					INNER JOIN identity_types it
					ON it.id=i.identity_type_id  AND it.name='email' AND NOT i.is_bot
					LEFT OUTER JOIN table_updates tu
					ON tu.identity_id = i.id AND tu.table_name='hashed_identities'
					WHERE tu.id IS NULL
					),
				changed_id AS (
					UPDATE identities i SET
						attributes=NULL, identity= COALESCE(MD5(%(salt)s||SUBSTRING(i.identity FROM '[^@]+'))||'@'||SUBSTRING(i.identity FROM '(?<=@).*'),
															MD5(%(salt)s||i.identity))
					WHERE i.id IN (SELECT * FROM to_be_updated)
					RETURNING id
					)
			INSERT INTO table_updates(identity_id,table_name)
			SELECT id,'hashed_identities'
				FROM changed_id;
			''',{'salt':salt})
	else:
		db.cursor.execute('''
					SELECT i.id,i.identity FROM identities i
					INNER JOIN identity_types it
					ON it.id=i.identity_type_id  AND it.name='email' AND NOT i.is_bot
					LEFT OUTER JOIN table_updates tu
					ON tu.identity_id = i.id AND tu.table_name='hashed_identities'
					WHERE tu.id IS NULL
					;''')
		to_update = [r for r in db.cursor.fetchall()]



		updated = [(iid,modify(old_id=old_id,salt=salt)) for iid,old_id in to_update]
		db.cursor.executemany('''
			UPDATE identities SET
						attributes=NULL , identity= :new_id
					WHERE id = :iid
					;
			''',({'salt':salt,'iid':iid,'new_id':new_id} for iid,new_id in updated))
		db.cursor.executemany('''
			INSERT INTO table_updates(identity_id,table_name)
			VALUES (:updated_id,'hashed_identities');
			''',({'updated_id':uid} for uid,new_id in updated))


	db.connection.commit()

	# users
	if db.db_type == 'postgres':
		db.cursor.execute('''
			WITH to_be_updated AS (
					SELECT u.id FROM users u
					INNER JOIN identity_types it
					ON it.id=u.creation_identity_type_id AND it.name='email' AND NOT u.is_bot
					AND u.creation_identity NOT LIKE '%%_HASHED'
					)
			UPDATE users u SET
						creation_identity= COALESCE(MD5(%(salt)s||SUBSTRING(u.creation_identity FROM '[^@]+'))||'@'||SUBSTRING(u.creation_identity FROM '(?<=@).*'),
															MD5(%(salt)s||u.creation_identity))||'_HASHED'
					WHERE u.id IN (SELECT * FROM to_be_updated)
			;''',{'salt':salt})
	else:
		db.cursor.execute('''
					SELECT u.id,u.creation_identity FROM users u
					INNER JOIN identity_types it
					ON it.id=u.creation_identity_type_id AND it.name='email' AND NOT u.is_bot
					AND u.creation_identity NOT LIKE '%%_HASHED'
					;''')
		to_update = [r for r in db.cursor.fetchall()]

		updated = [(uid,modify(old_id=old_id,salt=salt)+'_HASHED') for uid,old_id in to_update]
		db.cursor.executemany('''
			UPDATE users SET
						creation_identity= :new_id
					WHERE id = :uid
					;
			''',({'salt':salt,'uid':uid,'new_id':new_id} for uid,new_id in updated))

	db.connection.commit()
