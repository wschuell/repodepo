

import secrets

import repo_tools as rp

import time
import os
import hashlib


def modify(old_id,salt):
	parts = old_id.split('@')
	prefix = parts[0]
	suffix = '@'.join(parts[1:])
	if len(suffix) > 0:
		suffix = '@' + suffix
	return hashlib.md5((salt+prefix).encode()).hexdigest()+suffix

def pseudonymize(db=None,salt=None):
	'''
	# Steps for pseudonymizing
	 - WHAT:
	  - emails: hash prefix
	  - names: delete
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
