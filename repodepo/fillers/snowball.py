import os
import hashlib
import csv
import copy
import pygit2
import shutil
import datetime
import subprocess

from psycopg2 import extras

from .. import fillers

class UserSnowballFiller(fillers.Filler):
	"""
	Fills in missing users from tables like sponsors, followers, etc where only the login is provided.
	"""
	def __init__(self,table_list=None,**kwargs):
		available_tables = ['sponsors']
		if table_list is None:
			self.table_list = available_tables
		elif isinstance(table_list,str):
			self.table_list = [table_list]
		else:
			self.table_list = copy.deepcopy(table_list)

		for t in self.table_list:
			if t not in available_tables:
				raise ValueError('{} is not in available_tables for Filler UserSnowballFiller: {}'.format(t,available_tables))
		fillers.Filler.__init__(self,**kwargs)


	def apply(self):
		if 'sponsors' in self.table_list:
			self.fill_snowballsponsors()

	def fill_snowballsponsors(self):
		'''
		Goes into the sponsors fillers, and adds sponsor_login, sponsor_identity_id as a new user/identity
		everywhere where sponsor_id is NULL
		'''
		self.db.cursor.execute('''
			SELECT sponsor_identity_type_id, sponsor_login
			FROM sponsors_user
			WHERE sponsor_id IS NULL AND sponsor_login IS NOT NULL
			GROUP BY sponsor_identity_type_id, sponsor_login
			;''')
		missing_logins = list(self.db.cursor.fetchall())
		self.logger.info('Filling {} new identities from sponsors'.format(len(missing_logins)))
		self.fill_newidentities(new_identities=missing_logins,table_name='sponsors')
		self.db.cursor.execute('''
			UPDATE sponsors_user SET sponsor_id=(SELECT id FROM identities WHERE identity=sponsors_user.sponsor_login AND identity_type_id=sponsors_user.sponsor_identity_type_id)
				WHERE sponsor_id IS NULL
			;''')
		self.db.connection.commit()

	def fill_newidentities(self,new_identities,table_name):
		'''
		Takes a list of (identity_type_id,identity) and fills in the users and identities tables
		'''

		if self.db.db_type == 'postgres':
			extras.execute_batch(self.db.cursor,'''
				INSERT INTO users(
						creation_identity,
						creation_identity_type_id)
					SELECT %s,%s
					WHERE NOT EXISTS (SELECT 1 FROM identities
										WHERE identity_type_id=%s
										AND identity=%s)
				;''',((iid,itid,itid,iid) for (itid,iid) in new_identities))
			nb_users = self.db.cursor.rowcount
			extras.execute_batch(self.db.cursor,'''
				INSERT INTO identities(
						identity_type_id,
						identity,
						user_id)
					VALUES(%s,
							%s,
							(SELECT id FROM users u WHERE u.creation_identity=%s AND u.creation_identity_type_id=%s))
					ON CONFLICT DO NOTHING
				;''',((itid,iid,iid,itid) for (itid,iid,) in new_identities))
			nb_identities = self.db.cursor.rowcount
			nb_users = self.db.cursor.rowcount
		else:
			self.db.cursor.executemany('''
				INSERT INTO users(
						creation_identity,
						creation_identity_type_id)
					SELECT ?,?
					WHERE NOT EXISTS (SELECT 1 FROM identities
										WHERE identity_type_id=?
										AND identity=?)
				;''',((iid,itid,itid,iid) for (itid,iid) in new_identities))
			nb_users = self.db.cursor.rowcount

			self.db.cursor.executemany('''
				INSERT INTO identities(
						identity_type_id,
						identity,
						user_id)
					VALUES(?,
							?,
							(SELECT id FROM users u WHERE u.creation_identity=? AND u.creation_identity_type_id=?))
					ON CONFLICT DO NOTHING
				;''',((itid,iid,iid,itid) for (itid,iid,) in new_identities))
			nb_identities = self.db.cursor.rowcount

		self.db.connection.commit()
		self.logger.info('Filled {} new users and {} new identity associations from table {}'.format(nb_users,nb_identities,table_name))
