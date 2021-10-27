import os
import hashlib
import csv
import copy
import pygit2
import json
import shutil
import datetime
import subprocess

from psycopg2 import extras

from repo_tools import fillers
import repo_tools as rp


class BotFiller(fillers.Filler):
	"""
	Basic BotFiller, checks for github logins ending in '[bot]'
	can be reused as a template, modifying fill_bots
	"""
	def __init__(self,pattern='%[bot]',identity_type='github_login',**kwargs):
		self.pattern = pattern
		self.identity_type = identity_type
		fillers.Filler.__init__(self,**kwargs)

	def apply(self):
		self.fill_bots()
		self.db.connection.commit()

	def fill_bots(self):
		if self.db.db_type == 'postgres':
			self.db.cursor.execute('''
				UPDATE identities SET is_bot=true
				WHERE identity_type_id = (SELECT id FROM identity_types WHERE name=%(identity_type)s)
				AND identity LIKE %(pattern)s
				''',{'identity_type':self.identity_type,'pattern':self.pattern})
		else:
			self.db.cursor.execute('''
				UPDATE identities SET is_bot=1
				WHERE identity_type_id = (SELECT id FROM identity_types WHERE name=:identity_type)
				AND identity LIKE :pattern
				''',{'identity_type':self.identity_type,'pattern':self.pattern})

class BotUserFiller(BotFiller):
	'''
	Fills in bot boolean for identities matching a user having at least one is_bot identity
	'''		
	def fill_bots(self):
		if self.db.db_type == 'postgres':
			self.db.cursor.execute('''
					UPDATE identities SET is_bot=true
					WHERE user_id IN (SELECT DISTINCT user_id FROM identities
						WHERE is_bot);
				''')
		else:
			self.db.cursor.execute('''
					UPDATE identities SET is_bot=1
					WHERE user_id IN (SELECT DISTINCT user_id FROM identities
						WHERE is_bot);
				''')


class ResetBotsFiller(BotFiller):
	'''
	Resets all is_bot to false
	'''
	def fill_bots(self):
		if self.db.db_type == 'postgres':
			self.db.cursor.execute('''
				UPDATE identities SET is_bot=false;
				''')
		else:
			self.db.cursor.execute('''
				UPDATE identities SET is_bot=0;
				''')

class BotListFiller(BotFiller):
	'''
	fills bots from a string list
	'''
	def __init__(self,bot_list,identity_type='github_login',**kwargs):
		self.bot_list = bot_list
		self.identity_type = identity_type
		BotFiller.__init__(self,**kwargs)

	def fill_bots(self):
		if self.db.db_type == 'postgres':
			extras.execute_batch(self.db.cursor,'''
				UPDATE identities SET is_bot=true
				WHERE identity_type_id = (SELECT id FROM identity_types WHERE name=%(identity_type)s)
				AND identity = %(identity)s
				''',({'identity_type':self.identity_type,'identity':i} for i in self.bot_list))
		else:
			self.db.cursor.executemany('''
				UPDATE identities SET is_bot=1
				WHERE identity_type_id = (SELECT id FROM identity_types WHERE name=:identity_type)
				AND identity = :identity
				''',({'identity_type':self.identity_type,'identity':i} for i in self.bot_list))
		


class BotFileFiller(BotListFiller):
	'''
	fills bots from a file, containing only strings
	'''
	def __init__(self,bot_file,identity_type='github_login',**kwargs):
		self.bot_file = bot_file
		self.identity_type = identity_type
		BotFiller.__init__(self,**kwargs)


	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder

		filepath = os.path.join(self.data_folder,self.bot_file)
		with open(filepath,"rb") as f:
			filehash = hashlib.sha256(f.read()).hexdigest()
			self.source = '{}_{}'.format(self.bot_file,filehash)
			self.db.register_source(source=self.source)
		
		with open(filepath,'r') as f:
			self.bot_list = f.read().split('\n')
