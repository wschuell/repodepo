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

from .. import fillers


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
			self.db.cursor.execute('''
					UPDATE users SET is_bot=true
					WHERE id in (SELECT DISTINCT user_id FROM identities
						WHERE is_bot);
				''')
		else:
			self.db.cursor.execute('''
					UPDATE identities SET is_bot=1
					WHERE user_id IN (SELECT DISTINCT user_id FROM identities
						WHERE is_bot);
				''')
			self.db.cursor.execute('''
					UPDATE users SET is_bot=1
					WHERE id in (SELECT DISTINCT user_id FROM identities
						WHERE is_bot);
				''')


class ResetBotsFiller(BotFiller):
	'''
	Resets all is_bot to false
	'''
	def fill_bots(self):
		if self.db.db_type == 'postgres':
			self.db.cursor.execute('''
				UPDATE users SET is_bot=false;
				''')
			self.db.cursor.execute('''
				UPDATE identities SET is_bot=false;
				''')
		else:
			self.db.cursor.execute('''
				UPDATE users SET is_bot=0;
				''')
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

	def prepare(self):
		new_botlist = []
		for b in self.bot_list:
			if isinstance(b,str):
				new_botlist.append((self.identity_type,b))
			elif len(b) == 1:
				new_botlist.append((self.identity_type,b[0]))
			elif len(b) == 2:
				new_botlist.append((b[0],b[1]))
			else:
				raise SyntaxError('In provided botlist, an element has more than 2 items (should be (<identity_type>,<bot>) or <bot> :{}'.format(b))
		self.bot_list = new_botlist

	def fill_bots(self):
		if self.db.db_type == 'postgres':
			extras.execute_batch(self.db.cursor,'''
				UPDATE identities SET is_bot=true
				WHERE identity_type_id = (SELECT id FROM identity_types WHERE name=%(identity_type)s)
				AND identity = %(identity)s
				''',({'identity_type':it,'identity':i} for it,i in self.bot_list))
		else:
			self.db.cursor.executemany('''
				UPDATE identities SET is_bot=1
				WHERE identity_type_id = (SELECT id FROM identity_types WHERE name=:identity_type)
				AND identity = :identity
				''',({'identity_type':it,'identity':i} for it,i in self.bot_list))
		


class BotFileFiller(BotListFiller):
	'''
	fills bots from a file, containing only identity (with implied identity_type) or identity_type,identity
	'''
	def __init__(self,bot_file,identity_type='github_login',header=True,**kwargs):
		self.bot_file = bot_file
		self.identity_type = identity_type
		self.header = header
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
			reader = csv.reader(f)
			if self.header:
				next(reader)
			self.bot_list = list(reader)

		BotListFiller.prepare(self)

class MGBotFiller(BotFileFiller):
	'''
	Fills in bots from the paper "A ground-truth dataset and classification model for detecting bots in
	GitHub issue and PR comments" by Golzadeh et al
	'''
	def __init__(self,botfile_url='https://zenodo.org/record/4000388/files/groundtruthbots.csv.gz?download=1',**kwargs):
		self.botfile_url = botfile_url
		BotFileFiller.__init__(self,bot_file='groundtruthbots_formatted.csv',header=True,**kwargs)

	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder

		if not os.path.exists(os.path.join(self.data_folder,'groundtruthbots_formatted.csv')):
			if not os.path.exists(os.path.join(self.data_folder,'groundtruthbots.csv')):
				if not os.path.exists(os.path.join(self.data_folder,'groundtruthbots.csv.gz')):
					self.download(url=self.botfile_url,destination=os.path.join(self.data_folder,'groundtruthbots.csv.gz'))
				self.ungzip(orig_file=os.path.join(self.data_folder,'groundtruthbots.csv.gz'),destination=os.path.join(self.data_folder,'groundtruthbots.csv'))
			self.parse(orig_file=os.path.join(self.data_folder,'groundtruthbots.csv'),destination=os.path.join(self.data_folder,'groundtruthbots_formatted.csv'))
		BotFileFiller.prepare(self)

	def parse(self,orig_file,destination):
		bots = []
		with open(orig_file,'r') as f:
			reader = csv.reader(f)
			if self.header:
				next(reader) #header
			for login,project,bot_or_human in reader:
				if bot_or_human == 'Bot':
					bots.append(login)

		sorted_bots = sorted(set(bots))
		with open(destination,'w') as f:
			f.write('\n'.join(sorted_bots))

