import os
import hashlib
import csv
import copy
import pygit2
import json
import shutil
import datetime
import subprocess
import time
from psycopg2 import extras

from .. import fillers
from ..getters import bot_checks


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



class BotsManualChecksFiller(fillers.Filler):

	def __init__(self,blocking=True,auto_update=True,error_catch_counts = 5,**kwargs):
		fillers.Filler.__init__(self,**kwargs)
		self.blocking = blocking
		self.auto_update = auto_update
		self.error_catch_counts = error_catch_counts

	def prepare(self):
		df = bot_checks.BotChecks(db=self.db).get_result()
		self.to_check = [{'user_id':row['user_id'],'identity_type':row['login'].split('/')[0],'identity':row['login'].split('/')[1]} for idx,row in df.iterrows()]

	def apply(self):
		self.checks(silent=True)
		self.prepare() # In case checks uncover new 
		self.checks()

	def checks(self,silent=False):

		#### Filling in table _bots_manual_checks
		if self.db.db_type == 'postgres':
			extras.execute_batch(self.db.cursor,
				'''
				INSERT INTO _bots_manual_check(identity_id,
												identity,
												identity_type_id,
												identity_type)
				SELECT i.id,
						%(identity)s,
						it.id,
						%(identity_type)s
				FROM identities i INNER JOIN identity_types it 
						ON it.name=%(identity_type)s AND it.id=i.identity_type_id AND i.identity=%(identity)s
				
				ON CONFLICT DO NOTHING
				;''',self.to_check)
		else:
			self.db.cursor.executemany(
				'''
				INSERT OR IGNORE INTO _bots_manual_check(identity_id,
												identity,
												identity_type_id,
												identity_type)
				SELECT i.id,
						:identity,
						it.id,
						:identity_type
				FROM identities i INNER JOIN identity_types it 
						ON it.name=:identity_type AND it.id=i.identity_type_id AND i.identity=:identity
				;''',self.to_check)


		#### Filling bots info in identities and users tables
		self.db.cursor.execute('''
			UPDATE identities SET is_bot=true
			WHERE id IN (SELECT identity_id FROM _bots_manual_check bmc WHERE bmc.is_bot)
			;''')
		BotUserFiller.fill_bots(self) # Propagating to user level		

		#### Checking bots potentially already declared from another source
		self.db.cursor.execute('''
			UPDATE _bots_manual_check SET is_bot=true
			WHERE identity_id IN (SELECT id FROM identities i WHERE i.is_bot)
			;''')

		### Committing before potential block
		self.db.connection.commit()

		### Blocking if checks remaining
		if self.blocking:
			self.db.cursor.execute('SELECT COUNT(*) FROM _bots_manual_check WHERE is_bot IS NULL;')
			ans = self.db.cursor.fetchone()
			if ans[0] != 0:
				if self.auto_update:
					self.cycle()
				else:
					raise ValueError('Pending manual checks for bots/invalid identities in _bots_manual_checks table')
			elif not silent:
				self.logger.info('Passed bot checks, no further identities to be checked manually')


	def cycle(self):
		bots_decision_remaining = -1
		while bots_decision_remaining != 0:
			time.sleep(10)
			try:
				self.prepare()
				self.checks()
			except KeyboardInterrupt:
				raise
			except psycopg2.errors.AdminShutdown as e:
				if self.error_catch_counts>0:
					self.error_catch_counts -= 1
					self.logger.info(f'Catched error in bot manual check cycle: {e}')
					self.db.reconnect()
					continue
				else:
					raise
			except Exception as e:
				if self.error_catch_counts>0:
					self.error_catch_counts -= 1
					self.logger.info(f'Catched error in bot manual check cycle: {e}')
					continue
				else:
					raise

			before_val = bots_decision_remaining
			self.db.cursor.execute('SELECT COUNT(*) FROM _bots_manual_check WHERE is_bot IS NULL;')
			bots_decision_remaining = self.db.cursor.fetchone()[0]
			if bots_decision_remaining != before_val:
				self.logger.info(f'Bots manual checks remaining: {bots_decision_remaining}')
