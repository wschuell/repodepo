import datetime
import os
import psycopg2
import pygit2

from repo_tools import fillers
from repo_tools.fillers import generic
import repo_tools as rp

class CommitsFiller(fillers.Filler):
	"""
	global commit parser
	"""


	def __init__(self,
					**kwargs):
		fillers.Filler.__init__(self,**kwargs)

	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder
		data_folder = self.data_folder

		#create folder if needed
		if not os.path.exists(data_folder):
			os.makedirs(data_folder)

		pass

	def apply(self):
		self.fill_commit_info()


	def fill_commit_info(self,force=False,all_commits=False):
		'''
		Filling in authors, commits and parenthood using Database object methods
		'''

		self.db.cursor.execute('''SELECT MAX(updated_at) FROM full_updates WHERE update_type='commits';''')
		last_fu = self.db.cursor.fetchone()[0]

		self.db.cursor.execute('''SELECT MAX(updated_at) FROM table_updates WHERE table_name='clones' AND success;''')
		last_dl = self.db.cursor.fetchone()[0]

		if all_commits:
			option = 'basicinfo_dict_cloned'
		else:
			option = 'basicinfo_dict_time_cloned'

		if force or (last_fu is None) or (last_dl is not None and last_fu<last_dl):

			self.logger.info('Filling in users')

			for repo_info in self.db.get_repo_list(option=option):
				try:
					self.db.fill_authors(self.list_commits(basic_info_only=True,**repo_info))
				except:
					self.logger.error('Error with {}'.format(repo_info))
					raise
			self.db.create_indexes(table='users')

			self.logger.info('Filling in commits')

			for repo_info in self.db.get_repo_list(option=option):
				try:
					self.db.fill_commits(self.list_commits(basic_info_only=False,**repo_info))
				except:
					self.logger.error('Error with {}'.format(repo_info))
					raise
			self.db.create_indexes(table='commits')

			self.logger.info('Filling in commit parents')

			for repo_info in self.db.get_repo_list(option=option):
				try:
					self.db.fill_commit_parents(self.list_commits(basic_info_only=True,**repo_info))
				except:
					self.logger.error('Error with {}'.format(repo_info))
					raise
			self.db.create_indexes(table='commit_parents')

			self.db.cursor.execute('''INSERT INTO full_updates(update_type,updated_at) VALUES('commits',(SELECT CURRENT_TIMESTAMP));''')
			self.db.connection.commit()
		else:
			self.logger.info('Skipping filling of commits info')

	def list_commits(self,name,source,owner,basic_info_only=False,repo_id=None,after_time=None):
		'''
		Listing the commits of a repository
		if after time is set to an int (unix time def) or datetime.datetime instead of None, only commits strictly after given time. Commits are listed by default from most recent to least.
		'''
		if isinstance(after_time,datetime.datetime):
			after_time = datetime.datetime.timestamp(after_time)

		repo_obj = self.get_repo(source=source,name=name,owner=owner)
		if repo_id is None: # Letting the possibility to preset repo_id to avoid cursor recursive usage
			repo_id = self.db.get_repo_id(source=source,name=name,owner=owner)
		# repo_obj.walk(repo.head.target, pygit2.GIT_SORT_TOPOLOGICAL | pygit2.GIT_SORT_REVERSE)
		# for commit in repo_obj.walk(repo_obj.head.target, pygit2.GIT_SORT_TIME | pygit2.GIT_SORT_REVERSE):

		if not repo_obj.is_empty:
			for commit in repo_obj.walk(repo_obj.head.target, pygit2.GIT_SORT_TIME):
				if after_time is not None and commit.commit_time<after_time:
					break
				if basic_info_only:
					yield {
							'author_email':commit.author.email,
							'author_name':commit.author.name,
							'time':commit.commit_time,
							'time_offset':commit.commit_time_offset,
							'sha':commit.hex,
							'parents':[pid.hex for pid in commit.parent_ids],
							'repo_id':repo_id,
							}
				else:
					if commit.parents:
						diff_obj = repo_obj.diff(commit.parents[0],commit)# Inverted order wrt the expected one, to have expected values for insertions and deletions
						insertions = diff_obj.stats.insertions
						deletions = diff_obj.stats.deletions
					else:
						diff_obj = commit.tree.diff_to_tree()
						# re-inverting insertions and deletions, to get expected values
						deletions = diff_obj.stats.insertions
						insertions = diff_obj.stats.deletions
					yield {
							'author_email':commit.author.email,
							'author_name':commit.author.name,
							'time':commit.commit_time,
							'time_offset':commit.commit_time_offset,
							'sha':commit.hex,
							'parents':[pid.hex for pid in commit.parent_ids],
							'insertions':insertions,
							'deletions':deletions,
							'total':insertions+deletions,
							'repo_id':repo_id,
							}

	def get_repo(self,name,source,owner):
		'''
		Returns the pygit2 repository object
		'''
		repo_folder = os.path.join(self.data_folder,'cloned_repos',source,owner,name)
		if not os.path.exists(repo_folder):
			raise ValueError('Repository {}/{}/{} not found in cloned_repos folder'.format(source,owner,name))
		else:
			return pygit2.Repository(os.path.join(repo_folder,'.git'))
