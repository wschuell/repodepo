import datetime
import os
import copy
import logging
import time
import random

from .. import fillers
from . import generic,github_rest,commit_info,bot_fillers,github_gql




class DummyMetaFiller(fillers.Filler):
	"""
	Meta Filler with dummy data
	"""
	def __init__(self,workers=1,api_keys_file='github_api_keys.txt',packages_file=None,fail_on_wait=False,**kwargs):
		self.workers = workers
		self.api_keys_file = api_keys_file
		if packages_file is None:
			self.packages_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(fillers.__file__))),'tests','testmodule','dummy_data','packages.csv')
		else:
			self.packages_file = packages_file
		self.fail_on_wait = fail_on_wait
		fillers.Filler.__init__(self,**kwargs)

	def prepare(self):
		self.db.add_filler(generic.SourcesFiller(source=['GitHub',],source_urlroot=['github.com',]))

		if os.path.isabs(self.packages_file):
			self.db.add_filler(generic.PackageFiller(package_list_file=os.path.basename(self.packages_file),data_folder=os.path.dirname(self.packages_file)))
		else:
			self.db.add_filler(generic.PackageFiller(package_list_file=self.packages_file))

		self.db.add_filler(generic.RepositoriesFiller())

		if os.path.isabs(self.api_keys_file):
			data_folder = None
			api_keys_file = self.api_keys_file
		else:
			data_folder = os.path.dirname(self.api_keys_file)
			api_keys_file = os.path.basename(self.api_keys_file)

		self.db.add_filler(github_rest.ForksFiller(fail_on_wait=self.fail_on_wait,workers=self.workers,data_folder=data_folder,api_keys_file=api_keys_file))
		self.db.add_filler(generic.ClonesFiller()) # Clones after forks to have up-to-date repo URLS (detect redirects)
		self.db.add_filler(commit_info.CommitsFiller()) # Commits after forks because fork info needed for repo commit ownership
		self.db.add_filler(github_rest.GHLoginsFiller(fail_on_wait=self.fail_on_wait,workers=self.workers,data_folder=data_folder,api_keys_file=api_keys_file))
		self.db.add_filler(github_gql.StarsGQLFiller(fail_on_wait=self.fail_on_wait,workers=self.workers,data_folder=data_folder,api_keys_file=api_keys_file))
		self.db.add_filler(github_gql.FollowersGQLFiller(fail_on_wait=self.fail_on_wait,workers=self.workers,data_folder=data_folder,api_keys_file=api_keys_file))
		self.db.add_filler(github_gql.UserCreatedAtGQLFiller(fail_on_wait=self.fail_on_wait,workers=self.workers,data_folder=data_folder,api_keys_file=api_keys_file))
		self.db.add_filler(github_gql.RepoCreatedAtGQLFiller(fail_on_wait=self.fail_on_wait,workers=self.workers,data_folder=data_folder,api_keys_file=api_keys_file))
		self.db.add_filler(github_gql.RepoLanguagesGQLFiller(fail_on_wait=self.fail_on_wait,workers=self.workers,data_folder=data_folder,api_keys_file=api_keys_file))
		self.db.add_filler(github_gql.UserLanguagesGQLFiller(fail_on_wait=self.fail_on_wait,workers=self.workers,data_folder=data_folder,api_keys_file=api_keys_file))


class MetaBotFiller(fillers.Filler):
	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder

		# github logins
		self.db.add_filler(bot_fillers.BotFiller(pattern='%[bot]'))
		self.db.add_filler(bot_fillers.BotFiller(pattern='%-bot'))
		self.db.add_filler(bot_fillers.BotFiller(pattern='%-bors'))
		self.db.add_filler(bot_fillers.BotFiller(pattern='bors-%'))
		self.db.add_filler(bot_fillers.BotFiller(pattern='dependabot-%'))

		# emails
		self.db.add_filler(bot_fillers.BotFiller(pattern='dependabot-%',identity_type='email'))
		self.db.add_filler(bot_fillers.BotListFiller(bot_list=[''],identity_type='email')) # Marking as discarded empty email string in commits
		for prefix in ['','%-','%.','%+']:
			self.db.add_filler(bot_fillers.BotFiller(pattern=prefix+'bot@%'))
			self.db.add_filler(bot_fillers.BotFiller(pattern=prefix+'docbot@%'))
			self.db.add_filler(bot_fillers.BotFiller(pattern=prefix+'ghbot@%'))
			self.db.add_filler(bot_fillers.BotFiller(pattern=prefix+'bors@%'))
			self.db.add_filler(bot_fillers.BotFiller(pattern=prefix+'travis@%'))

		# Global
		self.db.add_filler(bot_fillers.BotFileFiller(os.path.join(os.path.dirname(os.path.dirname(fillers.__file__)),'data','botlist.csv')))
		self.db.add_filler(bot_fillers.MGBotFiller(data_folder=self.data_folder)) # Golzadeh et al. paper

		self.db.add_filler(bot_fillers.BotUserFiller()) # propagating from identities to users and connected identities
