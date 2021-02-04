import datetime
import os
import copy
import logging
import time
import random

from repo_tools import fillers
from repo_tools.fillers import generic,github,commit_info
import repo_tools as rp




class DummyMetaFiller(fillers.Filler):
	"""
	Meta Filler with dummy data
	"""
	def __init__(self,workers=1,api_keys_file='github_api_keys.txt',packages_file=None,**kwargs):
		self.workers = workers
		self.api_keys_file = api_keys_file
		if packages_file is None:
			self.packages_file = os.path.join(os.path.dirname(os.path.dirname(rp.__file__)),'tests','testmodule','dummy_data','packages.csv')
		else:
			self.packages_file = packages
		fillers.Filler.__init__(self,**kwargs)

	def prepare(self):
		self.db.add_filler(generic.SourcesFiller(source=['GitHub',],source_urlroot=['github.com',]))

		if os.path.isabs(self.packages_file):
			self.db.add_filler(generic.PackageFiller(package_list_file=os.path.basename(self.packages_file),data_folder=os.path.dirname(self.packages_file)))
		else:
			self.db.add_filler(generic.PackageFiller(package_list_file=self.packages_file))

		self.db.add_filler(generic.RepositoriesFiller())
		self.db.add_filler(generic.ClonesFiller())

		if os.path.isabs(self.api_keys_file):
			data_folder = None
			api_keys_file = self.api_keys_file
		else:
			data_folder = os.path.dirname(self.api_keys_file)
			api_keys_file = os.path.basename(self.api_keys_file)

		self.db.add_filler(github.ForksFiller(fail_on_wait=True,workers=self.workers,data_folder=data_folder,api_keys_file=api_keys_file))
		self.db.add_filler(generic.ClonesFiller())
		self.db.add_filler(commit_info.CommitsFiller())
		self.db.add_filler(github.GHLoginsFiller(fail_on_wait=True,workers=self.workers,data_folder=data_folder,api_keys_file=api_keys_file))
		self.db.add_filler(github.StarsFiller(fail_on_wait=True,workers=self.workers,data_folder=data_folder,api_keys_file=api_keys_file))
		self.db.add_filler(github.FollowersFiller(fail_on_wait=True,workers=self.workers,data_folder=data_folder,api_keys_file=api_keys_file))
