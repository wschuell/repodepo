
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import numpy as np

from . import pandas_freq
from .generic_getters import Getter
from . import generic_getters

from . import project_getters
from . import user_getters
from . import rank_getters
from . import edge_getters

default_start_date = datetime.datetime(2013,1,1)
default_end_date = datetime.datetime.now()

class CombinedGetter(Getter):
	'''
	abstract class
	'''
	def __init__(self,db,start_date=default_start_date,end_date=default_end_date,time_window='month',with_reponame=True,with_userlogin=True,**kwargs):
		self.time_window = time_window
		self.start_date = start_date
		self.end_date = end_date
		self.with_reponame = with_reponame
		self.with_userlogin = with_userlogin
		Getter.__init__(self,db=db,**kwargs)

class UsageGetter(CombinedGetter):
	'''
	Retrieves as a dataframe:
	repo_id,repo_name,timestamp,stars,downloads,commits,commits_cumulative,forks,total contributors, active contributors
	'''

	def get_result(self):
		stars = project_getters.Stars(db=self.db).get_result(time_window=self.time_window,start_date=self.start_date,end_date=self.end_date,aggregated=False)
		forks = project_getters.Forks(db=self.db).get_result(time_window=self.time_window,start_date=self.start_date,end_date=self.end_date,aggregated=False)
		commits = project_getters.Commits(db=self.db).get_result(time_window=self.time_window,start_date=self.start_date,end_date=self.end_date,aggregated=False,cumulative=False)
		commits_cumul = project_getters.Commits(db=self.db).get_result(time_window=self.time_window,start_date=self.start_date,end_date=self.end_date,aggregated=False,cumulative=True)
		commits_cumul.rename(columns={'commits':'commits_cumul'},inplace=True)
		devs = project_getters.Developers(db=self.db).get_result(time_window=self.time_window,start_date=self.start_date,end_date=self.end_date,aggregated=False)
		active_devs = project_getters.ActiveDevelopers(db=self.db).get_result(time_window=self.time_window,start_date=self.start_date,end_date=self.end_date,aggregated=False,cumulative=False)
		downloads = project_getters.Downloads(db=self.db).get_result(time_window=self.time_window,start_date=self.start_date,end_date=self.end_date,aggregated=False)

		df = pd.DataFrame(columns=['project_id','timestamp'])
		# df = commits_cumul
		df = pd.merge(df, stars,  how='outer', left_on=['project_id','timestamp'], right_on = ['project_id','timestamp'])
		df = pd.merge(df, forks,  how='outer', left_on=['project_id','timestamp'], right_on = ['project_id','timestamp'])
		df = pd.merge(df, commits,  how='outer', left_on=['project_id','timestamp'], right_on = ['project_id','timestamp'])
		df = pd.merge(df, commits_cumul,  how='outer', left_on=['project_id','timestamp'], right_on = ['project_id','timestamp'])
		df = pd.merge(df, devs,  how='outer', left_on=['project_id','timestamp'], right_on = ['project_id','timestamp'])
		df = pd.merge(df, active_devs,  how='outer', left_on=['project_id','timestamp'], right_on = ['project_id','timestamp'])
		df = pd.merge(df, downloads,  how='outer', left_on=['project_id','timestamp'], right_on = ['project_id','timestamp'])

		creation_dates = generic_getters.RepoCreatedAt(db=self.db).get_result()
		creation_dates.set_index('project_id',inplace=True)
		creation_dates.fillna(self.start_date,inplace=True)
		df = df.join(creation_dates,how='left',on='project_id')
		df.reset_index(inplace=True)
		df.drop(df[df.created_at > df.timestamp].index, inplace=True)
		df.set_index(['project_id','timestamp'],inplace=True)

		if self.with_reponame:
			reponames = generic_getters.RepoNames(db=self.db).get_result().set_index('project_id')
			df = df.join(reponames,how='left',on='project_id')


		# df.reset_index(inplace=True)
		df.index.set_names(['repo_id', 'timestamp'], inplace=True)


		# reordering columns
		order = [
				# 'repo_id',
				# 'timestamp',
				'repo_name',
				'stars',
				'forks',
				'commits',
				'commits_cumul',
				'developers',
				'active_developers',
				'downloads',
					]
		if not self.with_reponame:
			order.remove('repo_name')

		df = df[order]

		df.fillna(0,inplace=True)

		return df

class DepsGetter(CombinedGetter):
	'''
	Retrieves as a dataframe:
	repo_id,repo_name,dep_id,dep_name,timestamp

	Filtering dependencies so that the result is always a DAG
	'''

	def get_result(self):
		date_range = pd.date_range(self.start_date,self.end_date,freq=pandas_freq[self.time_window]).to_pydatetime()
		ans_df = pd.DataFrame(columns=['timestamp','repo_id','dep_id'])
		ans_df.set_index(['timestamp','repo_id','dep_id'],inplace=True)


		for t in date_range:
			self.logger.info('Getting dependency network on {}'.format(datetime.datetime.strftime(t,'%Y-%m-%d')))
			deps_df = pd.DataFrame(edge_getters.RepoToRepoDeps(db=self.db,ref_time=t).get_result(raw_result=True),columns=['repo_id','dep_id'])
			deps_df = deps_df[['repo_id','dep_id']]
			deps_df['timestamp'] = t
			deps_df.set_index(['timestamp','repo_id','dep_id'],inplace=True)
			ans_df = pd.concat([ans_df,deps_df])

		if self.with_reponame:
			reponames = generic_getters.RepoNames(db=self.db).get_result()
			depnames = reponames.rename(columns={'project_id':'dep_id','repo_name':'dep_name'}).set_index('dep_id')
			reponames = reponames.rename(columns={'project_id':'repo_id'}).set_index('repo_id')
			ans_df = ans_df.join(reponames,how='left',on='repo_id')
			ans_df = ans_df.join(depnames,how='left',on='dep_id')
		return ans_df



class FilteredDepsGetter(DepsGetter):
	'''
	Retrieves as a dataframe:
	repo_id,repo_name,dep_id,dep_name,timestamp
	Only for the filtered elements
	'''
	pass



class ContributionsGetter(CombinedGetter):
	'''
	Retrieves as a dataframe:
	repo_id,repo_name,user_id,nb_commits,timestamp

	removing bots. Not counting
	'''

	def get_result(self):
		# date_range = pd.date_range(self.start_date,self.end_date,freq=pandas_freq[self.time_window]).to_pydatetime()
		date_range = pd.date_range(self.start_date,self.end_date,freq=pandas_freq[self.time_window]).to_pydatetime()
		ans_df = pd.DataFrame(columns=['timestamp','repo_id','user_id','nb_commits'])
		ans_df.set_index(['timestamp','repo_id','user_id'],inplace=True)


		for t in date_range:
			self.logger.info('Getting contribution network on {}'.format(datetime.datetime.strftime(t,'%Y-%m-%d')))
			res = ({'repo_id':d['repo_id'],
						'user_id':d['user_id'],
						'nb_commits':d['abs_value']
						} for d in edge_getters.DevToRepo(db=self.db,start_time=t-relativedelta(**{self.time_window:1}),end_time=t).get_result(raw_result=True))
			deps_df = pd.DataFrame(res,columns=['timestamp','repo_id','user_id','nb_commits'])
			deps_df = deps_df.convert_dtypes()
			deps_df['timestamp'] = t
			deps_df.set_index(['timestamp','repo_id','user_id'],inplace=True)
			ans_df = pd.concat([ans_df,deps_df])

		if self.with_reponame:
			reponames = generic_getters.RepoNames(db=self.db).get_result()
			reponames = reponames.rename(columns={'project_id':'repo_id'}).set_index('repo_id')
			ans_df = ans_df.join(reponames,how='left',on='repo_id')

		if self.with_userlogin:
			userlogins = generic_getters.UserLogins(db=self.db).get_result()
			userlogins.set_index('user_id',inplace=True)
			ans_df = ans_df.join(userlogins,how='left',on='user_id')

		order = ['repo_name',
					'user_login',
					'nb_commits']
		if not self.with_reponame:
			order.remove('repo_name')
		if not self.with_userlogin:
			order.remove('user_login')
		ans_df = ans_df[order]

		return ans_df
