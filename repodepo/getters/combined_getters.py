
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
import copy

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
	def __init__(self,db,start_date=default_start_date,end_date=default_end_date,force_repo_date=False,time_window='month',with_reponame=True,with_userlogin=True,**kwargs):
		self.time_window = time_window
		self.start_date = start_date
		self.end_date = end_date
		self.with_reponame = with_reponame
		self.with_userlogin = with_userlogin
		self.force_repo_date = force_repo_date
		Getter.__init__(self,db=db,**kwargs)

class UsageGetter(CombinedGetter):
	'''
	Retrieves as a dataframe:
	repo_id,repo_name,timestamp,stars,downloads,commits,commits_cumulative,forks,total contributors, active contributors
	to add: committers, mergers, issue authors, issues, closed issues, PRs, merged PRs, comments (PR+issue+commits), 
	        reactions (commit comments, issue/PR comments, issues/PRs)
	also could distinguish commit authors and total contributors (authors + committers + mergers + comment writers etc)
	'''

	subgetters = [
		{'class':project_getters.Stars,'rename':{},'extra_kwargs':{}},
		{'class':project_getters.Forks,'rename':{},'extra_kwargs':{}},
		{'class':project_getters.Commits,'rename':{},'extra_kwargs':{'cumulative':False}},
		{'class':project_getters.Commits,'rename':{'commits':'commits_cumul'},'extra_kwargs':{'cumulative':True}},
		{'class':project_getters.Developers,'rename':{},'extra_kwargs':{}},
		{'class':project_getters.ActiveDevelopers,'rename':{},'extra_kwargs':{'cumulative':False}},
		{'class':project_getters.Downloads,'rename':{},'extra_kwargs':{}},
		{'class':project_getters.Issues,'rename':{},'extra_kwargs':{}},
		{'class':project_getters.ClosedIssues,'rename':{},'extra_kwargs':{}},
		{'class':project_getters.PullRequests,'rename':{},'extra_kwargs':{}},
		{'class':project_getters.MergedPullRequests,'rename':{},'extra_kwargs':{}},

			]

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
					'issues',
					'issues_closed',
					'pullrequests',
					'pullrequests_merged',]

	def get_result(self):

		df = pd.DataFrame(columns=['project_id','timestamp'])
		# df = commits_cumul
		for sg in self.subgetters:
			sg_df = sg['class'](db=self.db).get_result(time_window=self.time_window,
														start_date=self.start_date,
														end_date=self.end_date,
														aggregated=False,
														**sg['extra_kwargs'])
			sg_df.rename(columns=sg['rename'],inplace=True)
			df = pd.merge(df, sg_df,  how='outer', left_on=['project_id','timestamp'], right_on = ['project_id','timestamp'])

		creation_dates = generic_getters.RepoCreatedAt(db=self.db,force_repo_date=self.force_repo_date).get_result()
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
		order = copy.deepcopy(self.order)

		if not self.with_reponame:
			order.remove('repo_name')

		df = df[order]

		cols = df.select_dtypes(exclude=['string'])
		df.fillna({c:0 for c in cols},inplace=True)

		return df


class IdleReposGetter(UsageGetter):
	subgetters = [
		{'class':project_getters.LastCommit,'rename':{},'extra_kwargs':{'cumulative':False}},
		{'class':project_getters.Downloads,'rename':{'downloads':'downloads_noncumul'},'extra_kwargs':{'cumulative':False}},
		{'class':project_getters.Issues,'rename':{'issues':'issues_noncumul'},'extra_kwargs':{'cumulative':False}},
		{'class':project_getters.ClosedIssues,'rename':{'issues_closed':'issues_closed_noncumul'},'extra_kwargs':{'cumulative':False}},
		{'class':project_getters.PullRequests,'rename':{'pullrequests':'pullrequests_noncumul'},'extra_kwargs':{'cumulative':False}},
		{'class':project_getters.MergedPullRequests,'rename':{'pullrequests_merged':'pullrequests_merged_noncumul'},'extra_kwargs':{'cumulative':False}},
		] + UsageGetter.subgetters

	order = UsageGetter.order + [
						'downloads_noncumul',
						'issues_noncumul',
						'issues_closed_noncumul',
						'pullrequests_noncumul',
						'pullrequests_merged_noncumul',
						'created_at',
						'last_commit']

	def get_result(self):
		df = UsageGetter.get_result(self)
		# remove not idle
		df.drop(df[df.commits > 0].index, inplace=True)
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

	contrib_getter_class = edge_getters.DevToRepo

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
						} for d in self.contrib_getter_class(db=self.db,start_time=t-relativedelta(**{self.time_window:1}),end_time=t).get_result(raw_result=True))
			deps_df = pd.DataFrame(res,columns=['timestamp','repo_id','user_id','nb_commits'])
			deps_df = deps_df.convert_dtypes()
			deps_df['timestamp'] = t
			deps_df.set_index(['timestamp','repo_id','user_id'],inplace=True)
			deps_df.sort_values(['timestamp','repo_id','user_id'],inplace=True)
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


class IssueContributionsGetter(ContributionsGetter):
	contrib_getter_class = edge_getters.DevToRepoIssues