
import pandas as pd
import datetime
import numpy as np

from . import pandas_freq
from .generic_getters import Getter
from . import generic_getters

from . import project_getters
from . import user_getters
# from . import rank_getters
# from . import edge_getters

default_start_date = datetime.datetime(2013,1,1)
default_end_date = datetime.datetime.now()


class UsageGetter(Getter):
	'''
	Retrieves as a dataframe:
	repo_id,repo_name,timestamp,stars,downloads,commits,commits_cumulative,forks,total contributors, active contributors
	'''
	def __init__(self,db,start_date=default_start_date,end_date=default_end_date,time_window='month',with_reponame=True,**kwargs):
		self.time_window = time_window
		self.start_date = start_date
		self.end_date = end_date
		self.with_reponame = with_reponame
		Getter.__init__(self,db=db,**kwargs)

	def get_result(self):
		stars = project_getters.Stars(db=self.db).get_result(time_window=self.time_window,start_date=self.start_date,end_date=self.end_date,aggregated=False)
		forks = project_getters.Forks(db=self.db).get_result(time_window=self.time_window,start_date=self.start_date,end_date=self.end_date,aggregated=False)
		commits = project_getters.Commits(db=self.db).get_result(time_window=self.time_window,start_date=self.start_date,end_date=self.end_date,aggregated=False,cumulative=False)
		commits_cumul = project_getters.Commits(db=self.db).get_result(time_window=self.time_window,start_date=self.start_date,end_date=self.end_date,aggregated=False,cumulative=True)
		commits_cumul.rename(columns={'commits':'commits_cumul'},inplace=True)
		devs = project_getters.Developers(db=self.db).get_result(time_window=self.time_window,start_date=self.start_date,end_date=self.end_date,aggregated=False)
		active_devs = project_getters.ActiveDevelopers(db=self.db).get_result(time_window=self.time_window,start_date=self.start_date,end_date=self.end_date,aggregated=False)
		downloads = project_getters.Downloads(db=self.db).get_result(time_window=self.time_window,start_date=self.start_date,end_date=self.end_date,aggregated=False)

		df = commits_cumul
		df = pd.merge(df, stars,  how='left', left_on=['project_id','timestamp'], right_on = ['project_id','timestamp'])
		df = pd.merge(df, forks,  how='left', left_on=['project_id','timestamp'], right_on = ['project_id','timestamp'])
		df = pd.merge(df, commits,  how='left', left_on=['project_id','timestamp'], right_on = ['project_id','timestamp'])
		# df = pd.merge(df, commits_cumul,  how='left', left_on=['project_id','timestamp'], right_on = ['project_id','timestamp'])
		df = pd.merge(df, devs,  how='left', left_on=['project_id','timestamp'], right_on = ['project_id','timestamp'])
		df = pd.merge(df, active_devs,  how='left', left_on=['project_id','timestamp'], right_on = ['project_id','timestamp'])
		df = pd.merge(df, downloads,  how='left', left_on=['project_id','timestamp'], right_on = ['project_id','timestamp'])

		creation_dates = generic_getters.RepoCreatedAt(db=self.db).get_result()
		creation_dates.set_index('project_id',inplace=True)
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

class DepsGetter(Getter):
	'''
	Retrieves as a dataframe:
	repo_id,repo_name,dep_id,dep_name,timestamp

	Filtering dependencies so that the result is always a DAG
	'''
	def __init__(self,db,start_date=default_start_date,end_date=default_end_date,time_window='month',**kwargs):
		self.time_window = time_window
		self.start_date = start_date
		self.end_date = end_date
		Getter.__init__(self,db=db,**kwargs)



class FilteredDepsGetter(DepsGetter):
	'''
	Retrieves as a dataframe:
	repo_id,repo_name,dep_id,dep_name,timestamp
	Only for the filtered elements
	'''
	pass



class ContributionsGetter(Getter):
	'''
	Retrieves as a dataframe:
	repo_id,repo_name,user_id,nb_commits,timestamp

	removing bots. Not counting
	'''
	def __init__(self,db,start_date=default_start_date,end_date=default_end_date,time_window='month',**kwargs):
		self.time_window = time_window
		self.start_date = start_date
		self.end_date = end_date
		Getter.__init__(self,db=db,**kwargs)

'''

select sq.repo_id,sq.nb_stars,dq.nb_downloads from (select count(s.starred_at) as nb_stars, r.id as repo_id
from repositories r 
left outer join stars s
on s.repo_id =r.id and s.starred_at <='2020-01-01'
group by r.id) sq
inner join (select sum(coalesce(0,pvd.downloads)) as nb_downloads,r.id as repo_id from repositories r
inner join packages p
on p.repo_id=r.id
inner join package_versions pv
on pv.package_id=p.id
inner join package_version_downloads pvd
on pvd.package_version=pv.id
group by r.id) dq
on dq.repo_id=sq.repo_id



select sum(coalesce(0,pvd.downloads)) as nb_downloads,r.id as repo_id from repositories r
inner join packages p
on p.repo_id=r.id
inner join package_versions pv
on pv.package_id=p.id
inner join package_version_downloads pvd
on pvd.package_version=pv.id
group by r.id


select sum(coalesce(0,pvd.downloads)) as nb_downloads,r.id as repo_id
from package_version_downloads pvd
inner join package_versions pv
on pvd.package_version=pv.id
inner join packages p
on pv.package_id=p.id
right outer join repositories r
on p.repo_id=r.id
group by r.id


select avg(cnt) from (
select count(*) as cnt from package_versions pv 
left outer join package_dependencies pd ON pd.depending_version =pv.id
group by pv.id) a 



select i.user_id,count(*),c.repo_id 
from commits c
inner join identities i 
on c.author_id =i.id
and c.created_at >'2019-12-31' and c.created_at <'2021-01-01'
group by i.user_id,c.repo_id


'''
