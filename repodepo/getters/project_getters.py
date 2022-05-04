
import pandas as pd
import datetime
import numpy as np
# For email_id replacing repo_id when no login available: COALESCE(repo_id,-email_id)
from . import pandas_freq
from .generic_getters import Getter
from . import generic_getters


class ProjectGetter(Getter):
	'''
	Wrapper around the measures for projects, containing the shared code
	define subclasses, define the 4 subfunctions (SQL queries)
	call .get_result() for resulting dataframe

	NB: This class does not use yet the generic structure of the Getter class (methods query(), parse_results(), query_attributes() )
	'''
	measure_name = 'measure'

	def __init__(self,include_bots=False,**kwargs):
		self.include_bots = include_bots
		Getter.__init__(self,**kwargs)

	def clean_id(self,db,project_id):
		'''
		Available syntaxes for project_id:
		'source/owner/name','owner/name',('owner/name',source)
		project_id(int),(project_id(int),<any>),('owner/name',source_id(int)),('owner/name',None)
		When source is not provided, returns project with owner and name if only one match in DB
		'''
		if isinstance(project_id,str):
			projectname = project_id
			if len(project_id.split('/')) == 2:
				owner,name = project_id.split('/')
				source = None
			elif len(project_id.split('/')) == 3:
				source,owner,name = project_id.split('/')
			else:
				raise ValueError('Repository syntax not parsable: {}'.format(projectname))
			proj_id = db.get_repo_id(source=source,name=name,owner=owner)
			if proj_id is not None:
				return proj_id

		elif isinstance(project_id,int):
			return project_id
		elif isinstance(project_id,tuple) or isinstance(project_id,list):
			proj,source = tuple(project_id)
			if isinstance(proj,int):
				return proj
			elif isinstance(proj,str):
				if len(proj.split('/')) == 2:
					owner,name = proj.split('/')
					proj_id = db.get_repo_id(source=source,name=name,owner=owner)
					if proj_id is not None:
						return proj_id

		raise ValueError('No such repository or unparsed syntax (not int, str or tuple): {}'.format(project_id))


	def get_result(self,db=None,project_id=None,time_window=None,start_date=datetime.datetime(2013,1,1,0,0,0),end_date=datetime.datetime.now(),zero_date=datetime.datetime(1969,12,31),cumulative=True,aggregated=True):

		if db is None:
			db = self.db

		if project_id is not None:
			# if isinstance(project_id,str):
			# 	projectname = project_id
			# 	if len(project_id.split('/')) == 2:
			# 		owner,name = project_id.split('/')
			# 		source = 'GitHub'
			# 	else:
			# 		source,owner,name = project_id.split('/')

			# 	project_id = db.get_repo_id(source=source,name=name,owner=owner)
			# 	if project_id is None:
			# 		raise ValueError('No such repository: {}'.format(projectname))

			project_id = self.clean_id(db=db,project_id=project_id)

			if time_window is None:
				time_window = 'month'

			###query_proj
			# if db.db_type == 'postgres':
			# 	db.cursor.execute('''
			# 		SELECT COUNT(*),date_trunc(%(time_window)s, starred_at) + CONCAT('1 ',%(time_window)s)::interval  AS time_stamp FROM stars c
			# 		WHERE %(start_date)s <= starred_at AND starred_at < %(end_date)s
			# 		AND repo_id=%(project_id)s
			# 		GROUP BY time_stamp
			# 		''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
			# else:
			# 	db.cursor.execute('''
			# 		SELECT COUNT(*),date(datetime(starred_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp FROM stars c
			# 		WHERE datetime(:start_date) <= starred_at AND starred_at < datetime(:end_date)
			# 		AND repo_id=:project_id
			# 		GROUP BY time_stamp
			# 		''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})

			# query_result = list(db.cursor.fetchall())
			query_result = self.query_proj(db=db,project_id=project_id,time_window=time_window,start_date=start_date,end_date=end_date)

			# #correcting for datetime issue in sqlite:
			# if db.db_type == 'sqlite':
			# 	query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]


			df = pd.DataFrame(data=query_result,columns=(self.measure_name,'timestamp')).convert_dtypes()
			df.set_index('timestamp',inplace=True)
			df.sort_values(by='timestamp',inplace=True)

			if not df.empty:
				start_date_idx = df.index.min()
				end_date_idx = end_date
				idx = pd.date_range(start_date_idx,end_date_idx,freq=pandas_freq[time_window])
				# print(df)
				df = df.reindex(idx,fill_value=0)
				# print(df)
				if cumulative:
					df[self.measure_name] = df.cumsum()
					if start_date > zero_date:
						correction_df = self.get_result(db=db,project_id=project_id,time_window='year',start_date=zero_date,end_date=start_date,cumulative=True,aggregated=False)
						correction_value = correction_df[self.measure_name].fillna(0).max()
						df[self.measure_name] = df[self.measure_name]+correction_value

				complete_idx = pd.date_range(start_date,end_date,freq=pandas_freq[time_window],name='timestamp')
				df = df.reindex(complete_idx)


			return df

		else: #project_id is None

			if aggregated:
				if time_window is None:
					time_window = 'month'

				# ###query_aggregated
				# if db.db_type == 'postgres':
				# 	db.cursor.execute('''
				# 		SELECT COUNT(*),date_trunc(%(time_window)s, starred_at) + CONCAT('1 ',%(time_window)s)::interval  AS time_stamp FROM stars c
				# 		WHERE %(start_date)s <= starred_at AND starred_at < %(end_date)s
				# 		GROUP BY time_stamp
				# 		''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
				# else:
				# 	db.cursor.execute('''
				# 		SELECT COUNT(*),date(datetime(starred_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp FROM stars c
				# 		WHERE datetime(:start_date) <= starred_at AND starred_at < datetime(:end_date)
				# 		GROUP BY time_stamp
				# 		''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})

				# query_result = list(db.cursor.fetchall())
				# #correcting for datetime issue in sqlite:
				# if db.db_type == 'sqlite':
				# 	query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
				query_result = self.query_aggregated(db=db,start_date=start_date,end_date=end_date,time_window=time_window)

				df = pd.DataFrame(data=query_result,columns=(self.measure_name,'timestamp')).convert_dtypes()
				df.set_index('timestamp',inplace=True)
				df.sort_values(by='timestamp',inplace=True)

				if not df.empty:
					start_date_idx = df.index.min()
					end_date_idx = end_date
					idx = pd.date_range(start_date_idx,end_date_idx,freq=pandas_freq[time_window])
					# print(df)
					df = df.reindex(idx,fill_value=0)
					# print(df)
					if cumulative:
						df[self.measure_name] = df.cumsum()
						if start_date > zero_date:
							correction_df = self.get_result(db=db,project_id=None,time_window='year',start_date=zero_date,end_date=start_date,cumulative=True,aggregated=True)
							correction_value = correction_df[self.measure_name].fillna(0).max()
							df[self.measure_name] = df[self.measure_name]+correction_value
					complete_idx = pd.date_range(start_date,end_date,freq=pandas_freq[time_window],name='timestamp')
					df = df.reindex(complete_idx)


				return df


			else: # aggregated False: including project info
				if time_window is None:

					# ###query_notimeinfo
					# if db.db_type == 'postgres':
					# 	db.cursor.execute('''
					# 		SELECT COUNT(*),repo_id FROM stars c
					# 		WHERE %(start_date)s <= starred_at AND starred_at < %(end_date)s
					# 		GROUP BY repo_id
					# 		''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
					# else:
					# 	db.cursor.execute('''
					# 		SELECT COUNT(*),repo_id FROM stars c
					# 		WHERE :start_date <= starred_at AND starred_at < :end_date
					# 		GROUP BY repo_id
					# 		''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})

					# query_result = list(db.cursor.fetchall())

					query_result = self.query_notimeinfo(db=db,start_date=start_date,end_date=end_date)

					df = pd.DataFrame(data=query_result,columns=(self.measure_name,'project_id'))
					project_ids = generic_getters.RepoIDs(db=db).get_result()['project_id'].tolist()
					complete_idx = pd.Index(project_ids,name='project_id')

					df = df.convert_dtypes()
					df.set_index(['project_id'],inplace=True)
					df.sort_values(by='project_id',inplace=True)
					df = df.reindex(complete_idx,fill_value=0)
					df = df.convert_dtypes()
					if cumulative:
						if start_date > zero_date:
							correction_df = self.get_result(db=db,project_id=None,time_window=None,start_date=zero_date,end_date=start_date,cumulative=True,aggregated=False)
							correction_df.fillna(0,inplace=True)
							correction_df = correction_df.convert_dtypes()
							df[self.measure_name] = df[self.measure_name]+correction_df[self.measure_name]
					return df
				else: #time_window not None

					# if time_window is None:
					# 	time_window = 'month'

					# ###query_all
					# if db.db_type == 'postgres':
					# 	db.cursor.execute('''
					# 		SELECT COUNT(*),date_trunc(%(time_window)s, starred_at) AS time_stamp,repo_id FROM stars c
					# 		WHERE %(start_date)s <= starred_at AND starred_at < %(end_date)s
					# 		GROUP BY time_stamp,repo_id
					# 		''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
					# else:
					# 	db.cursor.execute('''
					# 		SELECT COUNT(*),date(datetime(starred_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp,repo_id FROM stars c
					# 		WHERE datetime(:start_date) <= starred_at AND starred_at < datetime(:end_date)
					# 		GROUP BY time_stamp,repo_id
					# 		''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})

					# query_result = list(db.cursor.fetchall())
					# #correcting for datetime issue in sqlite:
					# if db.db_type == 'sqlite':
					# 	query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d'),val_u) for val,val_d,val_u in query_result]

					query_result = self.query_all(db=db,start_date=start_date,end_date=end_date,time_window=time_window)

					df = pd.DataFrame(data=query_result,columns=(self.measure_name,'timestamp','project_id')).convert_dtypes()

					# project_ids = df['project_id'].sort_values().unique()#.tolist()
					project_ids = generic_getters.RepoIDs(db=db).get_result()['project_id'].tolist()


					df.set_index(['project_id','timestamp'],inplace=True)
					df.sort_values(by='timestamp',inplace=True)

					start_date_idx = start_date
					end_date_idx = end_date
					if not df.empty:
						# print(df)
						idx = pd.MultiIndex.from_product([project_ids,pd.date_range(start_date_idx,end_date_idx,freq=pandas_freq[time_window])],names=['project_id','timestamp'])
						df = df.reindex(idx,fill_value=0)

						if cumulative:
							df = df.groupby(level=0).cumsum().reset_index()
							df = df[df[self.measure_name]!=0]
							df.set_index(['project_id','timestamp'],inplace=True)
							# print(df)

						complete_idx = pd.MultiIndex.from_product([project_ids,pd.date_range(start_date,end_date,freq=pandas_freq[time_window])],names=['project_id','timestamp'])

						df = df.reindex(complete_idx,fill_value=0)


					return df

#########################
# stars
#########################
class Forks(ProjectGetter):
	'''
	Forks per project per time

	if project id is provided: timestamp, cumulative_forks

	When time_window needs to be used, the default value None is replaced by 'month'
	'''
	measure_name = 'forks'

	def query_proj(self,db,time_window,start_date,end_date,project_id):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%(time_window)s, forked_at) + CONCAT('1 ',%(time_window)s)::interval  AS time_stamp FROM forks c
				WHERE %(start_date)s <= forked_at AND forked_at < %(end_date)s
				AND forked_repo_id=%(project_id)s
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(forked_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp FROM forks c
				WHERE datetime(:start_date) <= forked_at AND forked_at < datetime(:end_date)
				AND forked_repo_id=:project_id
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})

		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result


	def query_aggregated(self,db,time_window,start_date,end_date,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%(time_window)s, forked_at) + CONCAT('1 ',%(time_window)s)::interval  AS time_stamp FROM forks c
				WHERE %(start_date)s <= forked_at AND forked_at < %(end_date)s
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(forked_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp FROM forks c
				WHERE datetime(:start_date) <= forked_at AND forked_at < datetime(:end_date)
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_notimeinfo(self,db,start_date,end_date,project_id=None,time_window=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),forked_repo_id FROM forks c
				WHERE %(start_date)s <= forked_at AND forked_at < %(end_date)s
				GROUP BY forked_repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT COUNT(*),forked_repo_id FROM forks c
				WHERE :start_date <= forked_at AND forked_at < :end_date
				GROUP BY forked_repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%(time_window)s, forked_at) + CONCAT('1 ',%(time_window)s)::interval  AS time_stamp,forked_repo_id FROM forks c
				WHERE %(start_date)s <= forked_at AND forked_at < %(end_date)s
				GROUP BY time_stamp,forked_repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(forked_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp,forked_repo_id FROM forks c
				WHERE datetime(:start_date) <= forked_at AND forked_at < datetime(:end_date)
				GROUP BY time_stamp,forked_repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d'),val_u) for val,val_d,val_u in query_result]
		return query_result


#########################
# stars
#########################
class Stars(ProjectGetter):
	'''
	Stars per project per time

	if project id is provided: timestamp, cumulative_stars
	if no stars_id:
		if aggregated is True: timestamp, cumulative_stars
		else: if time_window: timestamp, project_id, cumulative_stars
			else: project_id, cumulative_stars

	When time_window needs to be used, the default value None is replaced by 'month'
	'''
	measure_name = 'stars'

	def query_proj(self,db,time_window,start_date,end_date,project_id):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%(time_window)s, starred_at) + CONCAT('1 ',%(time_window)s)::interval  AS time_stamp FROM stars c
				WHERE %(start_date)s <= starred_at AND starred_at < %(end_date)s
				AND repo_id=%(project_id)s
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(starred_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp FROM stars c
				WHERE datetime(:start_date) <= starred_at AND starred_at < datetime(:end_date)
				AND repo_id=:project_id
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})

		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result


	def query_aggregated(self,db,time_window,start_date,end_date,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%(time_window)s, starred_at) + CONCAT('1 ',%(time_window)s)::interval  AS time_stamp FROM stars c
				WHERE %(start_date)s <= starred_at AND starred_at < %(end_date)s
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(starred_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp FROM stars c
				WHERE datetime(:start_date) <= starred_at AND starred_at < datetime(:end_date)
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_notimeinfo(self,db,start_date,end_date,project_id=None,time_window=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),repo_id FROM stars c
				WHERE %(start_date)s <= starred_at AND starred_at < %(end_date)s
				GROUP BY repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT COUNT(*),repo_id FROM stars c
				WHERE :start_date <= starred_at AND starred_at < :end_date
				GROUP BY repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%(time_window)s, starred_at) + CONCAT('1 ',%(time_window)s)::interval  AS time_stamp,repo_id FROM stars c
				WHERE %(start_date)s <= starred_at AND starred_at < %(end_date)s
				GROUP BY time_stamp,repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(starred_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp,repo_id FROM stars c
				WHERE datetime(:start_date) <= starred_at AND starred_at < datetime(:end_date)
				GROUP BY time_stamp,repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d'),val_u) for val,val_d,val_u in query_result]
		return query_result


#########################
# stars within community of devs
#########################
class StarsCommunity(ProjectGetter):
	'''
	Stars per project per time, only for users within the DB

	if project id is provided: timestamp, cumulative_stars
	if no stars_id:
		if aggregated is True: timestamp, cumulative_stars
		else: if time_window: timestamp, project_id, cumulative_stars
			else: project_id, cumulative_stars

	When time_window needs to be used, the default value None is replaced by 'month'
	'''
	measure_name = 'stars_community'

	def query_proj(self,db,time_window,start_date,end_date,project_id):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%(time_window)s, starred_at) + CONCAT('1 ',%(time_window)s)::interval  AS time_stamp FROM stars c
				WHERE %(start_date)s <= starred_at AND starred_at < %(end_date)s
				AND repo_id=%(project_id)s
				AND identity_id IS NOT NULL
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(starred_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp FROM stars c
				WHERE datetime(:start_date) <= starred_at AND starred_at < datetime(:end_date)
				AND repo_id=:project_id
				AND identity_id IS NOT NULL
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})

		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result


	def query_aggregated(self,db,time_window,start_date,end_date,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%(time_window)s, starred_at) + CONCAT('1 ',%(time_window)s)::interval  AS time_stamp FROM stars c
				WHERE %(start_date)s <= starred_at AND starred_at < %(end_date)s
				AND identity_id IS NOT NULL
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(starred_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp FROM stars c
				WHERE datetime(:start_date) <= starred_at AND starred_at < datetime(:end_date)
				AND identity_id IS NOT NULL
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_notimeinfo(self,db,start_date,end_date,project_id=None,time_window=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),repo_id FROM stars c
				WHERE %(start_date)s <= starred_at AND starred_at < %(end_date)s
				AND identity_id IS NOT NULL
				GROUP BY repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT COUNT(*),repo_id FROM stars c
				WHERE :start_date <= starred_at AND starred_at < :end_date
				AND identity_id IS NOT NULL
				GROUP BY repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%(time_window)s, starred_at) + CONCAT('1 ',%(time_window)s)::interval  AS time_stamp,repo_id FROM stars c
				WHERE %(start_date)s <= starred_at AND starred_at < %(end_date)s
				AND identity_id IS NOT NULL
				GROUP BY time_stamp,repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(starred_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp,repo_id FROM stars c
				WHERE datetime(:start_date) <= starred_at AND starred_at < datetime(:end_date)
				AND identity_id IS NOT NULL
				GROUP BY time_stamp,repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d'),val_u) for val,val_d,val_u in query_result]
		return query_result


#########################
# commits
#########################
class Commits(ProjectGetter):
	'''
	Commits per project per time

	if project id is provided: timestamp, cumulative_stars
	if no stars_id:
		if aggregated is True: timestamp, cumulative_stars
		else: if time_window: timestamp, project_id, cumulative_stars
			else: project_id, cumulative_stars

	When time_window needs to be used, the default value None is replaced by 'month'
	'''
	measure_name = 'commits'

	def query_proj(self,db,time_window,start_date,end_date,project_id):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%(time_window)s, c.created_at) + CONCAT('1 ',%(time_window)s)::interval AS time_stamp FROM commits c
				INNER JOIN identities i
				ON %(start_date)s <= c.created_at AND c.created_at < %(end_date)s
				AND c.repo_id=%(project_id)s
				AND i.id=c.author_id
				AND (%(include_bots)s OR NOT i.is_bot)
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(c.created_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp FROM commits c
				INNER JOIN identities i
				ON datetime(:start_date) <= c.created_at AND c.created_at < datetime(:end_date)
				AND i.id=c.author_id
				AND (:include_bots OR NOT i.is_bot)
				AND c.repo_id=:project_id
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_aggregated(self,db,time_window,start_date,end_date,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%(time_window)s, c.created_at) + CONCAT('1 ',%(time_window)s)::interval AS time_stamp FROM commits c
				INNER JOIN identities i
				ON %(start_date)s <= c.created_at AND c.created_at < %(end_date)s
				AND i.id=c.author_id
				AND (%(include_bots)s OR NOT i.is_bot)
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(c.created_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp FROM commits c
				INNER JOIN identities i
				ON datetime(:start_date) <= c.created_at AND c.created_at < datetime(:end_date)
				AND i.id=c.author_id
				AND (:include_bots OR NOT i.is_bot)
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_notimeinfo(self,db,start_date,end_date,project_id=None,time_window=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),repo_id FROM commits c
				INNER JOIN identities i
				ON %(start_date)s <= c.created_at AND c.created_at < %(end_date)s
				AND i.id=c.author_id
				AND (%(include_bots)s OR NOT i.is_bot)
				AND repo_id IS NOT NULL
				GROUP BY repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT COUNT(*),repo_id FROM commits c
				INNER JOIN identities i
				ON datetime(:start_date) <= c.created_at AND c.created_at < datetime(:end_date)
				AND i.id=c.author_id
				AND (:include_bots OR NOT i.is_bot)
				AND repo_id IS NOT NULL
				GROUP BY repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%(time_window)s, c.created_at) + CONCAT('1 ',%(time_window)s)::interval AS time_stamp,repo_id FROM commits c
				INNER JOIN identities i
				ON %(start_date)s <= c.created_at AND c.created_at < %(end_date)s
				AND i.id=c.author_id
				AND (%(include_bots)s OR NOT i.is_bot)
				AND repo_id IS NOT NULL
				GROUP BY time_stamp,repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(c.created_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp,repo_id FROM commits c
				INNER JOIN identities i
				ON datetime(:start_date) <= c.created_at AND c.created_at < datetime(:end_date)
				AND i.id=c.author_id
				AND (:include_bots OR NOT i.is_bot)
				AND repo_id IS NOT NULL
				GROUP BY time_stamp,repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d'),val_u) for val,val_d,val_u in query_result]
		return query_result


#########################
# developers
#########################
class Developers(ProjectGetter):
	'''
	New developers per project per time


	When time_window needs to be used, the default value None is replaced by 'month'
	'''
	measure_name = 'developers'

	def query_proj(self,db,time_window,start_date,end_date,project_id):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%(time_window)s, created_at) + CONCAT('1 ',%(time_window)s)::interval AS time_stamp FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				--AND %(start_date)s <= cc.created_at AND cc.created_at < %(end_date)s
				AND cc.repo_id=%(project_id)s
				AND ( %(include_bots)s OR NOT i.is_bot)
				GROUP BY i.user_id,cc.repo_id
				HAVING %(start_date)s <= MIN(cc.created_at) AND MIN(cc.created_at) < %(end_date)s
				) AS c
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(created_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				--AND datetime(:start_date) <= cc.created_at AND cc.created_at < datetime(:end_date)
				AND cc.repo_id=:project_id
				AND ( :include_bots OR NOT i.is_bot)
				GROUP BY i.user_id,cc.repo_id
				HAVING datetime(:start_date) <= MIN(cc.created_at) AND MIN(cc.created_at) < datetime(:end_date)
				) AS c
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_aggregated(self,db,time_window,start_date,end_date,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%(time_window)s, created_at) + CONCAT('1 ',%(time_window)s)::interval AS time_stamp FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id--,cc.repo_id
				FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND cc.repo_id IS NOT NULL
				AND ( %(include_bots)s OR NOT i.is_bot)
				GROUP BY i.user_id--,cc.repo_id
				HAVING %(start_date)s <= MIN(cc.created_at) AND MIN(cc.created_at) < %(end_date)s
				) AS c
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'include_bots':self.include_bots,})
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(created_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id--,cc.repo_id
				FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND cc.repo_id IS NOT NULL
				AND ( :include_bots OR NOT i.is_bot)
				GROUP BY i.user_id--,cc.repo_id
				HAVING datetime(:start_date) <= MIN(cc.created_at) AND MIN(cc.created_at) < datetime(:end_date)
				) AS c
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'include_bots':self.include_bots,})



		### Previous version: number of new links dev->repo
		# if db.db_type == 'postgres':
		# 	db.cursor.execute('''
		# 		SELECT COUNT(*),date_trunc(%(time_window)s, created_at) + CONCAT('1 ',%(time_window)s)::interval AS time_stamp FROM
		# 		(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
		# 		INNER JOIN identities i
		# 		ON i.id=cc.author_id
		# 		AND %(start_date)s <= cc.created_at AND cc.created_at < %(end_date)s
		# 		AND cc.repo_id IS NOT NULL
		# 		AND ( %(include_bots)s OR NOT i.is_bot)
		# 		GROUP BY i.user_id,cc.repo_id
		# 		) AS c
		# 		GROUP BY time_stamp
		# 		''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'include_bots':self.include_bots,})
		# else:
		# 	db.cursor.execute('''
		# 		SELECT COUNT(*),date(datetime(created_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp FROM
		# 		(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
		# 		INNER JOIN identities i
		# 		ON i.id=cc.author_id
		# 		AND datetime(:start_date) <= cc.created_at AND cc.created_at < datetime(:end_date)
		# 		AND cc.repo_id IS NOT NULL
		# 		AND ( :include_bots OR NOT i.is_bot)
		# 		GROUP BY i.user_id,cc.repo_id
		# 		) AS c
		# 		GROUP BY time_stamp
		# 		''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'include_bots':self.include_bots,})
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_notimeinfo(self,db,start_date,end_date,project_id=None,time_window=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),repo_id FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				--AND %(start_date)s <= cc.created_at AND cc.created_at < %(end_date)s
				AND cc.repo_id IS NOT NULL
				AND ( %(include_bots)s OR NOT i.is_bot)
				GROUP BY i.user_id,cc.repo_id
				HAVING %(start_date)s <= MIN(cc.created_at) AND MIN(cc.created_at) < %(end_date)s
				) AS c
				GROUP BY repo_id
				''',{'start_date':start_date,'end_date':end_date,'include_bots':self.include_bots,})
		else:
			db.cursor.execute('''
				SELECT COUNT(*),repo_id FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				--AND datetime(:start_date) <= cc.created_at AND cc.created_at < datetime(:end_date)
				AND cc.repo_id IS NOT NULL
				AND ( :include_bots OR NOT i.is_bot)
				GROUP BY i.user_id,cc.repo_id
				HAVING datetime(:start_date) <= MIN(cc.created_at) AND MIN(cc.created_at) < datetime(:end_date)
				) AS c
				GROUP BY repo_id
				''',{'start_date':start_date,'end_date':end_date,'include_bots':self.include_bots,})
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%(time_window)s, created_at) + CONCAT('1 ',%(time_window)s)::interval AS time_stamp,repo_id FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				--AND %(start_date)s <= cc.created_at AND cc.created_at < %(end_date)s
				AND cc.repo_id IS NOT NULL
				AND ( %(include_bots)s OR NOT i.is_bot)
				GROUP BY i.user_id,cc.repo_id
				HAVING %(start_date)s <= MIN(cc.created_at) AND MIN(cc.created_at) < %(end_date)s
				) AS c
				GROUP BY time_stamp,repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'include_bots':self.include_bots,})
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(created_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp,repo_id FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND cc.repo_id IS NOT NULL
				--AND datetime(:start_date) <= cc.created_at AND cc.created_at < datetime(:end_date)
				AND ( :include_bots OR NOT i.is_bot)
				GROUP BY i.user_id,cc.repo_id
				HAVING datetime(:start_date) <= MIN(cc.created_at) AND MIN(cc.created_at) < datetime(:end_date)
				) AS c
				GROUP BY time_stamp,repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'include_bots':self.include_bots,})
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d'),val_u) for val,val_d,val_u in query_result]
		return query_result


#########################
# active developers
#########################
class ActiveDevelopers(ProjectGetter):
	'''
	Active developers per project per time


	When time_window needs to be used, the default value None is replaced by 'month'
	'''
	measure_name = 'active_developers'

	def query_proj(self,db,time_window,start_date,end_date,project_id):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*), time_stamp FROM
				(SELECT date_trunc(%(time_window)s, cc.created_at) + CONCAT('1 ',%(time_window)s)::interval AS time_stamp,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND %(start_date)s <= cc.created_at AND cc.created_at < %(end_date)s
				AND cc.repo_id=%(project_id)s
				AND ( %(include_bots)s OR NOT i.is_bot)
				GROUP BY time_stamp,i.user_id,cc.repo_id
				) AS c
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT COUNT(*), time_stamp FROM
				(SELECT date(datetime(cc.created_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND datetime(:start_date) <= cc.created_at AND cc.created_at < datetime(:end_date)
				AND cc.repo_id=:project_id
				AND ( :include_bots OR NOT i.is_bot)
				GROUP BY time_stamp,i.user_id,cc.repo_id
				) AS c
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_aggregated(self,db,time_window,start_date,end_date,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*), time_stamp FROM
				(SELECT date_trunc(%(time_window)s, cc.created_at) + CONCAT('1 ',%(time_window)s)::interval AS time_stamp,i.user_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND %(start_date)s <= cc.created_at AND cc.created_at < %(end_date)s
				AND ( %(include_bots)s OR NOT i.is_bot)
				GROUP BY time_stamp,i.user_id
				) AS c
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'include_bots':self.include_bots,})
		else:
			db.cursor.execute('''
				SELECT COUNT(*), time_stamp FROM
				(SELECT date(datetime(cc.created_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp,i.user_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND datetime(:start_date) <= cc.created_at AND cc.created_at < datetime(:end_date)
				AND ( :include_bots OR NOT i.is_bot)
				GROUP BY time_stamp,i.user_id
				) AS c
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'include_bots':self.include_bots,})
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_notimeinfo(self,db,start_date,end_date,project_id=None,time_window=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(DISTINCT user_id),repo_id FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND %(start_date)s <= cc.created_at AND cc.created_at < %(end_date)s
				AND cc.repo_id IS NOT NULL
				AND ( %(include_bots)s OR NOT i.is_bot)
				GROUP BY i.user_id,cc.repo_id
				) AS c
				GROUP BY repo_id
				''',{'start_date':start_date,'end_date':end_date,'include_bots':self.include_bots,})
		else:
			db.cursor.execute('''
				SELECT COUNT(DISTINCT user_id),repo_id FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND datetime(:start_date) <= cc.created_at AND cc.created_at < datetime(:end_date)
				AND cc.repo_id IS NOT NULL
				AND ( :include_bots OR NOT i.is_bot)
				GROUP BY i.user_id,cc.repo_id
				) AS c
				GROUP BY repo_id
				''',{'start_date':start_date,'end_date':end_date,'include_bots':self.include_bots,})
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*), time_stamp, repo_id FROM
				(SELECT date_trunc(%(time_window)s, cc.created_at) + CONCAT('1 ',%(time_window)s)::interval AS time_stamp,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND %(start_date)s <= cc.created_at AND cc.created_at < %(end_date)s
				AND cc.repo_id IS NOT NULL
				AND ( %(include_bots)s OR NOT i.is_bot)
				GROUP BY time_stamp,i.user_id,cc.repo_id
				) AS c
				GROUP BY time_stamp,repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'include_bots':self.include_bots,})
		else:
			db.cursor.execute('''
				SELECT COUNT(*), time_stamp, repo_id FROM
				(SELECT date(datetime(cc.created_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND datetime(:start_date) <= cc.created_at AND cc.created_at < datetime(:end_date)
				AND cc.repo_id IS NOT NULL
				AND ( :include_bots OR NOT i.is_bot)
				GROUP BY time_stamp,i.user_id,cc.repo_id
				) AS c
				GROUP BY time_stamp,repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'include_bots':self.include_bots,})
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d'),val_u) for val,val_d,val_u in query_result]
		return query_result


#########################
# total lines
#########################
class TotalLines(ProjectGetter):
	'''
	insertions+deletions


	When time_window needs to be used, the default value None is replaced by 'month'
	'''
	measure_name = 'total_lines'

	def query_proj(self,db,time_window,start_date,end_date,project_id):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT SUM(insertions+deletions),date_trunc(%(time_window)s, created_at) + CONCAT('1 ',%(time_window)s)::interval  AS time_stamp FROM commits c
				WHERE %(start_date)s <= created_at AND created_at < %(end_date)s
				AND repo_id=%(project_id)s
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT SUM(insertions+deletions),date(datetime(created_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp FROM commits c
				WHERE datetime(:start_date) <= created_at AND created_at < datetime(:end_date)
				AND repo_id=:project_id
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_aggregated(self,db,time_window,start_date,end_date,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT SUM(insertions+deletions),date_trunc(%(time_window)s, created_at) + CONCAT('1 ',%(time_window)s)::interval  AS time_stamp FROM commits c
				WHERE %(start_date)s <= created_at AND created_at < %(end_date)s
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT SUM(insertions+deletions),date(datetime(created_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp FROM commits c
				WHERE datetime(:start_date) <= created_at AND created_at < datetime(:end_date)
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_notimeinfo(self,db,start_date,end_date,project_id=None,time_window=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT SUM(insertions+deletions),repo_id FROM commits c
				WHERE %(start_date)s <= created_at AND created_at < %(end_date)s
				AND c.repo_id IS NOT NULL
				GROUP BY repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT SUM(insertions+deletions),repo_id FROM commits c
				WHERE :start_date <= created_at AND created_at < :end_date
				AND c.repo_id IS NOT NULL
				GROUP BY repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT SUM(insertions+deletions),date_trunc(%(time_window)s, created_at) + CONCAT('1 ',%(time_window)s)::interval  AS time_stamp,repo_id FROM commits c
				WHERE %(start_date)s <= created_at AND created_at < %(end_date)s
				AND c.repo_id IS NOT NULL
				GROUP BY time_stamp,repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT SUM(insertions+deletions),date(datetime(created_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp,repo_id FROM commits c
				WHERE datetime(:start_date) <= created_at AND created_at < datetime(:end_date)
				AND c.repo_id IS NOT NULL
				GROUP BY time_stamp,repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d'),val_u) for val,val_d,val_u in query_result]
		return query_result



#########################
# lines
#########################
class Lines(ProjectGetter):
	'''
	insertions-deletions


	When time_window needs to be used, the default value None is replaced by 'month'
	'''
	measure_name = 'lines'

	def query_proj(self,db,time_window,start_date,end_date,project_id):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT SUM(insertions-deletions),date_trunc(%(time_window)s, created_at) + CONCAT('1 ',%(time_window)s)::interval  AS time_stamp FROM commits c
				WHERE %(start_date)s <= created_at AND created_at < %(end_date)s
				AND repo_id=%(project_id)s
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT SUM(insertions-deletions),date(datetime(created_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp FROM commits c
				WHERE datetime(:start_date) <= created_at AND created_at < datetime(:end_date)
				AND repo_id=:project_id
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_aggregated(self,db,time_window,start_date,end_date,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT SUM(insertions-deletions),date_trunc(%(time_window)s, created_at) + CONCAT('1 ',%(time_window)s)::interval  AS time_stamp FROM commits c
				WHERE %(start_date)s <= created_at AND created_at < %(end_date)s
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT SUM(insertions-deletions),date(datetime(created_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp FROM commits c
				WHERE datetime(:start_date) <= created_at AND created_at < datetime(:end_date)
				GROUP BY time_stamp
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_notimeinfo(self,db,start_date,end_date,project_id=None,time_window=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT SUM(insertions-deletions),repo_id FROM commits c
				WHERE %(start_date)s <= created_at AND created_at < %(end_date)s
				AND c.repo_id IS NOT NULL
				GROUP BY repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT SUM(insertions-deletions),repo_id FROM commits c
				WHERE :start_date <= created_at AND created_at < :end_date
				AND c.repo_id IS NOT NULL
				GROUP BY repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT SUM(insertions-deletions),date_trunc(%(time_window)s, created_at) + CONCAT('1 ',%(time_window)s)::interval  AS time_stamp,repo_id FROM commits c
				WHERE %(start_date)s <= created_at AND created_at < %(end_date)s
				AND c.repo_id IS NOT NULL
				GROUP BY time_stamp,repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		else:
			db.cursor.execute('''
				SELECT SUM(insertions-deletions),date(datetime(created_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp,repo_id FROM commits c
				WHERE datetime(:start_date) <= created_at AND created_at < datetime(:end_date)
				AND c.repo_id IS NOT NULL
				GROUP BY time_stamp,repo_id
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,'include_bots':self.include_bots})
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d'),val_u) for val,val_d,val_u in query_result]
		return query_result



#########################
# lines
#########################
class Downloads(ProjectGetter):
	'''
	downloads from all packages linked to the repository

	When time_window needs to be used, the default value None is replaced by 'month'
	'''
	measure_name = 'downloads'

	def query_proj(self,db,time_window,start_date,end_date,project_id):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT SUM(vd.downloads),date_trunc(%(time_window)s, vd.downloaded_at)::timestamp + CONCAT('1 ',%(time_window)s)::interval  AS time_stamp
				FROM packages p
				INNER JOIN package_versions v
				ON p.repo_id=%(project_id)s
				AND v.package_id=p.id
				INNER JOIN package_version_downloads vd
				ON vd.package_version=v.id
				AND %(start_date)s <= vd.downloaded_at AND vd.downloaded_at < %(end_date)s
				GROUP BY time_stamp;
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,})
		else:
			db.cursor.execute('''
				SELECT SUM(vd.downloads),date(datetime(vd.downloaded_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp
				FROM packages p
				INNER JOIN package_versions v
				ON p.repo_id=:project_id
				AND v.package_id=p.id
				INNER JOIN package_version_downloads vd
				ON vd.package_version=v.id
				AND datetime(:start_date) <= vd.downloaded_at AND vd.downloaded_at < datetime(:end_date)
				GROUP BY time_stamp
				;
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,'project_id':project_id,})
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_aggregated(self,db,time_window,start_date,end_date,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT SUM(vd.downloads),date_trunc(%(time_window)s, vd.downloaded_at)::timestamp + CONCAT('1 ',%(time_window)s)::interval  AS time_stamp
				FROM packages p
				INNER JOIN package_versions v
				ON v.package_id=p.id AND p.repo_id IS NOT NULL
				INNER JOIN package_version_downloads vd
				ON vd.package_version=v.id
				AND %(start_date)s <= vd.downloaded_at AND vd.downloaded_at < %(end_date)s
				GROUP BY time_stamp;
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,})
		else:
			db.cursor.execute('''
				SELECT SUM(vd.downloads),date(datetime(vd.downloaded_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp
				FROM packages p
				INNER JOIN package_versions v
				ON v.package_id=p.id AND p.repo_id IS NOT NULL
				INNER JOIN package_version_downloads vd
				ON vd.package_version=v.id
				AND datetime(:start_date) <= vd.downloaded_at AND vd.downloaded_at < datetime(:end_date)
				GROUP BY time_stamp
				;
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,})
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_notimeinfo(self,db,start_date,end_date,project_id=None,time_window=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT SUM(vd.downloads),p.repo_id FROM packages p
				INNER JOIN package_versions v
				ON v.package_id=p.id AND p.repo_id IS NOT NULL
				INNER JOIN package_version_downloads vd
				ON vd.package_version=v.id
				AND %(start_date)s <= vd.downloaded_at AND vd.downloaded_at < %(end_date)s
				GROUP BY p.repo_id
				;
				''',{'start_date':start_date,'end_date':end_date,})
		else:
			db.cursor.execute('''
				SELECT SUM(vd.downloads),p.repo_id FROM packages p
				INNER JOIN package_versions v
				ON v.package_id=p.id AND p.repo_id IS NOT NULL
				INNER JOIN package_version_downloads vd
				ON vd.package_version=v.id
				AND :start_date <= vd.downloaded_at AND vd.downloaded_at < :end_date
				GROUP BY p.repo_id
				;
				''',{'start_date':start_date,'end_date':end_date,})
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT SUM(vd.downloads),date_trunc(%(time_window)s, vd.downloaded_at)::timestamp + CONCAT('1 ',%(time_window)s)::interval  AS time_stamp,p.repo_id
				FROM packages p
				INNER JOIN package_versions v
				ON v.package_id=p.id AND p.repo_id IS NOT NULL
				INNER JOIN package_version_downloads vd
				ON vd.package_version=v.id
				AND %(start_date)s <= vd.downloaded_at AND vd.downloaded_at < %(end_date)s
				GROUP BY time_stamp,p.repo_id;
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,})
		else:
			db.cursor.execute('''
				SELECT SUM(vd.downloads),date(datetime(vd.downloaded_at,'start of '||:time_window),'+1 '||:time_window||'s') AS time_stamp,p.repo_id
				FROM packages p
				INNER JOIN package_versions v
				ON v.package_id=p.id AND p.repo_id IS NOT NULL
				INNER JOIN package_version_downloads vd
				ON vd.package_version=v.id
				AND datetime(:start_date) <= vd.downloaded_at AND vd.downloaded_at < datetime(:end_date)
				GROUP BY time_stamp,p.repo_id
				;
				''',{'time_window':time_window,'start_date':start_date,'end_date':end_date,})
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d'),val_u) for val,val_d,val_u in query_result]
		return query_result


