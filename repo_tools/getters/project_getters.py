
import pandas as pd
import datetime
import numpy as np
# For email_id replacing repo_id when no login available: COALESCE(repo_id,-email_id)
from . import pandas_freq
from .generic_getters import Getter


class ProjectGetter(Getter):
	'''
	Wrapper around the measures for projects, containing the shared code
	define subclasses, define the 4 subfunctions (SQL queries)
	call .get_result() for resulting dataframe

	NB: This class does not use yet the generic structure of the Getter class (methods query(), parse_results(), query_attributes() )
	'''
	measure_name = 'measure'

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


	def get_result(self,db,project_id=None,time_window=None,start_date=datetime.datetime(2013,1,1,0,0,0),end_date=datetime.datetime.now(),cumulative=True,aggregated=True):


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
			# 		SELECT COUNT(*),date_trunc(%s, starred_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM stars c
			# 		WHERE %s <= starred_at AND starred_at < %s
			# 		AND repo_id=%s
			# 		GROUP BY time_stamp
			# 		''',(time_window,time_window,start_date,end_date,project_id,))
			# else:
			# 	db.cursor.execute('''
			# 		SELECT COUNT(*),date(datetime(starred_at,?),'+1 '||?||'s') AS time_stamp FROM stars c
			# 		WHERE datetime(?) <= starred_at AND starred_at < datetime(?)
			# 		AND repo_id=?
			# 		GROUP BY time_stamp
			# 		''',('start of {}'.format(time_window),time_window,start_date,end_date,project_id,))

			# query_result = list(db.cursor.fetchall())
			query_result = self.query_proj(db=db,project_id=project_id,time_window=time_window,start_date=start_date,end_date=end_date)

			# #correcting for datetime issue in sqlite:
			# if db.db_type == 'sqlite':
			# 	query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]


			df = pd.DataFrame(data=query_result,columns=(self.measure_name,'timestamp'))
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
				# 		SELECT COUNT(*),date_trunc(%s, starred_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM stars c
				# 		WHERE %s <= starred_at AND starred_at < %s
				# 		GROUP BY time_stamp
				# 		''',(time_window,time_window,start_date,end_date,))
				# else:
				# 	db.cursor.execute('''
				# 		SELECT COUNT(*),date(datetime(starred_at,?),'+1 '||?||'s') AS time_stamp FROM stars c
				# 		WHERE datetime(?) <= starred_at AND starred_at < datetime(?)
				# 		GROUP BY time_stamp
				# 		''',('start of {}'.format(time_window),time_window,start_date,end_date,))

				# query_result = list(db.cursor.fetchall())
				# #correcting for datetime issue in sqlite:
				# if db.db_type == 'sqlite':
				# 	query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
				query_result = self.query_aggregated(db=db,start_date=start_date,end_date=end_date,time_window=time_window)

				df = pd.DataFrame(data=query_result,columns=(self.measure_name,'timestamp'))
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
					complete_idx = pd.date_range(start_date,end_date,freq=pandas_freq[time_window],name='timestamp')
					df = df.reindex(complete_idx)


				return df


			else: # aggregated False: including project info
				if time_window is None:

					# ###query_notimeinfo
					# if db.db_type == 'postgres':
					# 	db.cursor.execute('''
					# 		SELECT COUNT(*),repo_id FROM stars c
					# 		WHERE %s <= starred_at AND starred_at < %s
					# 		GROUP BY repo_id
					# 		''',(start_date,end_date,))
					# else:
					# 	db.cursor.execute('''
					# 		SELECT COUNT(*),repo_id FROM stars c
					# 		WHERE ? <= starred_at AND starred_at < ?
					# 		GROUP BY repo_id
					# 		''',(start_date,end_date,))

					# query_result = list(db.cursor.fetchall())

					query_result = self.query_notimeinfo(db=db,start_date=start_date,end_date=end_date)

					df = pd.DataFrame(data=query_result,columns=(self.measure_name,'project_id'))
					df.set_index(['project_id'],inplace=True)
					df.sort_values(by='project_id',inplace=True)
					return df
				else: #time_window not None

					# if time_window is None:
					# 	time_window = 'month'

					# ###query_all
					# if db.db_type == 'postgres':
					# 	db.cursor.execute('''
					# 		SELECT COUNT(*),date_trunc(%s, starred_at) AS time_stamp,repo_id FROM stars c
					# 		WHERE %s <= starred_at AND starred_at < %s
					# 		GROUP BY time_stamp,repo_id
					# 		''',(time_window,time_window,start_date,end_date,))
					# else:
					# 	db.cursor.execute('''
					# 		SELECT COUNT(*),date(datetime(starred_at,?),'+1 '||?||'s') AS time_stamp,repo_id FROM stars c
					# 		WHERE datetime(?) <= starred_at AND starred_at < datetime(?)
					# 		GROUP BY time_stamp,repo_id
					# 		''',('start of {}'.format(time_window),time_window,start_date,end_date,))

					# query_result = list(db.cursor.fetchall())
					# #correcting for datetime issue in sqlite:
					# if db.db_type == 'sqlite':
					# 	query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d'),val_u) for val,val_d,val_u in query_result]

					query_result = self.query_all(db=db,start_date=start_date,end_date=end_date,time_window=time_window)

					df = pd.DataFrame(data=query_result,columns=(self.measure_name,'timestamp','project_id'))

					project_ids = df['project_id'].sort_values().unique().tolist()


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

						df = df.reindex(complete_idx)


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
				SELECT COUNT(*),date_trunc(%s, forked_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM forks c
				WHERE %s <= forked_at AND forked_at < %s
				AND forked_repo_id=%s
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,project_id,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(forked_at,?),'+1 '||?||'s') AS time_stamp FROM forks c
				WHERE datetime(?) <= forked_at AND forked_at < datetime(?)
				AND forked_repo_id=?
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,project_id,))

		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result


	def query_aggregated(self,db,time_window,start_date,end_date,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, forked_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM forks c
				WHERE %s <= forked_at AND forked_at < %s
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(forked_at,?),'+1 '||?||'s') AS time_stamp FROM forks c
				WHERE datetime(?) <= forked_at AND forked_at < datetime(?)
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_notimeinfo(self,db,start_date,end_date,project_id=None,time_window=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),forked_repo_id FROM forks c
				WHERE %s <= forked_at AND forked_at < %s
				GROUP BY forked_repo_id
				''',(start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),forked_repo_id FROM forks c
				WHERE ? <= forked_at AND forked_at < ?
				GROUP BY forked_repo_id
				''',(start_date,end_date,))
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, forked_at) + CONCAT('1 ',%s)::interval  AS time_stamp,forked_repo_id FROM forks c
				WHERE %s <= forked_at AND forked_at < %s
				GROUP BY time_stamp,forked_repo_id
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(forked_at,?),'+1 '||?||'s') AS time_stamp,forked_repo_id FROM forks c
				WHERE datetime(?) <= forked_at AND forked_at < datetime(?)
				GROUP BY time_stamp,forked_repo_id
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
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
				SELECT COUNT(*),date_trunc(%s, starred_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM stars c
				WHERE %s <= starred_at AND starred_at < %s
				AND repo_id=%s
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,project_id,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(starred_at,?),'+1 '||?||'s') AS time_stamp FROM stars c
				WHERE datetime(?) <= starred_at AND starred_at < datetime(?)
				AND repo_id=?
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,project_id,))

		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result


	def query_aggregated(self,db,time_window,start_date,end_date,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, starred_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM stars c
				WHERE %s <= starred_at AND starred_at < %s
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(starred_at,?),'+1 '||?||'s') AS time_stamp FROM stars c
				WHERE datetime(?) <= starred_at AND starred_at < datetime(?)
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_notimeinfo(self,db,start_date,end_date,project_id=None,time_window=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),repo_id FROM stars c
				WHERE %s <= starred_at AND starred_at < %s
				GROUP BY repo_id
				''',(start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),repo_id FROM stars c
				WHERE ? <= starred_at AND starred_at < ?
				GROUP BY repo_id
				''',(start_date,end_date,))
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, starred_at) + CONCAT('1 ',%s)::interval  AS time_stamp,repo_id FROM stars c
				WHERE %s <= starred_at AND starred_at < %s
				GROUP BY time_stamp,repo_id
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(starred_at,?),'+1 '||?||'s') AS time_stamp,repo_id FROM stars c
				WHERE datetime(?) <= starred_at AND starred_at < datetime(?)
				GROUP BY time_stamp,repo_id
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
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
				SELECT COUNT(*),date_trunc(%s, starred_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM stars c
				WHERE %s <= starred_at AND starred_at < %s
				AND repo_id=%s
				AND identity_id IS NOT NULL
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,project_id,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(starred_at,?),'+1 '||?||'s') AS time_stamp FROM stars c
				WHERE datetime(?) <= starred_at AND starred_at < datetime(?)
				AND repo_id=?
				AND identity_id IS NOT NULL
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,project_id,))

		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result


	def query_aggregated(self,db,time_window,start_date,end_date,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, starred_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM stars c
				WHERE %s <= starred_at AND starred_at < %s
				AND identity_id IS NOT NULL
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(starred_at,?),'+1 '||?||'s') AS time_stamp FROM stars c
				WHERE datetime(?) <= starred_at AND starred_at < datetime(?)
				AND identity_id IS NOT NULL
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_notimeinfo(self,db,start_date,end_date,project_id=None,time_window=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),repo_id FROM stars c
				WHERE %s <= starred_at AND starred_at < %s
				AND identity_id IS NOT NULL
				GROUP BY repo_id
				''',(start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),repo_id FROM stars c
				WHERE ? <= starred_at AND starred_at < ?
				AND identity_id IS NOT NULL
				GROUP BY repo_id
				''',(start_date,end_date,))
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, starred_at) + CONCAT('1 ',%s)::interval  AS time_stamp,repo_id FROM stars c
				WHERE %s <= starred_at AND starred_at < %s
				AND identity_id IS NOT NULL
				GROUP BY time_stamp,repo_id
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(starred_at,?),'+1 '||?||'s') AS time_stamp,repo_id FROM stars c
				WHERE datetime(?) <= starred_at AND starred_at < datetime(?)
				AND identity_id IS NOT NULL
				GROUP BY time_stamp,repo_id
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
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
				SELECT COUNT(*),date_trunc(%s, created_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM commits c
				WHERE %s <= created_at AND created_at < %s
				AND repo_id=%s
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,project_id,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(created_at,?),'+1 '||?||'s') AS time_stamp FROM commits c
				WHERE datetime(?) <= created_at AND created_at < datetime(?)
				AND repo_id=?
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,project_id,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_aggregated(self,db,time_window,start_date,end_date,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, created_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM commits c
				WHERE %s <= created_at AND created_at < %s
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(created_at,?),'+1 '||?||'s') AS time_stamp FROM commits c
				WHERE datetime(?) <= created_at AND created_at < datetime(?)
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_notimeinfo(self,db,start_date,end_date,project_id=None,time_window=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),repo_id FROM commits c
				WHERE %s <= created_at AND created_at < %s
				AND repo_id IS NOT NULL
				GROUP BY repo_id
				''',(start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),repo_id FROM commits c
				WHERE ? <= created_at AND created_at < ?
				AND repo_id IS NOT NULL
				GROUP BY repo_id
				''',(start_date,end_date,))
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, created_at) + CONCAT('1 ',%s)::interval  AS time_stamp,repo_id FROM commits c
				WHERE %s <= created_at AND created_at < %s
				AND repo_id IS NOT NULL
				GROUP BY time_stamp,repo_id
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(created_at,?),'+1 '||?||'s') AS time_stamp,repo_id FROM commits c
				WHERE datetime(?) <= created_at AND created_at < datetime(?)
				AND repo_id IS NOT NULL
				GROUP BY time_stamp,repo_id
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
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
				SELECT COUNT(*),date_trunc(%s, created_at) + CONCAT('1 ',%s)::interval AS time_stamp FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND %s <= cc.created_at AND cc.created_at < %s
				AND cc.repo_id=%s
				GROUP BY i.user_id,cc.repo_id
				) AS c
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,project_id,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(created_at,?),'+1 '||?||'s') AS time_stamp FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				WHERE datetime(?) <= cc.created_at AND cc.created_at < datetime(?)
				AND cc.repo_id=?
				GROUP BY i.user_id,cc.repo_id
				) AS c
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,project_id,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_aggregated(self,db,time_window,start_date,end_date,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, created_at) + CONCAT('1 ',%s)::interval AS time_stamp FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND %s <= cc.created_at AND cc.created_at < %s
				AND cc.repo_id IS NOT NULL
				GROUP BY i.user_id,cc.repo_id
				) AS c
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(created_at,?),'+1 '||?||'s') AS time_stamp FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND datetime(?) <= cc.created_at AND cc.created_at < datetime(?)
				AND cc.repo_id IS NOT NULL
				GROUP BY i.user_id,cc.repo_id
				) AS c
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
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
				AND %s <= cc.created_at AND cc.created_at < %s
				AND cc.repo_id IS NOT NULL
				GROUP BY i.user_id,cc.repo_id
				) AS c
				GROUP BY repo_id
				''',(start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),repo_id FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND datetime(?) <= cc.created_at AND cc.created_at < datetime(?)
				AND cc.repo_id IS NOT NULL
				GROUP BY i.user_id,cc.repo_id
				) AS c
				GROUP BY repo_id
				''',(start_date,end_date,))
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, created_at) + CONCAT('1 ',%s)::interval AS time_stamp,repo_id FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND %s <= cc.created_at AND cc.created_at < %s
				AND cc.repo_id IS NOT NULL
				GROUP BY i.user_id,cc.repo_id
				) AS c
				GROUP BY time_stamp,repo_id
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(created_at,?),'+1 '||?||'s') AS time_stamp,repo_id FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND datetime(?) <= cc.created_at AND cc.created_at < datetime(?)
				GROUP BY i.user_id,cc.repo_id
				AND cc.repo_id IS NOT NULL
				) AS c
				GROUP BY time_stamp,repo_id
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
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
				(SELECT date_trunc(%s, cc.created_at) + CONCAT('1 ',%s)::interval AS time_stamp,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND %s <= cc.created_at AND cc.created_at < %s
				AND cc.repo_id=%s
				GROUP BY time_stamp,i.user_id,cc.repo_id
				) AS c
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,project_id,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*), time_stamp FROM
				(SELECT date(datetime(created_at,?),'+1 '||?||'s') AS time_stamp,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND datetime(?) <= cc.created_at AND cc.created_at < datetime(?)
				AND cc.repo_id=?
				GROUP BY time_stamp,i.user_id,cc.repo_id
				) AS c
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,project_id,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_aggregated(self,db,time_window,start_date,end_date,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*), time_stamp FROM
				(SELECT date_trunc(%s, cc.created_at) + CONCAT('1 ',%s)::interval AS time_stamp,i.user_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND %s <= cc.created_at AND cc.created_at < %s
				GROUP BY time_stamp,i.user_id
				) AS c
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*), time_stamp FROM
				(SELECT date(datetime(cc.created_at,?),'+1 '||?||'s') AS time_stamp,i.user_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND datetime(?) <= cc.created_at AND cc.created_at < datetime(?)
				GROUP BY time_stamp,i.user_id
				) AS c
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
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
				AND %s <= cc.created_at AND cc.created_at < %s
				AND cc.repo_id IS NOT NULL
				GROUP BY i.user_id,cc.repo_id
				) AS c
				GROUP BY repo_id
				''',(start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),repo_id FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND datetime(?) <= cc.created_at AND cc.created_at < datetime(?)
				AND cc.repo_id IS NOT NULL
				GROUP BY i.user_id,cc.repo_id
				) AS c
				GROUP BY repo_id
				''',(start_date,end_date,))
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*), time_stamp, repo_id FROM
				(SELECT date_trunc(%s, cc.created_at) + CONCAT('1 ',%s)::interval AS time_stamp,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND %s <= cc.created_at AND cc.created_at < %s
				AND cc.repo_id IS NOT NULL
				GROUP BY time_stamp,i.user_id,cc.repo_id
				) AS c
				GROUP BY time_stamp,repo_id
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*), time_stamp, repo_id FROM
				(SELECT date(datetime(cc.created_at,?),'+1 '||?||'s') AS time_stamp,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND datetime(?) <= cc.created_at AND cc.created_at < datetime(?)
				AND cc.repo_id IS NOT NULL
				GROUP BY time_stamp,i.user_id,cc.repo_id
				) AS c
				GROUP BY time_stamp,repo_id
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
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
				SELECT SUM(insertions+deletions),date_trunc(%s, created_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM commits c
				WHERE %s <= created_at AND created_at < %s
				AND repo_id=%s
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,project_id,))
		else:
			db.cursor.execute('''
				SELECT SUM(insertions+deletions),date(datetime(created_at,?),'+1 '||?||'s') AS time_stamp FROM commits c
				WHERE datetime(?) <= created_at AND created_at < datetime(?)
				AND repo_id=?
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,project_id,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_aggregated(self,db,time_window,start_date,end_date,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT SUM(insertions+deletions),date_trunc(%s, created_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM commits c
				WHERE %s <= created_at AND created_at < %s
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT SUM(insertions+deletions),date(datetime(created_at,?),'+1 '||?||'s') AS time_stamp FROM commits c
				WHERE datetime(?) <= created_at AND created_at < datetime(?)
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_notimeinfo(self,db,start_date,end_date,project_id=None,time_window=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT SUM(insertions+deletions),repo_id FROM commits c
				WHERE %s <= created_at AND created_at < %s
				AND c.repo_id IS NOT NULL
				GROUP BY repo_id
				''',(start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT SUM(insertions+deletions),repo_id FROM commits c
				WHERE ? <= created_at AND created_at < ?
				AND c.repo_id IS NOT NULL
				GROUP BY repo_id
				''',(start_date,end_date,))
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT SUM(insertions+deletions),date_trunc(%s, created_at) + CONCAT('1 ',%s)::interval  AS time_stamp,repo_id FROM commits c
				WHERE %s <= created_at AND created_at < %s
				AND c.repo_id IS NOT NULL
				GROUP BY time_stamp,repo_id
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT SUM(insertions+deletions),date(datetime(created_at,?),'+1 '||?||'s') AS time_stamp,repo_id FROM commits c
				WHERE datetime(?) <= created_at AND created_at < datetime(?)
				AND c.repo_id IS NOT NULL
				GROUP BY time_stamp,repo_id
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
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
				SELECT SUM(insertions-deletions),date_trunc(%s, created_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM commits c
				WHERE %s <= created_at AND created_at < %s
				AND repo_id=%s
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,project_id,))
		else:
			db.cursor.execute('''
				SELECT SUM(insertions-deletions),date(datetime(created_at,?),'+1 '||?||'s') AS time_stamp FROM commits c
				WHERE datetime(?) <= created_at AND created_at < datetime(?)
				AND repo_id=?
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,project_id,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_aggregated(self,db,time_window,start_date,end_date,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT SUM(insertions-deletions),date_trunc(%s, created_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM commits c
				WHERE %s <= created_at AND created_at < %s
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT SUM(insertions-deletions),date(datetime(created_at,?),'+1 '||?||'s') AS time_stamp FROM commits c
				WHERE datetime(?) <= created_at AND created_at < datetime(?)
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_notimeinfo(self,db,start_date,end_date,project_id=None,time_window=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT SUM(insertions-deletions),repo_id FROM commits c
				WHERE %s <= created_at AND created_at < %s
				AND c.repo_id IS NOT NULL
				GROUP BY repo_id
				''',(start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT SUM(insertions-deletions),repo_id FROM commits c
				WHERE ? <= created_at AND created_at < ?
				AND c.repo_id IS NOT NULL
				GROUP BY repo_id
				''',(start_date,end_date,))
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,project_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT SUM(insertions-deletions),date_trunc(%s, created_at) + CONCAT('1 ',%s)::interval  AS time_stamp,repo_id FROM commits c
				WHERE %s <= created_at AND created_at < %s
				AND c.repo_id IS NOT NULL
				GROUP BY time_stamp,repo_id
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT SUM(insertions-deletions),date(datetime(created_at,?),'+1 '||?||'s') AS time_stamp,repo_id FROM commits c
				WHERE datetime(?) <= created_at AND created_at < datetime(?)
				AND c.repo_id IS NOT NULL
				GROUP BY time_stamp,repo_id
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d'),val_u) for val,val_d,val_u in query_result]
		return query_result


