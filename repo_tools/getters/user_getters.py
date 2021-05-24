
import pandas as pd
import datetime
import numpy as np
# For email_id replacing author_id when no login available: COALESCE(author_id,-email_id)
from . import pandas_freq
from .generic_getters import Getter


class UserGetter(Getter):
	'''
	Wrapper around the measures for users, containing the shared code
	define subclasses, define the 4 subfunctions (SQL queries)
	call .get_result() for resulting dataframe

	NB: This class does not use yet the generic structure of the Getter class (methods query(), parse_results(), query_attributes() )
	'''
	measure_name = 'measure'

	def clean_id(self,db,user_id=None,identity_id=None):
		return db.get_user_id(user_id=user_id,identity_id=identity_id)

	def get_result(self,db,user_id=None,identity_id=None,time_window=None,start_date=datetime.datetime(2013,1,1,0,0,0),end_date=datetime.datetime.now(),cumulative=True,aggregated=True):

		if user_id is not None or identity_id is not None:
			user_id = self.clean_id(db=db,user_id=user_id,identity_id=identity_id)
			if time_window is None:
				time_window = 'month'

			# if db.db_type == 'postgres':
			# 	db.cursor.execute('''
			# 		SELECT COUNT(*),date_trunc(%s, created_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM commits c
			# 		WHERE %s <= created_at AND created_at < %s
			# 		AND author_id=%s
			# 		GROUP BY time_stamp
			# 		''',(time_window,time_window,start_date,end_date,user_id,))
			# else:
			# 	db.cursor.execute('''
			# 		SELECT COUNT(*),date(datetime(created_at,?),'+1 '||?||'s') AS time_stamp FROM commits c
			# 		WHERE datetime(?) <= created_at AND created_at < datetime(?)
			# 		AND author_id=?
			# 		GROUP BY time_stamp
			# 		''',('start of {}'.format(time_window),time_window,start_date,end_date,user_id,))

			# query_result = list(db.cursor.fetchall())

			query_result = self.query_user(db=db,user_id=user_id,time_window=time_window,start_date=start_date,end_date=end_date)
			#correcting for datetime issue in sqlite:
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

		else: #user_id is None

			if aggregated:
				if time_window is None:
					time_window = 'month'

				# if db.db_type == 'postgres':
				# 	db.cursor.execute('''
				# 		SELECT COUNT(*),date_trunc(%s, created_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM commits c
				# 		WHERE %s <= created_at AND created_at < %s
				# 		GROUP BY time_stamp
				# 		''',(time_window,time_window,start_date,end_date,))
				# else:
				# 	db.cursor.execute('''
				# 		SELECT COUNT(*),date(datetime(created_at,?),'+1 '||?||'s') AS time_stamp FROM commits c
				# 		WHERE datetime(?) <= created_at AND created_at < datetime(?)
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


			else: # aggregated False: including user info
				if time_window is None:

					# if db.db_type == 'postgres':
					# 	db.cursor.execute('''
					# 		SELECT COUNT(*),author_id FROM commits c
					# 		WHERE %s <= created_at AND created_at < %s
					# 		GROUP BY author_id
					# 		''',(start_date,end_date,))
					# else:
					# 	db.cursor.execute('''
					# 		SELECT COUNT(*),author_id FROM commits c
					# 		WHERE ? <= created_at AND created_at < ?
					# 		GROUP BY author_id
					# 		''',(start_date,end_date,))

					# query_result = list(db.cursor.fetchall())
					query_result = self.query_notimeinfo(db=db,start_date=start_date,end_date=end_date)


					df = pd.DataFrame(data=query_result,columns=(self.measure_name,'user_id'))
					df.set_index(['user_id'],inplace=True)
					df.sort_values(by='user_id',inplace=True)
					return df
				else: #time_window not None

					if time_window is None:
						time_window = 'month'

					# if db.db_type == 'postgres':
					# 	db.cursor.execute('''
					# 		SELECT COUNT(*),date_trunc(%s, created_at) + CONCAT('1 ',%s)::interval  AS time_stamp,author_id FROM commits c
					# 		WHERE %s <= created_at AND created_at < %s
					# 		GROUP BY time_stamp,author_id
					# 		''',(time_window,time_window,start_date,end_date,))
					# else:
					# 	db.cursor.execute('''
					# 		SELECT COUNT(*),date(datetime(created_at,?),'+1 '||?||'s') AS time_stamp,author_id FROM commits c
					# 		WHERE datetime(?) <= created_at AND created_at < datetime(?)
					# 		GROUP BY time_stamp,author_id
					# 		''',('start of {}'.format(time_window),time_window,start_date,end_date,))

					# query_result = list(db.cursor.fetchall())
					# #correcting for datetime issue in sqlite:
					# if db.db_type == 'sqlite':
					# 	query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d'),val_u) for val,val_d,val_u in query_result]

					query_result = self.query_all(db=db,start_date=start_date,end_date=end_date,time_window=time_window)

					df = pd.DataFrame(data=query_result,columns=(self.measure_name,'timestamp','user_id'))

					user_ids = df['user_id'].sort_values().unique().tolist()


					df.set_index(['user_id','timestamp'],inplace=True)
					df.sort_values(by='timestamp',inplace=True)

					start_date_idx = start_date
					end_date_idx = end_date
					if not df.empty:
						# print(df)
						idx = pd.MultiIndex.from_product([user_ids,pd.date_range(start_date_idx,end_date_idx,freq=pandas_freq[time_window])],names=['user_id','timestamp'])
						df = df.reindex(idx,fill_value=0)

						if cumulative:
							# df[self.measure_name] = df.groupby(level=0).cumsum().reset_index()
							df = df.groupby(level=0).cumsum().reset_index()
							df = df[df[self.measure_name]!=0]
							df.set_index(['user_id','timestamp'],inplace=True)

						complete_idx = pd.MultiIndex.from_product([user_ids,pd.date_range(start_date,end_date,freq=pandas_freq[time_window])],names=['user_id','timestamp'])

						df = df.reindex(complete_idx)


					return df


#########################
# commits
#########################
class Commits(UserGetter):
	'''
	Commits per project per time


	When time_window needs to be used, the default value None is replaced by 'month'
	'''
	measure_name = 'commits'

	def query_user(self,db,time_window,start_date,end_date,user_id):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, c.created_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM commits c
				INNER JOIN identities i
				ON c.author_id=i.id AND i.user_id=%s
				AND %s <= c.created_at AND c.created_at < %s
				GROUP BY time_stamp
				''',(time_window,time_window,user_id,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(c.created_at,?),'+1 '||?||'s') AS time_stamp FROM commits c
				INNER JOIN identities i
				ON c.author_id=i.id AND datetime(?) <= c.created_at AND c.created_at < datetime(?)
				AND i.user_id=?
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,user_id,))

		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_aggregated(self,db,time_window,start_date,end_date,user_id=None):
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

	def query_notimeinfo(self,db,start_date,end_date,user_id=None,time_window=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),i.user_id FROM commits c
				INNER JOIN identities i
				ON c.author_id=i.id
				AND %s <= c.created_at AND c.created_at < %s
				GROUP BY i.user_id
				''',(start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),i.user_id FROM commits c
				INNER JOIN identities i
				ON c.author_id=i.id
				AND ? <= created_at AND created_at < ?
				GROUP BY i.user_id
				''',(start_date,end_date,))
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,user_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, c.created_at) + CONCAT('1 ',%s)::interval  AS time_stamp,i.user_id FROM commits c
				INNER JOIN identities i
				ON c.author_id=i.id
				AND %s <= c.created_at AND c.created_at < %s
				GROUP BY time_stamp,i.user_id
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(c.created_at,?),'+1 '||?||'s') AS time_stamp,i.user_id FROM commits c
				INNER JOIN identities i
				ON c.author_id=i.id
				AND datetime(?) <= c.created_at AND c.created_at < datetime(?)
				GROUP BY time_stamp,i.user_id
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d'),val_u) for val,val_d,val_u in query_result]
		return query_result



#########################
# total lines
#########################
class TotalLines(UserGetter):
	'''
	insertions+deletions


	When time_window needs to be used, the default value None is replaced by 'month'
	'''
	measure_name = 'total_lines'

	def query_user(self,db,time_window,start_date,end_date,user_id):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT SUM(c.insertions+c.deletions),date_trunc(%s, c.created_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM commits c
				INNER JOIN identities i
				ON %s <= c.created_at AND c.created_at < %s
				AND c.author_id=i.id AND i.user_id=%s
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,user_id,))
		else:
			db.cursor.execute('''
				SELECT SUM(c.insertions+c.deletions),date(datetime(c.created_at,?),'+1 '||?||'s') AS time_stamp FROM commits c
				INNER JOIN identities i
				ON datetime(?) <= c.created_at AND c.created_at < datetime(?)
				AND c.author_id=i.id AND i.user_id=?
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,user_id,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_aggregated(self,db,time_window,start_date,end_date,user_id=None):
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

	def query_notimeinfo(self,db,start_date,end_date,user_id=None,time_window=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT SUM(c.insertions+c.deletions),i.user_id FROM commits c
				INNER JOIN identities i
				ON c.author_id=i.id
				AND %s <= c.created_at AND c.created_at < %s
				GROUP BY i.user_id
				''',(start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT SUM(c.insertions+c.deletions),i.user_id FROM commits c
				INNER JOIN identities i
				ON c.author_id=i.id
				AND ? <= created_at AND created_at < ?
				GROUP BY i.user_id
				''',(start_date,end_date,))
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,user_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT SUM(c.insertions+c.deletions),date_trunc(%s, c.created_at) + CONCAT('1 ',%s)::interval  AS time_stamp,i.user_id FROM commits c
				INNER JOIN identities i
				ON c.author_id=i.id
				AND %s <= c.created_at AND c.created_at < %s
				GROUP BY time_stamp,i.user_id
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT SUM(c.insertions+c.deletions),date(datetime(c.created_at,?),'+1 '||?||'s') AS time_stamp,i.user_id FROM commits c
				INNER JOIN identities i
				ON c.author_id=i.id
				AND datetime(?) <= c.created_at AND c.created_at < datetime(?)
				GROUP BY time_stamp,i.user_id
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d'),val_u) for val,val_d,val_u in query_result]
		return query_result



#########################
# lines
#########################
class Lines(UserGetter):
	'''
	insertions-deletions


	When time_window needs to be used, the default value None is replaced by 'month'
	'''
	measure_name = 'lines'

	def query_user(self,db,time_window,start_date,end_date,user_id):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT SUM(c.insertions-c.deletions),date_trunc(%s, c.created_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM commits c
				INNER JOIN identities i
				ON %s <= c.created_at AND c.created_at < %s
				AND c.author_id=i.id AND i.user_id=%s
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,user_id,))
		else:
			db.cursor.execute('''
				SELECT SUM(c.insertions-c.deletions),date(datetime(c.created_at,?),'+1 '||?||'s') AS time_stamp FROM commits c
				INNER JOIN identities i
				ON datetime(?) <= c.created_at AND c.created_at < datetime(?)
				AND c.author_id=i.id AND i.user_id=?
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,user_id,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_aggregated(self,db,time_window,start_date,end_date,user_id=None):
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

	def query_notimeinfo(self,db,start_date,end_date,user_id=None,time_window=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT SUM(c.insertions-c.deletions),i.user_id FROM commits c
				INNER JOIN identities i
				ON c.author_id=i.id
				AND %s <= c.created_at AND c.created_at < %s
				GROUP BY i.user_id
				''',(start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT SUM(c.insertions-c.deletions),i.user_id FROM commits c
				INNER JOIN identities i
				ON c.author_id=i.id
				AND ? <= created_at AND created_at < ?
				GROUP BY i.user_id
				''',(start_date,end_date,))
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,user_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT SUM(c.insertions-c.deletions),date_trunc(%s, c.created_at) + CONCAT('1 ',%s)::interval  AS time_stamp,i.user_id FROM commits c
				INNER JOIN identities i
				ON c.author_id=i.id
				AND %s <= c.created_at AND c.created_at < %s
				GROUP BY time_stamp,i.user_id
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT SUM(c.insertions-c.deletions),date(datetime(c.created_at,?),'+1 '||?||'s') AS time_stamp,i.user_id FROM commits c
				INNER JOIN identities i
				ON c.author_id=i.id
				AND datetime(?) <= c.created_at AND c.created_at < datetime(?)
				GROUP BY time_stamp,i.user_id
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d'),val_u) for val,val_d,val_u in query_result]
		return query_result


#########################
# projects
#########################
class Projects(UserGetter):
	'''
	New projects participated to per user per time


	When time_window needs to be used, the default value None is replaced by 'month'
	'''
	measure_name = 'projects'

	def query_user(self,db,time_window,start_date,end_date,user_id):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, created_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND %s <= cc.created_at AND cc.created_at < %s
				AND i.user_id=%s
				GROUP BY i.user_id,cc.repo_id
				) AS c
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,user_id,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(created_at,?),'+1 '||?||'s') AS time_stamp FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND datetime(?) <= cc.created_at AND cc.created_at < datetime(?)
				AND i.user_id=?
				GROUP BY i.user_id,cc.repo_id
				) AS c
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,user_id,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_aggregated(self,db,time_window,start_date,end_date,user_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, created_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND %s <= cc.created_at AND cc.created_at < %s
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
				GROUP BY i.user_id,cc.repo_id
				) AS c
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_notimeinfo(self,db,start_date,end_date,user_id=None,time_window=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),user_id FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON cc.author_id=i.id
				AND %s <= cc.created_at AND cc.created_at < %s
				GROUP BY i.user_id,cc.repo_id
				) AS c
				GROUP BY user_id
				''',(start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),user_id FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON cc.author_id=i.id
				AND datetime(?) <= created_at AND created_at < datetime(?)
				GROUP BY i.user_id,cc.repo_id
				) AS c
				GROUP BY user_id
				''',(start_date,end_date,))
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,user_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, created_at) + CONCAT('1 ',%s)::interval  AS time_stamp,user_id FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND %s <= cc.created_at AND cc.created_at < %s
				GROUP BY i.user_id,cc.repo_id
				) AS c
				GROUP BY time_stamp,user_id
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(created_at,?),'+1 '||?||'s') AS time_stamp,user_id FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND datetime(?) <= cc.created_at AND cc.created_at < datetime(?)
				GROUP BY i.user_id,cc.repo_id
				) AS c
				GROUP BY time_stamp,user_id
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d'),val_u) for val,val_d,val_u in query_result]
		return query_result

#########################
# active projects
#########################
class ActiveProjects(UserGetter):
	'''
	Active projects per user per time


	When time_window needs to be used, the default value None is replaced by 'month'
	'''
	measure_name = 'active_projects'

	def query_user(self,db,time_window,start_date,end_date,user_id):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*), time_stamp FROM
				(SELECT date_trunc(%s, cc.created_at) + CONCAT('1 ',%s)::interval AS time_stamp,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND %s <= cc.created_at AND cc.created_at < %s
				AND i.user_id=%s
				GROUP BY time_stamp,i.user_id,cc.repo_id
				) AS c
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,user_id,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*), time_stamp FROM
				(SELECT date(datetime(cc.created_at,?),'+1 '||?||'s') AS time_stamp,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND datetime(?) <= cc.created_at AND cc.created_at < datetime(?)
				AND i.user_id=?
				GROUP BY time_stamp,i.user_id,cc.repo_id
				) AS c
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,user_id,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_aggregated(self,db,time_window,start_date,end_date,user_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*), time_stamp FROM
				(SELECT date_trunc(%s, created_at) + CONCAT('1 ',%s)::interval AS time_stamp,repo_id FROM commits cc
				WHERE %s <= cc.created_at AND cc.created_at < %s
				GROUP BY time_stamp,repo_id
				) AS c
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*), time_stamp FROM
				(SELECT date(datetime(created_at,?),'+1 '||?||'s') AS time_stamp,repo_id FROM commits cc
				WHERE datetime(?) <= cc.created_at AND cc.created_at < datetime(?)
				GROUP BY time_stamp,repo_id
				) AS c
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_notimeinfo(self,db,start_date,end_date,user_id=None,time_window=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),user_id FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON cc.author_id=i.id
				AND %s <= cc.created_at AND cc.created_at < %s
				GROUP BY i.user_id,cc.repo_id
				) AS c
				GROUP BY user_id
				''',(start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),user_id FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON cc.author_id=i.id
				AND datetime(?) <= created_at AND created_at < datetime(?)
				GROUP BY i.user_id,cc.repo_id
				) AS c
				GROUP BY user_id
				''',(start_date,end_date,))
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,user_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*), time_stamp, user_id FROM
				(SELECT date_trunc(%s, cc.created_at) + CONCAT('1 ',%s)::interval AS time_stamp,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND %s <= cc.created_at AND cc.created_at < %s
				GROUP BY time_stamp,i.user_id,cc.repo_id
				) AS c
				GROUP BY time_stamp,user_id
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*), time_stamp, user_id FROM
				(SELECT date(datetime(cc.created_at,?),'+1 '||?||'s') AS time_stamp,i.user_id,cc.repo_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND datetime(?) <= cc.created_at AND cc.created_at < datetime(?)
				GROUP BY time_stamp,i.user_id,cc.repo_id
				) AS c
				GROUP BY time_stamp,user_id
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d'),val_u) for val,val_d,val_u in query_result]
		return query_result


#########################
# followers
#########################
class Followers(UserGetter):
	'''
	Followers, constant over time (events are not timed)


	When time_window needs to be used, the default value None is replaced by 'month'
	'''
	measure_name = 'followers'

	def query_user(self,db,time_window,start_date,end_date,user_id):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, %s) + CONCAT('1 ',%s)::interval  AS time_stamp FROM followers f
				INNER JOIN identities i
				ON f.followee_id=i.id AND i.user_id=%s
				GROUP BY time_stamp
				''',(time_window,start_date,time_window,user_id))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(?,?),'+1 '||?||'s') AS time_stamp FROM followers f
				INNER JOIN identities i
				ON f.followee_id=i.id AND i.user_id=?
				GROUP BY time_stamp
				''',(start_date,'start of {}'.format(time_window),time_window,user_id))

		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_aggregated(self,db,time_window,start_date,end_date,user_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, %s) + CONCAT('1 ',%s)::interval  AS time_stamp FROM followers f
				GROUP BY time_stamp
				''',(time_window,start_date,time_window,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(?,?),'+1 '||?||'s') AS time_stamp FROM followers f
				GROUP BY time_stamp
				''',(start_date,'start of {}'.format(time_window),time_window))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_notimeinfo(self,db,start_date,end_date,user_id=None,time_window=None):
		db.cursor.execute('''
				SELECT COUNT(*),i.user_id FROM followers f
				INNER JOIN identities i
				ON f.followee_id=i.id
				GROUP BY i.user_id
				''')
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,user_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, %s) + CONCAT('1 ',%s)::interval  AS time_stamp,i.user_id FROM followers f
				INNER JOIN identities i
				ON f.followee_id=i.id
				GROUP BY time_stamp,i.user_id
				''',(time_window,start_date,time_window,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(?,?),'+1 '||?||'s') AS time_stamp,i.user_id FROM followers f
				INNER JOIN identities i
				ON f.followee_id=i.id
				GROUP BY time_stamp,i.user_id
				''',(start_date,'start of {}'.format(time_window),time_window,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d'),val_u) for val,val_d,val_u in query_result]
		return query_result


#########################
# followers_community
#########################
class FollowersCommunity(UserGetter):
	'''
	Followers within the community, constant over time (events are not timed)


	When time_window needs to be used, the default value None is replaced by 'month'
	'''
	measure_name = 'followers_community'

	def query_user(self,db,time_window,start_date,end_date,user_id):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, %s) + CONCAT('1 ',%s)::interval  AS time_stamp FROM followers f
				INNER JOIN identities i
				ON f.followee_id=i.id AND i.user_id=%s
				AND f.follower_id IS NOT NULL
				GROUP BY time_stamp
				''',(time_window,start_date,time_window,user_id))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(?,?),'+1 '||?||'s') AS time_stamp FROM followers f
				INNER JOIN identities i
				ON f.followee_id=i.id AND i.user_id=?
				AND f.follower_id IS NOT NULL
				GROUP BY time_stamp
				''',(start_date,'start of {}'.format(time_window),time_window,user_id))

		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_aggregated(self,db,time_window,start_date,end_date,user_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, %s) + CONCAT('1 ',%s)::interval  AS time_stamp FROM followers f
				WHERE f.follower_id IS NOT NULL
				GROUP BY time_stamp
				''',(time_window,start_date,time_window,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(?,?),'+1 '||?||'s') AS time_stamp FROM followers f
				WHERE f.follower_id IS NOT NULL
				GROUP BY time_stamp
				''',(start_date,'start of {}'.format(time_window),time_window))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_notimeinfo(self,db,start_date,end_date,user_id=None,time_window=None):
		db.cursor.execute('''
				SELECT COUNT(*),i.user_id FROM followers f
				INNER JOIN identities i
				ON f.followee_id=i.id
				AND f.follower_id IS NOT NULL
				GROUP BY i.user_id
				''')
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,user_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, %s) + CONCAT('1 ',%s)::interval  AS time_stamp,i.user_id FROM followers f
				INNER JOIN identities i
				ON f.followee_id=i.id
				AND f.follower_id IS NOT NULL
				GROUP BY time_stamp,i.user_id
				''',(time_window,start_date,time_window,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(?,?),'+1 '||?||'s') AS time_stamp,i.user_id FROM followers f
				INNER JOIN identities i
				ON f.followee_id=i.id
				AND f.follower_id IS NOT NULL
				GROUP BY time_stamp,i.user_id
				''',(start_date,'start of {}'.format(time_window),time_window,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d'),val_u) for val,val_d,val_u in query_result]
		return query_result



#########################
# sponsors
#########################
class Sponsors(UserGetter):
	'''
	Sponsors per project per time

	When time_window needs to be used, the default value None is replaced by 'month'
	'''
	measure_name = 'sponsors'

	def query_user(self,db,time_window,start_date,end_date,user_id):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, s.created_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM sponsors_user s
				INNER JOIN identities i
				ON s.sponsored_id=i.id AND i.user_id=%s
				AND %s <= s.created_at AND s.created_at < %s
				GROUP BY time_stamp
				''',(time_window,time_window,user_id,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(s.created_at,?),'+1 '||?||'s') AS time_stamp FROM sponsors_user s
				INNER JOIN identities i
				ON s.sponsored_id=i.id AND datetime(?) <= s.created_at AND s.created_at < datetime(?)
				AND i.user_id=?
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,user_id,))

		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_aggregated(self,db,time_window,start_date,end_date,user_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, created_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM sponsors_user s
				WHERE %s <= created_at AND created_at < %s
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(created_at,?),'+1 '||?||'s') AS time_stamp FROM sponsors_user s
				WHERE datetime(?) <= created_at AND created_at < datetime(?)
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_notimeinfo(self,db,start_date,end_date,user_id=None,time_window=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),i.user_id FROM sponsors_user s
				INNER JOIN identities i
				ON s.sponsored_id=i.id
				AND %s <= s.created_at AND s.created_at < %s
				GROUP BY i.user_id
				''',(start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),i.user_id FROM sponsors_user s
				INNER JOIN identities i
				ON s.sponsored_id=i.id
				AND ? <= created_at AND created_at < ?
				GROUP BY i.user_id
				''',(start_date,end_date,))
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,user_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, s.created_at) + CONCAT('1 ',%s)::interval  AS time_stamp,i.user_id FROM sponsors_user s
				INNER JOIN identities i
				ON s.sponsored_id=i.id
				AND %s <= s.created_at AND s.created_at < %s
				GROUP BY time_stamp,i.user_id
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(s.created_at,?),'+1 '||?||'s') AS time_stamp,i.user_id FROM sponsors_user s
				INNER JOIN identities i
				ON s.sponsored_id=i.id
				AND datetime(?) <= s.created_at AND s.created_at < datetime(?)
				GROUP BY time_stamp,i.user_id
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d'),val_u) for val,val_d,val_u in query_result]
		return query_result

#########################
# sponsors_community
#########################
class SponsorsCommunity(UserGetter):
	'''
	Sponsors per project per time, when sponsor is in the DB

	When time_window needs to be used, the default value None is replaced by 'month'
	'''
	measure_name = 'sponsors_community'

	def query_user(self,db,time_window,start_date,end_date,user_id):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, s.created_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM sponsors_user s
				INNER JOIN identities i
				ON s.sponsored_id=i.id AND i.user_id=%s
				AND %s <= s.created_at AND s.created_at < %s
				AND s.sponsor_id IS NOT NULL
				GROUP BY time_stamp
				''',(time_window,time_window,user_id,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(s.created_at,?),'+1 '||?||'s') AS time_stamp FROM sponsors_user s
				INNER JOIN identities i
				ON s.sponsored_id=i.id AND datetime(?) <= s.created_at AND s.created_at < datetime(?)
				AND i.user_id=?
				AND s.sponsor_id IS NOT NULL
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,user_id,))

		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_aggregated(self,db,time_window,start_date,end_date,user_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, created_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM sponsors_user s
				WHERE %s <= created_at AND created_at < %s
				AND s.sponsor_id IS NOT NULL
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(created_at,?),'+1 '||?||'s') AS time_stamp FROM sponsors_user s
				WHERE datetime(?) <= created_at AND created_at < datetime(?)
				AND s.sponsor_id IS NOT NULL
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_notimeinfo(self,db,start_date,end_date,user_id=None,time_window=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),i.user_id FROM sponsors_user s
				INNER JOIN identities i
				ON s.sponsored_id=i.id
				AND %s <= s.created_at AND s.created_at < %s
				AND s.sponsor_id IS NOT NULL
				GROUP BY i.user_id
				''',(start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),i.user_id FROM sponsors_user s
				INNER JOIN identities i
				ON s.sponsored_id=i.id
				AND ? <= created_at AND created_at < ?
				AND s.sponsor_id IS NOT NULL
				GROUP BY i.user_id
				''',(start_date,end_date,))
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,user_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, s.created_at) + CONCAT('1 ',%s)::interval  AS time_stamp,i.user_id FROM sponsors_user s
				INNER JOIN identities i
				ON s.sponsored_id=i.id
				AND %s <= s.created_at AND s.created_at < %s
				AND s.sponsor_id IS NOT NULL
				GROUP BY time_stamp,i.user_id
				''',(time_window,time_window,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(s.created_at,?),'+1 '||?||'s') AS time_stamp,i.user_id FROM sponsors_user s
				INNER JOIN identities i
				ON s.sponsored_id=i.id
				AND datetime(?) <= s.created_at AND s.created_at < datetime(?)
				AND s.sponsor_id IS NOT NULL
				GROUP BY time_stamp,i.user_id
				''',('start of {}'.format(time_window),time_window,start_date,end_date,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d'),val_u) for val,val_d,val_u in query_result]
		return query_result


#########################
# coworkers
#########################
class CoWorkers(UserGetter):
	'''
	Coworkers per time (when not cumulative; new coworkers)

	When time_window needs to be used, the default value None is replaced by 'month'
	'''
	measure_name = 'coworkers'

	def query_user(self,db,time_window,start_date,end_date,user_id):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, created_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,icw.user_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND %s <= cc.created_at AND cc.created_at < %s
				AND i.user_id=%s
				INNER JOIN commits cccw
				ON %s <= cc.created_at AND cc.created_at < %s
				AND cc.repo_id=cccw.repo_id
				INNER JOIN identities icw
				ON cccw.author_id=icw.id
				AND icw.user_id!=i.user_id
				GROUP BY i.user_id,icw.user_id
				) AS c
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,user_id,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(created_at,?),'+1 '||?||'s') AS time_stamp FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,icw.user_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND ? <= cc.created_at AND cc.created_at < ?
				AND i.user_id=?
				INNER JOIN commits cccw
				ON ? <= cc.created_at AND cc.created_at < ?
				AND cc.repo_id=cccw.repo_id
				INNER JOIN identities icw
				ON cccw.author_id=icw.id
				AND icw.user_id!=i.user_id
				GROUP BY i.user_id,icw.user_id
				) AS c
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,user_id,start_date,end_date,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_aggregated(self,db,time_window,start_date,end_date,user_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, created_at) + CONCAT('1 ',%s)::interval  AS time_stamp FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,icw.user_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND %s <= cc.created_at AND cc.created_at < %s
				INNER JOIN commits cccw
				ON %s <= cc.created_at AND cc.created_at < %s
				AND cc.repo_id=cccw.repo_id
				INNER JOIN identities icw
				ON cccw.author_id=icw.id
				AND icw.user_id>i.user_id
				GROUP BY i.user_id,icw.user_id
				) AS c
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,start_date,end_date))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(created_at,?),'+1 '||?||'s') AS time_stamp FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id,icw.user_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND ? <= cc.created_at AND cc.created_at < ?
				INNER JOIN commits cccw
				ON ? <= cc.created_at AND cc.created_at < ?
				AND cc.repo_id=cccw.repo_id
				INNER JOIN identities icw
				ON cccw.author_id=icw.id
				AND icw.user_id>i.user_id
				GROUP BY i.user_id,icw.user_id
				) AS c
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,start_date,end_date))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_notimeinfo(self,db,start_date,end_date,user_id=None,time_window=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),main_user_id FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id AS main_user_id,icw.user_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND %s <= cc.created_at AND cc.created_at < %s
				INNER JOIN commits cccw
				ON %s <= cc.created_at AND cc.created_at < %s
				AND cc.repo_id=cccw.repo_id
				INNER JOIN identities icw
				ON cccw.author_id=icw.id
				AND icw.user_id!=i.user_id
				GROUP BY i.user_id,icw.user_id
				) AS c
				GROUP BY main_user_id
				''',(start_date,end_date,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),main_user_id FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id AS main_user_id,icw.user_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND ? <= cc.created_at AND cc.created_at < ?
				INNER JOIN commits cccw
				ON ? <= cc.created_at AND cc.created_at < ?
				AND cc.repo_id=cccw.repo_id
				INNER JOIN identities icw
				ON cccw.author_id=icw.id
				AND icw.user_id!=i.user_id
				GROUP BY i.user_id,icw.user_id
				) AS c
				GROUP BY main_user_id
				''',(start_date,end_date,start_date,end_date,))
		return list(db.cursor.fetchall())

	def query_all(self,db,start_date,end_date,time_window,user_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),date_trunc(%s, created_at) + CONCAT('1 ',%s)::interval  AS time_stamp,main_user_id FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id AS main_user_id,icw.user_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND %s <= cc.created_at AND cc.created_at < %s
				INNER JOIN commits cccw
				ON %s <= cc.created_at AND cc.created_at < %s
				AND cc.repo_id=cccw.repo_id
				INNER JOIN identities icw
				ON cccw.author_id=icw.id
				AND icw.user_id!=i.user_id
				GROUP BY i.user_id,icw.user_id
				) AS c
				GROUP BY time_stamp,main_user_id
				''',(time_window,time_window,start_date,end_date,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),date(datetime(created_at,?),'+1 '||?||'s') AS time_stamp,main_user_id FROM
				(SELECT MIN(cc.created_at) AS created_at,i.user_id AS main_user_id,icw.user_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND ? <= cc.created_at AND cc.created_at < ?
				INNER JOIN commits cccw
				ON ? <= cc.created_at AND cc.created_at < ?
				AND cc.repo_id=cccw.repo_id
				INNER JOIN identities icw
				ON cccw.author_id=icw.id
				AND icw.user_id!=i.user_id
				GROUP BY i.user_id,icw.user_id
				) AS c
				GROUP BY time_stamp,main_user_id
				''',('start of {}'.format(time_window),time_window,start_date,end_date,start_date,end_date,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d'),val_u) for val,val_d,val_u in query_result]
		return query_result
#########################
# active_coworkers
#########################
class ActiveCoWorkers(CoWorkers):
	'''
	Active coworkers per time

	When time_window needs to be used, the default value None is replaced by 'month'
	'''
	measure_name = 'active_coworkers'

	def query_user(self,db,time_window,start_date,end_date,user_id):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),time_stamp FROM
				(SELECT date_trunc(%s, cc.created_at) + CONCAT('1 ',%s)::interval  AS time_stamp,i.user_id,icw.user_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND %s <= cc.created_at AND cc.created_at < %s
				AND i.user_id=%s
				INNER JOIN commits cccw
				ON %s <= cc.created_at AND cc.created_at < %s
				AND cc.repo_id=cccw.repo_id
				INNER JOIN identities icw
				ON cccw.author_id=icw.id
				AND icw.user_id!=i.user_id
				GROUP BY time_stamp,i.user_id,icw.user_id
				) AS c
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,user_id,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),time_stamp FROM
				(SELECT date(datetime(cc.created_at,?),'+1 '||?||'s') AS time_stamp,i.user_id,icw.user_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND ? <= cc.created_at AND cc.created_at < ?
				AND i.user_id=?
				INNER JOIN commits cccw
				ON ? <= cc.created_at AND cc.created_at < ?
				AND cc.repo_id=cccw.repo_id
				INNER JOIN identities icw
				ON cccw.author_id=icw.id
				AND icw.user_id!=i.user_id
				GROUP BY time_stamp,i.user_id,icw.user_id
				) AS c
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,user_id,start_date,end_date,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_aggregated(self,db,time_window,start_date,end_date,user_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),time_stamp FROM
				(SELECT date_trunc(%s, cc.created_at) + CONCAT('1 ',%s)::interval  AS time_stamp,i.user_id,icw.user_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND %s <= cc.created_at AND cc.created_at < %s
				INNER JOIN commits cccw
				ON %s <= cc.created_at AND cc.created_at < %s
				AND cc.repo_id=cccw.repo_id
				INNER JOIN identities icw
				ON cccw.author_id=icw.id
				AND icw.user_id>i.user_id
				GROUP BY time_stamp,i.user_id,icw.user_id
				) AS c
				GROUP BY time_stamp
				''',(time_window,time_window,start_date,end_date,start_date,end_date))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),time_stamp FROM
				(SELECT date(datetime(cc.created_at,?),'+1 '||?||'s') AS time_stamp,i.user_id,icw.user_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND ? <= cc.created_at AND cc.created_at < ?
				INNER JOIN commits cccw
				ON ? <= cc.created_at AND cc.created_at < ?
				AND cc.repo_id=cccw.repo_id
				INNER JOIN identities icw
				ON cccw.author_id=icw.id
				AND icw.user_id>i.user_id
				GROUP BY time_stamp,i.user_id,icw.user_id
				) AS c
				GROUP BY time_stamp
				''',('start of {}'.format(time_window),time_window,start_date,end_date,start_date,end_date))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d')) for val,val_d in query_result]
		return query_result

	def query_all(self,db,start_date,end_date,time_window,user_id=None):
		if db.db_type == 'postgres':
			db.cursor.execute('''
				SELECT COUNT(*),time_stamp,main_user_id FROM
				(SELECT date_trunc(%s, cc.created_at) + CONCAT('1 ',%s)::interval  AS time_stamp,i.user_id AS main_user_id,icw.user_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND %s <= cc.created_at AND cc.created_at < %s
				INNER JOIN commits cccw
				ON %s <= cc.created_at AND cc.created_at < %s
				AND cc.repo_id=cccw.repo_id
				INNER JOIN identities icw
				ON cccw.author_id=icw.id
				AND icw.user_id!=i.user_id
				GROUP BY time_stamp,i.user_id,icw.user_id
				) AS c
				GROUP BY time_stamp,main_user_id
				''',(time_window,time_window,start_date,end_date,start_date,end_date,))
		else:
			db.cursor.execute('''
				SELECT COUNT(*),time_stamp,main_user_id FROM
				(SELECT date(datetime(cc.created_at,?),'+1 '||?||'s') AS time_stamp,i.user_id AS main_user_id,icw.user_id FROM commits cc
				INNER JOIN identities i
				ON i.id=cc.author_id
				AND ? <= cc.created_at AND cc.created_at < ?
				INNER JOIN commits cccw
				ON ? <= cc.created_at AND cc.created_at < ?
				AND cc.repo_id=cccw.repo_id
				INNER JOIN identities icw
				ON cccw.author_id=icw.id
				AND icw.user_id!=i.user_id
				GROUP BY time_stamp,i.user_id,icw.user_id
				) AS c
				GROUP BY time_stamp,main_user_id
				''',('start of {}'.format(time_window),time_window,start_date,end_date,start_date,end_date,))
		query_result = list(db.cursor.fetchall())
		#correcting for datetime issue in sqlite:
		if db.db_type == 'sqlite':
			query_result = [(val,datetime.datetime.strptime(val_d,'%Y-%m-%d'),val_u) for val,val_d,val_u in query_result]
		return query_result
