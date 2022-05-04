from . import Getter
from ..extras import check_sqlname_safe
import datetime
import numpy as np
import pandas as pd
import copy
import json

class BotChecks(Getter):
	queries = ['''
select i.user_id from commits c
inner join identities i 
on i.id=c.author_id and not i.is_bot 
group by i.user_id
order by count(*) desc
limit 100
				;''',
				'''
(select i.user_id from commits c
inner join identities i 
on i.id=c.author_id and not i.is_bot and c.created_at <='2022-01-01' and c.created_at >='2020-12-31'
group by i.user_id
order by count(*) desc
limit 100)
except
(select i.user_id from commits c
inner join identities i 
on i.id=c.author_id and not i.is_bot 
group by i.user_id
order by count(*) desc
limit 100)
				;''',
				'''
select i.user_id,max(i."identity" ), count(*) from commits c
inner join identities i 
on i.id=c.author_id and not i.is_bot and i.identity not like '%@%'
group by i.user_id
order by count(*) desc
				;''',
				'''
select i.user_id from commits c
inner join identities i 
on i.id=c.author_id and not i.is_bot and i.identity like '%bot%'
group by i.user_id
order by count(*) desc
				;''',]

				


	def process_list(self):
		if self.db.db_type == 'postgres':
			self.db.cursor.execute('''
				WITH commit_counts AS (SELECT COUNT(*) AS cnt,i.user_id FROM commits c
							INNER JOIN identities i
							ON i.id=c.author_id AND i.user_id IN %(user_list)s
							GROUP BY i.user_id),
					main_identity AS (SELECT DISTINCT ON (user_id) user_id,user_login FROM
						(SELECT u.id AS user_id,CONCAT(it.name,'/',i.identity) AS user_login
							FROM users u
							INNER JOIN identities i
								ON i.user_id=u.id
								AND i.user_id IN %(user_list)s
							INNER JOIN identity_types it
								ON i.identity_type_id=it.id
							ORDER BY user_id,(CASE it.name
									WHEN 'github_login' THEN -1
									WHEN 'gitlab_login' THEN 0
									WHEN 'email' THEN NULL
									ELSE it.id END) NULLS LAST) AS subquery
							ORDER BY user_id)
				SELECT mi.user_id,mi.user_login,cc.cnt
					FROM main_identity mi
					INNER JOIN commit_counts cc
					ON cc.user_id=mi.user_id
					ORDER BY cc.cnt DESC
				;''',{'user_list':tuple(self.user_list)})


		else:
			for uid in self.user_list:
				check_sqlname_safe(str(uid))
			self.db.cursor.execute('''
				WITH commit_counts AS (SELECT COUNT(*) AS cnt,i.user_id FROM commits c
							INNER JOIN identities i
							ON i.id=c.author_id AND i.user_id IN {user_list_str}
							GROUP BY i.user_id),
					main_identity AS (SELECT DISTINCT ON (user_id) user_id,user_login FROM
						(SELECT u.id,it.name||'/'||i.identity AS user_login FROM users u
							JOIN identities i ON i.id IN (
 								SELECT i2.id FROM identities i2
									INNER JOIN identity_types it2
										ON it2.id=i2.identity_type_id
										AND i2.user_id=u.id
										AND i2.user_id IN {user_list_str}
									ORDER BY (CASE it2.name
											WHEN 'github_login' THEN -1
											WHEN 'gitlab_login' THEN 0
											WHEN 'email' THEN NULL
											ELSE it2.id END) NULLS LAST,
										i2.id
									LIMIT 1)
							INNER JOIN identity_types it
							ON i.identity_type_id=it.id)
				SELECT mi.user_id,mi.user_login,cc.cnt
					FROM main_identity mi
					INNER JOIN commit_counts cc
					ON cc.user_id=mi.user_id
					ORDER BY cc.cnt DESC
				;'''.format(user_list_str='({})'.format(','.join([str(uid) for uid in self.user_list]))))


		self.results = list(self.db.cursor.fetchall())


	def get(self,db,raw_result=False,**kwargs):
		self.get_list()
		self.user_list = list(set(self.user_list))
		self.process_list()
		query_result = self.results
		if raw_result:
			return query_result
		else:
			df = pd.DataFrame(self.parse_results(query_result=query_result))
			return df

	def parse_results(self,query_result):
		return [{'commits':c_cnt,'user_id':uid,'login':ulogin} for uid,ulogin,c_cnt in query_result]

	def get_list(self):
		self.user_list = []
		for q in self.queries:
			self.db.cursor.execute(q)
			for r in self.db.cursor.fetchall():
				self.user_list.append(r[0])