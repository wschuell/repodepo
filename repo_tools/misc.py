import os
import datetime

def get_packages_from_crates(conn,limit=None):
	'''
	DEPRECATED! Integrated in fillers
	From a connection to a crates.io database, output the list of packages as expected by RepoCrawler.add_packages()
	package id, package name, created_at (datetime.datetime),repo_url
	'''

	cursor = conn.cursor()


	if limit is not None:
		if not isinstance(limit,int):
			raise ValueError('limit should be an integer, given {}'.format(limit))
		else:
			limit_str = ' LIMIT {}'.format(limit)
	else:
		limit_str = ''


	cursor.execute('''
		SELECT id,name,created_at,repository FROM crates {}
		;'''.format(limit_str))



	return cursor.fetchall()
