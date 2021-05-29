import os
import requests
import zipfile
import pandas as pd
import logging
import csv
from psycopg2 import extras
import json
import subprocess

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.INFO)


class Getter(object):
	"""
	The Getter class and its children provide methods to extract data from the database, e.g. in the form of DataFrames.
	They can be linked to the database either by mentioning the database at creation of the instance (see __init__),
	or by using the 'db' argument of the get_result() method.

	For writing children, just change the 'get' method, and do not forget the commit at the end.
	This class is just an abstract 'mother' class
	"""

	def __init__(self,db=None,name=None,data_folder=None):#,file_info=None):
		if name is None:
			name = self.__class__.__name__
		self.db = db
		self.name = name
		self.data_folder = data_folder
		self.logger = logging.getLogger('{}.{}'.format(__name__,self.__class__.__name__))
		self.logger.addHandler(ch)
		self.logger.setLevel(logging.INFO)

	def get_result(self,db=None,**kwargs):
		if db is None:
			db = self.db
		if db is None:
			raise ValueError('please set a database to query from')
		return self.get(db=db,**kwargs)

	def get(self,db,raw_result=False,**kwargs):
		db.cursor.execute(self.query(),self.query_attributes())
		query_result = list(db.cursor.fetchall())
		if raw_result:
			return query_result
		else:
			df = pd.DataFrame(self.parse_results(query_result=query_result))
			return df

	def query(self):
		'''
		query string with %(variablename)s convention
 		'''
		raise NotImplementedError


	def query_attributes(self):
		'''
		returns dict to be used as var dict for the query
		'''
		raise NotImplementedError

	def parse_results(self,query_result):
		'''
		returns list of elements to be used for pandas or geopandas
		'''
		raise NotImplementedError
