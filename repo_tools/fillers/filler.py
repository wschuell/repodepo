import os
import requests
import zipfile
import pandas as pd
import logging
import csv
from psycopg2 import extras
import json
import subprocess
import gzip
import shutil

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.INFO)

class Filler(object):
	"""
	The Filler class and its children provide methods to fill the database, potentially from different sources.
	They can be linked to the database either by mentioning the database at creation of the instance (see __init__),
	or by calling the 'add_filler' method of the database. Caution, the order may be important, e.g. when foreign keys are involved.

	For writing children, just change the 'apply' method, and do not forget the commit at the end.
	This class is just an abstract 'mother' class
	"""

	def __init__(self,db=None,name=None,data_folder=None,unique_name=False):#,file_info=None):
		self.done = False
		if name is None:
			name = self.__class__.__name__
		self.name = name
		if db is not None:
			db.add_filler(self)
		self.data_folder = data_folder
		self.logger = logging.getLogger('fillers.'+self.__class__.__name__)
		self.logger.addHandler(ch)
		self.logger.setLevel(logging.INFO)
		self.unique_name = unique_name

		# if file_info is not None:
		# 	self.set_file_info(file_info)


	# def set_file_info(self,file_info): # deprecated, files are managed at filler level only
	# 	"""set_file_info should add the filename in self.db.fillers_shareddata['files'][filecode] = filename
	# 	while checking that the filecode is not present already in the relevant dict"""
	# 	raise NotImplementedError

	def apply(self):
		# filling script here
		self.db.connection.commit()

	def prepare(self,**kwargs):
		'''
		Method called to potentially do necessary preprocessings (downloading files, uncompressing, converting, ...)
		'''

		if self.data_folder is None:
			self.data_folder = self.db.data_folder
		data_folder = self.data_folder

		#create folder if needed
		if not os.path.exists(data_folder):
			os.makedirs(data_folder)
		pass





	def download(self,url,destination,wget=False,force=False):
		if not force and os.path.exists(destination):
			self.logger.info('Skipping download {}'.format(url))
		else:
			self.logger.info('Downloading {}'.format(url))
			if not wget:
				r = requests.get(url, allow_redirects=True)
				with open(destination, 'wb') as f:
					f.write(r.content)
			else:
				subprocess.check_call('wget -O {} {}'.format(destination,url).split(' '))

	def unzip(self,orig_file,destination,clean_zip=False):
		self.logger.info('Unzipping {}'.format(orig_file))
		with zipfile.ZipFile(orig_file, 'r') as zip_ref:
			zip_ref.extractall(destination)
		if clean_zip:
			os.remove(orig_file)

	def convert_xlsx(self,orig_file,destination,clean_xlsx=False):
		self.logger.info('Converting {} to CSV'.format(orig_file))
		data_xls = pd.read_excel(orig_file, index_col=None, engine='openpyxl')
		data_xls.to_csv(destination,index=False,header=None ,encoding='utf-8')
		if clean_xlsx:
			os.remove(orig_file)

	def record_file(self,**kwargs):
		'''
		Wrapper to solve data_folder mismatch with DB
		'''
		self.db.record_file(folder=self.data_folder,**kwargs)

	def ungzip(self,orig_file,destination,clean_zip=False):
		self.logger.info('UnGzipping {}'.format(orig_file))
		with gzip.open(orig_file, 'rb') as f_in:
			with open(destination, 'wb') as f_out:
				shutil.copyfileobj(f_in, f_out)
		if clean_zip:
			os.remove(orig_file)
