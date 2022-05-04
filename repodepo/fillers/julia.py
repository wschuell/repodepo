import datetime
import os
import psycopg2
import json

from .. import fillers
from ..fillers import generic

class JuliaHubFiller(generic.PackageFiller):
	"""
	wrapper around generic.PackageFiller for data from https://juliahub.com/app/packages/info
	"""

	def __init__(self,
			source='juliahub',
			source_urlroot=None,
			package_limit=None,
			force=False,
			file_url='https://juliahub.com/app/packages/info',
					**kwargs):
		self.source = source
		self.source_urlroot = source_urlroot
		self.package_limit = package_limit
		self.force = force
		self.file_url = file_url
		fillers.Filler.__init__(self,**kwargs)

	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder
		data_folder = self.data_folder

		#create folder if needed
		if not os.path.exists(data_folder):
			os.makedirs(data_folder)

		self.db.register_source(source=self.source,source_urlroot=self.source_urlroot)
		if self.source_urlroot is None:
			self.source_url_root = self.db.get_source_info(source=self.source)[1]

		self.download(url=self.file_url,destination=os.path.join(self.data_folder,'juliahub_packages.json'))
		with open(os.path.join(self.data_folder,'juliahub_packages.json'),'r') as f:
			packages_json = json.load(f)
		self.package_list = [(i,p['name'],None,p['metadata']['repo']) for i,p in enumerate(packages_json['packages'])]
		if not self.force:
			if self.db.db_type == 'postgres':
				self.db.cursor.execute('''SELECT COUNT(*) FROM packages p
								INNER JOIN sources s
								ON p.source_id=s.id
								AND s.name=%s
								; ''',(self.source,))
			else:
				self.db.cursor.execute('''SELECT COUNT(*) FROM packages p
								INNER JOIN sources s
								ON p.source_id=s.id
								AND s.name=?
								; ''',(self.source,))
			ans = self.db.cursor.fetchone()
			if ans is not None:
				pk_cnt = ans[0]
				if len(self.package_list) == pk_cnt:
					self.package_list = []

		self.db.connection.commit()


	def apply(self):
		self.fill_packages()
		self.db.connection.commit()
