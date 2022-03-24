import psycopg2
from collections import OrderedDict
import oyaml as yaml
from ..getters import generic_getters

class Stats(generic_getters.Getter):
	'''
	abstract class to be inherited from
	'''

	def format_result(self):
		'''
		formats results in yml format
		'''
		if not hasattr(self,'results'):
			self.get_result()
		return yaml.dump(self.results)

	def print_result(self):
		'''
		prints results in yml format
		'''
		print(self.format_result())

	def save(self,filepath):
		'''
		saves output from format_resuts in a file
		'''
		with open(filepath,'w') as f:
			f.write(self.format_result())

	def get_result(self,db=None,force=False,**kwargs):
		'''
		sets self.results as an ordered dict, potentially nested, by calling .get() if necessary
		'''
		if not hasattr(self,'results') or force:
			self.results = self.get(db=db,**kwargs)
		return self.results

	def get(self,db,**kwargs):
		'''
		sets self.results as an ordered dict, potentially nested
		'''
		raise NotImplementedError

class DBStats(Stats):
	'''
	abstract class for DB stats
	'''
	def __init__(self,db,**kwargs):
		self.db = db
		Stats.__init__(self,**kwargs)

	def get_result(self,db=None,**kwargs):
		'''
		sets self.results as an ordered dict, potentially nested, by calling .get() if necessary
		'''
		if db is None:
			db = self.db
		else:
			raise NotImplementedError('For DBStats and children classes, call to another DB is not implemented/passed down to .get()')
		if db is None:
			raise ValueError('please set a database to query from')
		Stats.get_result(self,db=db,**kwargs)

class PackageStats(DBStats):
	'''
	packages stats
	'''
	def get(self,db,**kwargs):
		results = OrderedDict()
		results['nb_total'] = self.get_nb_packages()
		# results['nb_url_repo'] = self.get_nb_url_repo()
		# results['nb_url_homepage'] = self.get_nb_url_homepage()
		# results['nb_url_doc'] = self.get_nb_url_doc()
		# results['nb_url'] = self.get_nb_url()
		# results['nb_url_parsed'] = self.get_nb_url_parsed() # total, gh,gl
		# results['nb_unique_url_parsed'] = self.get_nb_unique_url_parsed() # total, gh,gl
		# results['nb_url_cloneable'] = self.get_nb_url_cloneable() # total, gh,gl
		# results['nb_unique_url_cloneable'] = self.get_nb_unique_url_cloneable() # total, gh,gl

		return results


	def get_nb_packages(self):
		self.db.cursor.execute('SELECT COUNT(*) FROM packages;')
		return self.db.cursor.fetchone()[0]

		# results['nb_url_repo'] = self.get_nb_url_repo()
		# results['nb_url_homepage'] = self.get_nb_url_homepage()
		# results['nb_url_doc'] = self.get_nb_url_doc()
		# results['nb_url'] = self.get_nb_url()
		# results['nb_url_parsed'] = self.get_nb_url_parsed() # total, gh,gl
		# results['nb_unique_url_parsed'] = self.get_nb_unique_url_parsed() # total, gh,gl
		# results['nb_url_cloneable'] = self.get_nb_url_cloneable() # total, gh,gl
		# results['nb_unique_url_cloneable'] = self.get_nb_unique_url_cloneable() # total, gh,gl

class GlobalStats(DBStats):
	def get(self,db,**kwargs):
		results = OrderedDict()

		for (name,cl) in [
				('packages',PackageStats),
				]:

			s = cl(db=db)
			s.get_result()
			results[name] = s.results

		return results

