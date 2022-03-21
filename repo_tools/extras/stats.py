import psycopg2
from collections import OrderedDict
import oyaml as yaml

class Stats(object):
	'''
	abstract class to be inherited from
	'''

	def get_results(self):
		'''
		sets self.results as an ordered dict, potentially nested
		'''
		raise NotImplementedError

	def format_results(self):
		'''
		formats results in yml format
		'''
		if not hasattr(self,'results'):
			self.get_results()
		return yaml.dump(self.results)

	def print_results(self):
		'''
		prints results in yml format
		'''
		print(self.format_results())

	def save(self,filepath):
		'''
		saves output from format_resuts in a file
		'''
		with open(filepath,'w') as f:
			f.write(self.format_results())


class DBStats(Stats):
	'''
	abstract class for DB stats
	'''
	def __init__(self,db,**kwargs):
		self.db = db
		Stats.__init__(self,**kwargs)

class PackageStats(DBStats):
	'''
	packages stats
	'''
	def get_results(self):
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

		self.results = results


	def get_nb_packages(self):
		self.db.cursor.execute('SELECT COUNT(*) FROM packages;')
		return self.db.cursor.fetchone()[0]

class GlobalStats(DBStats):
	def get_results(self):
		results = OrderedDict()

		for (name,cl) in [
				('packages',PackageStats),
				]:

			s = cl(db=self.db)
			s.get_results()
			results[name] = s.results

		self.results = results

