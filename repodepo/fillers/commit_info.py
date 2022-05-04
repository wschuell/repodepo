import datetime
import os
import psycopg2
from psycopg2 import extras
import pygit2
import json
import git
import itertools
import psutil

from .. import fillers
from ..fillers import generic

import multiprocessing as mp

class CommitsFiller(fillers.Filler):
	"""
	global commit parser
	"""


	def __init__(self,
			only_null_commit_origs=True,
			all_commits=False,
			force=False,
			allbranches=True,
			created_at_batchsize=1000,
			fix_created_at=False,
			workers=1, # It does not seem that more workers speed up the process, IO seems to be the bottleneck.
					**kwargs):
		self.force = force
		self.allbranches = allbranches
		self.only_null_commit_origs = only_null_commit_origs
		self.all_commits = all_commits
		self.created_at_batchsize = created_at_batchsize
		self.fix_created_at = fix_created_at
		if workers is None:
			self.workers = len(psutil.Process().cpu_affinity())
		else:
			self.workers = workers
		fillers.Filler.__init__(self,**kwargs)

	def prepare(self):
		if self.data_folder is None:
			self.data_folder = self.db.data_folder
		data_folder = self.data_folder

		#create folder if needed
		if not os.path.exists(data_folder):
			os.makedirs(data_folder)

		pass

	def apply(self):
		self.fill_commit_info(force=self.force,all_commits=self.all_commits)
		self.fill_commit_orig_repo(only_null=self.only_null_commit_origs)
		if self.fix_created_at:
			self.fill_commit_created_at(batch_size=self.created_at_batchsize)
		self.db.connection.commit()


	def get_repo_list(self,all_commits=False,option=None):

		if all_commits:
			self.db.cursor.execute('''
				SELECT s.name,r.owner,r.name,r.id
				FROM repositories r
				INNER JOIN sources s
				ON s.id=r.source AND r.cloned
				ORDER BY s.name,r.owner,r.name
				;''')
			return [{'source':r[0],'owner':r[1],'name':r[2],'repo_id':r[3]} for r in self.db.cursor.fetchall()]

		else:
			if option is not None:
				# listing all repository being cloned, and not having any successful <option> table update
				if self.db.db_type == 'postgres':
					self.db.cursor.execute('''
						SELECT s.name AS sname,r.owner AS rowner,r.name AS rname,r.id,extract(epoch from r.latest_commit_time)
						FROM repositories r
						INNER JOIN sources s
						ON s.id=r.source AND r.cloned
						EXCEPT
						(SELECT s.name AS sname,r.owner AS rowner,r.name AS rname,r.id,extract(epoch from r.latest_commit_time)
						FROM repositories r
						INNER JOIN sources s
						ON s.id=r.source AND r.cloned
						INNER JOIN table_updates tu
						ON tu.table_name=%s AND tu.repo_id=r.id AND tu.success)
						ORDER BY sname,rowner,rname
						;''',(option,))
				else:
					self.db.cursor.execute('''
						SELECT s.name AS sname,r.owner AS rowner,r.name AS rname,r.id,CAST(strftime('%s', r.latest_commit_time) AS INTEGER)
						FROM repositories r
						INNER JOIN sources s
						ON s.id=r.source AND r.cloned
						EXCEPT
						SELECT s.name AS sname,r.owner AS rowner,r.name AS rname,r.id,CAST(strftime('%s', r.latest_commit_time) AS INTEGER)
						FROM repositories r
						INNER JOIN sources s
						ON s.id=r.source AND r.cloned
						INNER JOIN table_updates tu
						ON tu.table_name=? AND tu.repo_id=r.id AND tu.success
						ORDER BY sname,rowner,rname
						;''',(option,))
			else:
				if self.db.db_type == 'postgres':
					self.db.cursor.execute('''
						SELECT s.name,r.owner,r.name,r.id,extract(epoch from r.latest_commit_time)
						FROM repositories r
						INNER JOIN sources s
						ON s.id=r.source AND r.cloned
						ORDER BY s.name,r.owner,r.name
						;''')
				else:
					self.db.cursor.execute('''
						SELECT s.name,r.owner,r.name,r.id,CAST(strftime('%s', r.latest_commit_time) AS INTEGER)
						FROM repositories r
						INNER JOIN sources s
						ON s.id=r.source AND r.cloned
						ORDER BY s.name,r.owner,r.name
						;''')

			return [{'source':r[0],'owner':r[1],'name':r[2],'repo_id':r[3],'after_time':r[4]} for r in self.db.cursor.fetchall()]

	def fill_commit_info(self,force=False,all_commits=False):
		'''
		Filling in authors, commits and parenthood using Database object methods
		'''

		self.db.cursor.execute('''SELECT MAX(updated_at) FROM full_updates WHERE update_type='commits';''')
		last_fu = self.db.cursor.fetchone()[0]

		self.db.cursor.execute('''SELECT MAX(updated_at) FROM table_updates WHERE table_name='clones' AND success;''')
		last_dl = self.db.cursor.fetchone()[0]

		if force or (last_fu is None) or (last_dl is not None and last_fu<last_dl):

			self.logger.info('Filling in identities')

			for repo_info in self.get_repo_list(all_commits=all_commits,option='identities'):
				self.logger.info('Filling identities info for {source}/{owner}/{name}'.format(**repo_info))
				try:
					self.fill_authors(self.list_commits(group_by='authors',basic_info_only=True,allbranches=self.allbranches,**repo_info),repo_id=repo_info['repo_id'])
				except:
					self.logger.error('Error with {}'.format(repo_info))
					raise
			self.logger.info('Filling in commits')

			for repo_info in self.get_repo_list(all_commits=all_commits,option='commits'):
				self.logger.info('Filling commits info for {source}/{owner}/{name}'.format(**repo_info))
				try:
					self.fill_commits(self.list_commits(basic_info_only=False,allbranches=self.allbranches,**repo_info),repo_id=repo_info['repo_id'])
				except:
					self.logger.error('Error with {}'.format(repo_info))
					raise


			self.logger.info('Filling in repository commit ownership')

			for repo_info in self.get_repo_list(all_commits=all_commits,option='commit_repos'):
				self.logger.info('Filling commit ownership info for {source}/{owner}/{name}'.format(**repo_info))
				try:
					self.fill_commit_repos(self.list_commits(basic_info_only=True,allbranches=self.allbranches,**repo_info),repo_id=repo_info['repo_id'])
				except:
					self.logger.error('Error with {}'.format(repo_info))
					raise

			self.logger.info('Filling in commit parents')

			for repo_info in self.get_repo_list(all_commits=all_commits,option='commit_parents'):
				self.logger.info('Filling commit parenthood info for {source}/{owner}/{name}'.format(**repo_info))
				try:
					self.fill_commit_parents(self.list_commits(basic_info_only=True,allbranches=self.allbranches,**repo_info),repo_id=repo_info['repo_id'])
				except:
					self.logger.error('Error with {}'.format(repo_info))
					raise

			self.db.cursor.execute('''INSERT INTO full_updates(update_type,updated_at) VALUES('commits',(SELECT CURRENT_TIMESTAMP));''')
			self.db.connection.commit()
		else:
			self.logger.info('Skipping filling of commits info')

	def list_commits(self,name,source,owner,basic_info_only=False,repo_id=None,after_time=None,allbranches=False,group_by='sha',remote_branches_only=True):
		'''
		Listing the commits of a repository
		if after time is set to an int (unix time def) or datetime.datetime instead of None, only commits strictly after given time. Commits are listed by default from most recent to least.
		'''
		if isinstance(after_time,datetime.datetime):
			after_time = datetime.datetime.timestamp(after_time)

		repo_obj = self.get_repo(source=source,name=name,owner=owner,engine='pygit2')
		if repo_id is None: # Letting the possibility to preset repo_id to avoid cursor recursive usage
			repo_id = self.db.get_repo_id(source=source,name=name,owner=owner)
		# repo_obj.walk(repo.head.target, pygit2.GIT_SORT_TOPOLOGICAL | pygit2.GIT_SORT_REVERSE)
		# for commit in repo_obj.walk(repo_obj.head.target, pygit2.GIT_SORT_TIME | pygit2.GIT_SORT_REVERSE):

		# tracked_args = set() # When
		tracked_args = mp.Manager().dict()

		if not repo_obj.is_empty:
			if allbranches:
				# gitpython_repo = self.get_repo(source=source,name=name,owner=owner,engine='gitpython')
				# walker = gitpython_repo.iter_commits('--all')
				walker = itertools.chain()
				for b in (repo_obj.branches.remote if remote_branches_only else repo_obj.branches):   # remote branches only ensures that when a git pull results in a merge commit the newly made commit wont be considered
					branch = repo_obj.lookup_branch(b)
					if branch is None:
						branch = repo_obj.lookup_branch(b,2) # parameter 2 is necessary for remotes; main interest in 'allbranches' case
					if not str(branch.target).startswith('refs/'): # The remote branch set on HEAD is behaving differently and instead of commit sha returns a branch name
						walker = itertools.chain(walker,repo_obj.walk(branch.target)) # This is suboptimal: common parts of branches are traversed several times
						# Using gitpython could give the walk among all branches directly; but then the commit object parsing is different and insertions/deletions seem more complicated to compute

			else:
				walker = repo_obj.walk(repo_obj.head.target, pygit2.GIT_SORT_TIME)

			def process_commit(commit):
				if basic_info_only:
					if isinstance(commit,dict):
						return {
							'author_email':commit['author_email'],
							'author_name':commit['author_name'],
							#'localtime':commit['commit_time'],
							'time':commit['time'],#-60*commit.commit_time_offset,
							'time_offset':commit['time_offset'],
							'sha':commit['sha'],
							'parents':commit['parents'],
							'repo_id':repo_id,
							}
					else:
						return {
							'author_email':commit.author.email,
							'author_name':commit.author.name,
							#'localtime':commit.commit_time,
							'time':commit.commit_time,#-60*commit.commit_time_offset,
							'time_offset':commit.commit_time_offset,
							'sha':commit.hex,
							'parents':[pid.hex for pid in commit.parent_ids],
							'repo_id':repo_id,
							}
				else:
					if isinstance(commit,dict):
						commit = repo_obj.get(commit['sha'])
					if commit.parents:
						diff_obj = repo_obj.diff(commit.parents[0],commit)# Inverted order wrt the expected one, to have expected values for insertions and deletions
						insertions = diff_obj.stats.insertions
						deletions = diff_obj.stats.deletions
					else:
						diff_obj = commit.tree.diff_to_tree()
						# re-inverting insertions and deletions, to get expected values
						deletions = diff_obj.stats.insertions
						insertions = diff_obj.stats.deletions
					return {
							'author_email':commit.author.email,
							'author_name':commit.author.name,
							#'localtime':commit.commit_time,
							'time':commit.commit_time,#-60*commit.commit_time_offset,
							'time_offset':commit.commit_time_offset,
							'sha':commit.hex,
							'parents':[pid.hex for pid in commit.parent_ids],
							'insertions':insertions,
							'deletions':deletions,
							'total':insertions+deletions,
							'repo_id':repo_id,
							}

			# Wrapping the walker generator into another one to be able to deal with multiprocessing
			def subgenerator(walker_gen):
				for commit in walker_gen:
					if after_time is not None and (not allbranches) and commit.commit_time<after_time:
						break
					if group_by == 'authors':
						if commit.author.email in tracked_args.keys():
							continue
						else:
							tracked_args[commit.author.email] = True
					elif group_by == 'sha':
						if commit.hex in tracked_args.keys():
							continue
						else:
							tracked_args[commit.hex] = True
					elif group_by is None:
						pass
					else:
						raise ValueError('Unrecognized group_by value: {}'.format(group_by))
					yield process_commit(commit)

			if self.workers == 1:
				wrapper_gen = subgenerator(walker)
			else:
				def mp_generator(subgen):
					# inspired by https://stackoverflow.com/questions/43078980/python-multiprocessing-with-generator
					# Probably does not work on Windows, better to stay at workers = 1
					def gen_to_queue(in_gen,in_q):
						for cmt in in_gen:
							in_q.put(cmt)
						for _ in range(self.workers):
							in_q.put(None)

					def process(in_q, out_q):
						while True:
							cmt = in_q.get()
							if cmt is None:
								out_q.put(None)
								break
							out_q.put(process_commit(cmt))

					# this may be inefficient for small repos: pools and queues could be defined at a more global level and be reused
					input_q = mp.Queue(maxsize=self.workers * 2)
					output_q = mp.Queue(maxsize=self.workers * 2)

					gen_pool = mp.Pool(1, initializer=gen_to_queue, initargs=(subgen,input_q))
					pool = mp.Pool(self.workers, initializer=process, initargs=(input_q, output_q))

					finished_workers = 0
					while True:
						cmt_data = output_q.get()
						if cmt_data is None:
							finished_workers += 1
							if finished_workers == self.workers:
								break
						else:
							yield cmt_data


				wrapper_gen = mp_generator(subgenerator(walker))

			while True:
				yield next(wrapper_gen)

	def get_repo(self,name,source,owner,engine='pygit2'):
		'''
		Returns the pygit2 repository object
		'''
		repo_folder = os.path.join(self.data_folder,'cloned_repos',source,owner,name)
		if not os.path.exists(repo_folder):
			raise ValueError('Repository {}/{}/{} not found in cloned_repos folder'.format(source,owner,name))
		else:
			if engine == 'gitpython':
				return git.Repo(repo_folder)
			elif engine == 'pygit2':
				return pygit2.Repository(os.path.join(repo_folder,'.git'))
			else:
				raise ValueError('Engine not found for getting repo: {}\n Use gitpython or pygit2'.format(engine))

	def fill_authors(self,commit_info_list,repo_id,autocommit=True):
		'''
		Filling authors in table.

		Defining a wrapper around the commit list generator to keep track of data
		Using generator and not lists to be able to deal with high volumes, and lets choice to caller to provide a list or generator.
		'''


		tracked_data = {'latest_commit_time':0,'empty':True}
		def tracked_gen(orig_gen):
			while True:
				try:
					c = next(orig_gen)
					tracked_data['empty'] = False
					tracked_data['last_commit'] = c
					tracked_data['latest_commit_time'] = max(tracked_data['latest_commit_time'],c['time'])
					yield c
				except StopIteration:
					return
				except RuntimeError as e:
					if str(e) == 'generator raised StopIteration':
						return
					else:
						raise

		tr_gen = tracked_gen(commit_info_list)
		# tr_gen = list(tracked_gen(commit_info_list)) # temporary solution, because the list is used twice: one to prefill users table, once for identities. For large number of commits this can trigger enormous memory consumption. Solution would be to call list_commits() twice.


		if self.db.db_type == 'postgres':

			self.db.cursor.execute('''
				INSERT INTO identity_types(name) VALUES('email')
				ON CONFLICT DO NOTHING
				;''')
			self.db.connection.commit()

			extras.execute_batch(self.db.cursor,'''
				INSERT INTO users(
						creation_identity,
						creation_identity_type_id)
							SELECT %(email)s,id FROM identity_types WHERE name='email'
					AND NOT EXISTS (SELECT 1 FROM identities i
						INNER JOIN identity_types it
						ON i.identity=%(email)s AND i.identity_type_id=it.id AND it.name='email')
				ON CONFLICT DO NOTHING;
				INSERT INTO identities(
						attributes,
						identity,
						user_id,
						identity_type_id) SELECT %(info)s,%(email)s,u.id,it.id
						FROM users u
						INNER JOIN identity_types it
						ON it.name='email' AND u.creation_identity=%(email)s AND u.creation_identity_type_id=it.id
				ON CONFLICT DO NOTHING;
				''',({'info':json.dumps({'name':c['author_name']}),'email':c['author_email']} for c in tr_gen))
			self.db.connection.commit()



		else:
			self.db.cursor.execute('''
				INSERT OR IGNORE INTO identity_types(name) VALUES('email')
				;''')
			self.db.connection.commit()
			for c in tr_gen:
				info_dict = {'info':json.dumps({'name':c['author_name']}),'email':c['author_email']}
				self.db.cursor.execute('''
					INSERT OR IGNORE INTO users(
							creation_identity,
							creation_identity_type_id)
								SELECT :email,id FROM identity_types WHERE name='email'
						AND NOT EXISTS  (SELECT 1 FROM identities i
							INNER JOIN identity_types it
							ON i.identity='email' AND i.identity_type_id=it.id AND it.name='email')
					;
					''',info_dict)
				self.db.cursor.execute('''
					INSERT OR IGNORE INTO identities(
							attributes,
							identity,
							user_id,
							identity_type_id) SELECT :info,:email,u.id,it.id
							FROM users u
							INNER JOIN identity_types it
							ON it.name='email' AND u.creation_identity=:email AND u.creation_identity_type_id=it.id
					;
					''',info_dict )
			self.db.connection.commit()

		# self.complete_id_users()

		if not tracked_data['empty']:
			# repo_id = tracked_data['last_commit']['repo_id']
			latest_commit_time = datetime.datetime.fromtimestamp(tracked_data['latest_commit_time'])
		else:
			latest_commit_time = None

		if self.db.db_type == 'postgres':
			self.db.cursor.execute('''INSERT INTO table_updates(repo_id,table_name,latest_commit_time) VALUES(%s,'identities',%s) ;''',(repo_id,latest_commit_time))
		else:
			self.db.cursor.execute('''INSERT INTO table_updates(repo_id,table_name,latest_commit_time) VALUES(?,'identities',?) ;''',(repo_id,latest_commit_time))


		if autocommit:
			self.db.connection.commit()





	def fill_commits(self,commit_info_list,repo_id,autocommit=True):
		'''
		Filling commits in table.
		'''

		tracked_data = {'latest_commit_time':0,'empty':True}
		def tracked_gen(orig_gen):
			while True:
				try:
					c = next(orig_gen)
					tracked_data['last_commit'] = c
					tracked_data['empty'] = False
					tracked_data['latest_commit_time'] = max(tracked_data['latest_commit_time'],c['time'])
					yield c
				except StopIteration:
					return
				except RuntimeError as e:
					if str(e) == 'generator raised StopIteration':
						return
					else:
						raise

		if self.db.db_type == 'postgres':
			extras.execute_batch(self.db.cursor,'''
				INSERT INTO commits(sha,author_id,created_at,insertions,deletions)
					VALUES(%s,
							(SELECT i.id FROM identities i
								INNER JOIN identity_types it
							 ON i.identity=%s AND it.name='email' AND it.id=i.identity_type_id),
							%s,
							%s,
							%s
							)
				ON CONFLICT DO NOTHING;
				''',((c['sha'],c['author_email'],datetime.datetime.fromtimestamp(c['time']),c['insertions'],c['deletions'],) for c in tracked_gen(commit_info_list)))

		else:
			self.db.cursor.executemany('''
				INSERT OR IGNORE INTO commits(sha,author_id,created_at,insertions,deletions)
					VALUES(?,
							(SELECT i.id FROM identities i
								INNER JOIN identity_types it
							 ON i.identity=? AND it.name='email' AND it.id=i.identity_type_id),
							?,
							?,
							?
							);
				''',((c['sha'],c['author_email'],datetime.datetime.fromtimestamp(c['time']),c['insertions'],c['deletions'],) for c in tracked_gen(commit_info_list)))

		if not tracked_data['empty']:
#			repo_id = tracked_data['last_commit']['repo_id']
			latest_commit_time = datetime.datetime.fromtimestamp(tracked_data['latest_commit_time'])
		else:
			latest_commit_time = None
		if self.db.db_type == 'postgres':
			self.db.cursor.execute('''INSERT INTO table_updates(repo_id,table_name,latest_commit_time) VALUES(%s,'commits',%s) ;''',(repo_id,latest_commit_time))
		else:
			self.db.cursor.execute('''INSERT INTO table_updates(repo_id,table_name,latest_commit_time) VALUES(?,'commits',?) ;''',(repo_id,latest_commit_time))



		if autocommit:
			self.db.connection.commit()

	def fill_commit_repos(self,commit_info_list,repo_id,autocommit=True):
		'''
		Filling commit/repo ownership table.
		'''

		tracked_data = {'latest_commit_time':0,'empty':True}
		def tracked_gen(orig_gen):
			while True:
				try:
					c = next(orig_gen)
					tracked_data['last_commit'] = c
					tracked_data['empty'] = False
					tracked_data['latest_commit_time'] = max(tracked_data['latest_commit_time'],c['time'])
					yield c
				except StopIteration:
					return
				except RuntimeError as e:
					if str(e) == 'generator raised StopIteration':
						return
					else:
						raise

		if self.db.db_type == 'postgres':
			extras.execute_batch(self.db.cursor,'''
				INSERT INTO commit_repos(commit_id,repo_id)
					VALUES(
							(SELECT id FROM commits WHERE sha=%s),
							%s
							)
				ON CONFLICT DO NOTHING;
				''',((c['sha'],c['repo_id'],) for c in tracked_gen(commit_info_list)))

		else:
			self.db.cursor.executemany('''
				INSERT OR IGNORE INTO commit_repos(commit_id,repo_id)
					VALUES(
							(SELECT id FROM commits WHERE sha=?),
							?
							);
				''',((c['sha'],c['repo_id'],) for c in tracked_gen(commit_info_list)))


		if not tracked_data['empty']:
			# repo_id = tracked_data['last_commit']['repo_id']
			latest_commit_time = datetime.datetime.fromtimestamp(tracked_data['latest_commit_time'])
		else:
			latest_commit_time = None
		if self.db.db_type == 'postgres':
			self.db.cursor.execute('''INSERT INTO table_updates(repo_id,table_name,latest_commit_time) VALUES(%s,'commit_repos',%s) ;''',(repo_id,latest_commit_time))
		else:
			self.db.cursor.execute('''INSERT INTO table_updates(repo_id,table_name,latest_commit_time) VALUES(?,'commit_repos',?) ;''',(repo_id,latest_commit_time))



		if autocommit:
			self.db.connection.commit()

	# def fill_commit_orig_repo(self,commit_info_list,autocommit=True):
	# 	'''
	# 	Filling commits attribute is_orig_repo, based on forks table
	# 	'''

	# 	tracked_data = {'latest_commit_time':0,'empty':True}
	# 	def tracked_gen(orig_gen):
	# 		for c in orig_gen:
	# 			tracked_data['last_commit'] = c
	# 			tracked_data['empty'] = False
	# 			tracked_data['latest_commit_time'] = max(tracked_data['latest_commit_time'],c['time'])
	# 			yield c

	# 	if self.db.db_type == 'postgres':
	# 		extras.execute_batch(self.db.cursor,'''
	# 			INSERT INTO commit_repos(commit_id,repo_id)
	# 				VALUES(
	# 						(SELECT id FROM commits WHERE sha=%s),
	# 						%s
	# 						)
	# 			ON CONFLICT DO NOTHING;
	# 			''',((c['sha'],c['repo_id'],) for c in tracked_gen(commit_info_list)))

	# 	else:
	# 		self.db.cursor.executemany('''
	# 			INSERT OR IGNORE INTO commit_repos(commit_id,repo_id)
	# 				VALUES(
	# 						(SELECT id FROM commits WHERE sha=?),
	# 						?
	# 						);
	# 			''',((c['sha'],c['repo_id'],) for c in tracked_gen(commit_info_list)))


	# 	if not tracked_data['empty']:
	# 		repo_id = tracked_data['last_commit']['repo_id']
	# 		latest_commit_time = datetime.datetime.fromtimestamp(tracked_data['latest_commit_time'])
	# 		if self.db.db_type == 'postgres':
	# 			self.db.cursor.execute('''INSERT INTO table_updates(repo_id,table_name,latest_commit_time) VALUES(%s,'commit_orig_repos',%s) ;''',(repo_id,latest_commit_time))
	# 		else:
	# 			self.db.cursor.execute('''INSERT INTO table_updates(repo_id,table_name,latest_commit_time) VALUES(?,'commit_orig_repos',?) ;''',(repo_id,latest_commit_time))



	# 	if autocommit:
	# 		self.db.connection.commit()

	def fill_commit_parents(self,commit_info_list,repo_id,autocommit=True):
		'''
		Creating table if necessary.
		Filling commit parenthood in table.
		'''

		tracked_data = {'latest_commit_time':0,'empty':True}
		def transformed_list(orig_gen):
			while True:
				try:
					c = next(orig_gen)
					tracked_data['last_commit'] = c
					tracked_data['empty'] = False
					tracked_data['latest_commit_time'] = max(tracked_data['latest_commit_time'],c['time'])
					c_id = c['sha']
					for r,p_id in enumerate(c['parents']):
						yield (c_id,p_id,r)
				except StopIteration:
					return
				except RuntimeError as e:
					if str(e) == 'generator raised StopIteration':
						return
					else:
						raise

		if self.db.db_type == 'postgres':
			extras.execute_batch(self.db.cursor,'''
				INSERT INTO commit_parents(child_id,parent_id,rank)
					VALUES(
							(SELECT id FROM commits WHERE sha=%s),
							(SELECT id FROM commits WHERE sha=%s),
							%s)
				ON CONFLICT DO NOTHING;
				''',transformed_list(commit_info_list))

		else:
			self.db.cursor.executemany('''
				INSERT OR IGNORE INTO commit_parents(child_id,parent_id,rank)
					VALUES(
							(SELECT id FROM commits WHERE sha=?),
							(SELECT id FROM commits WHERE sha=?),
							?);
				''',transformed_list(commit_info_list))

		if not tracked_data['empty']:
			# repo_id = tracked_data['last_commit']['repo_id']
			latest_commit_time = datetime.datetime.fromtimestamp(tracked_data['latest_commit_time'])
		else:
			latest_commit_time = None
		if self.db.db_type == 'postgres':
			self.db.cursor.execute('''INSERT INTO table_updates(repo_id,table_name,latest_commit_time) VALUES(%s,'commit_parents',%s) ;''',(repo_id,latest_commit_time))
			self.db.cursor.execute('''UPDATE repositories SET latest_commit_time=COALESCE(%s,latest_commit_time) WHERE id=%s;''',(latest_commit_time,repo_id))
		else:
			self.db.cursor.execute('''INSERT INTO table_updates(repo_id,table_name,latest_commit_time) VALUES(?,'commit_parents',?) ;''',(repo_id,latest_commit_time))
			self.db.cursor.execute('''UPDATE repositories SET latest_commit_time=COALESCE(?,latest_commit_time) WHERE id=?;''',(latest_commit_time,repo_id))


		if autocommit:
			self.db.connection.commit()

	def fill_commit_orig_repo(self,only_null=True,force=False,autocommit=True):
		'''
		Setting the repo_id field for commits table, using the forks table and supposing that it is updated.

		If only_null is set to True, only commits with a null value for repo_id will be updated.

		It first sets is_orig_repo in commit_repos, and then fills in the commits table.
		is_orig_repo is set to true if one of the following is true
		 - there is only one repo owning the commit
		 - the repo owning the commit is the forked repo with the highest rank among those owning the commit
		For repos lower in fork rank, is_orig_repo is set to false.
		Table commits is filled with ids of true is_orig_repo, provided there are no NULL values remaining


		NB: This supposes that forks have been filled in before!! And that if a commit is part of a set of repos that are part of a fork tree, all repos are part of the fork tree.
		This last comment could lead to 2 repositories fitting to is_orig_repo in a suposedly rare case:
			- on a first update, one single repo is present for a given commit and gets orig status.
			- on a second update, more repos have been added, part of a fork tree, but the first repo is not part of that tree
			- the root of the fork tree get the orig status
		This is avoided with a run with only_null set to False.
		'''


		self.db.cursor.execute('''SELECT MAX(updated_at) FROM full_updates WHERE update_type='commits orig repos';''')
		last_fu = self.db.cursor.fetchone()[0]
		self.db.cursor.execute('''SELECT MAX(updated_at) FROM full_updates WHERE update_type='commits';''')
		last_fu_commits = self.db.cursor.fetchone()[0]
		if not force and last_fu is not None and last_fu_commits<=last_fu:
			self.logger.info('Skipping commit origin repository attribution')
		else:
			self.logger.info('Filling commit origin repository attribution')
			if only_null:
				# update is_orig_repo to true for roots of fork trees
				self.logger.info('Filling commit origin repository attribution: update is_orig_repo to true for roots of fork trees')
				self.db.cursor.execute('''
						UPDATE commit_repos SET is_orig_repo=true
							WHERE is_orig_repo IS NULL
								AND repo_id = (SELECT ccp.repo_id
									FROM commit_repos ccp
									INNER JOIN forks f
									ON ccp.commit_id=commit_repos.commit_id AND f.forked_repo_id=ccp.repo_id
									INNER JOIN commit_repos ccp2
									ON ccp2.commit_id=commit_repos.commit_id AND f.forking_repo_id=ccp2.repo_id
									ORDER BY f.fork_rank DESC
									LIMIT 1
									)
						;''')



				# update is_orig_repo to true for repos that are the only ones owning the commit

				self.logger.info('Filling commit origin repository attribution: update is_orig_repo to true for repos that are the only ones owning the commit')
				self.db.cursor.execute('''
						UPDATE commit_repos SET is_orig_repo=true
							WHERE is_orig_repo IS NULL
								AND (SELECT COUNT(*) FROM commit_repos ccp
									WHERE ccp.commit_id=commit_repos.commit_id) = 1
						;''')

				# set to null where twice true for is_orig_repo
				self.logger.info('Filling commit origin repository attribution: set to null where twice true for is_orig_repo')
				self.db.cursor.execute('''
						UPDATE commit_repos SET is_orig_repo=NULL
							WHERE commit_id IN (
								SELECT cr.commit_id FROM commit_repos cr
								WHERE cr.is_orig_repo
								GROUP BY cr.commit_id
								HAVING COUNT(*)>=2
									)
						;''')


				#REDO STEP1 update is_orig_repo to true for roots of fork trees
				self.logger.info('Filling commit origin repository attribution: REDO update is_orig_repo to true for roots of fork trees')
				self.db.cursor.execute('''
						UPDATE commit_repos SET is_orig_repo=true
							WHERE is_orig_repo IS NULL
								AND repo_id = (SELECT ccp.repo_id
									FROM commit_repos ccp
									INNER JOIN forks f
									ON ccp.commit_id=commit_repos.commit_id AND f.forked_repo_id=ccp.repo_id
									INNER JOIN commit_repos ccp2
									ON ccp2.commit_id=commit_repos.commit_id AND f.forking_repo_id=ccp2.repo_id
									ORDER BY f.fork_rank DESC
									LIMIT 1
									)
						;''')



				#REDO STEP2 update is_orig_repo to true for repos that are the only ones owning the commit

				self.logger.info('Filling commit origin repository attribution: REDO update is_orig_repo to true for repos that are the only ones owning the commit')
				self.db.cursor.execute('''
						UPDATE commit_repos SET is_orig_repo=true
							WHERE is_orig_repo IS NULL
								AND (SELECT COUNT(*) FROM commit_repos ccp
									WHERE ccp.commit_id=commit_repos.commit_id) = 1
						;''')


				# update is_orig_repo to false for repos elsewhere in fork trees
				self.logger.info('Filling commit origin repository attribution: update is_orig_repo to false for repos elsewhere in fork trees')
				self.db.cursor.execute('''
						UPDATE commit_repos SET is_orig_repo=false
							WHERE is_orig_repo IS NULL
								AND repo_id IN (SELECT ccp2.repo_id
									FROM commit_repos ccp
									INNER JOIN forks f
									ON ccp.commit_id=commit_repos.commit_id AND f.forked_repo_id=ccp.repo_id
									INNER JOIN commit_repos ccp2
									ON ccp2.commit_id=commit_repos.commit_id AND f.forking_repo_id=ccp2.repo_id
									)
						;''')


			else:


				# set all origs to NULL
				self.logger.info('Filling commit origin repository attribution: setting everything to null')
				self.db.cursor.execute('''
						UPDATE commit_repos SET is_orig_repo=NULL;''')

				# update is_orig_repo to true for roots of fork trees
				self.logger.info('Filling commit origin repository attribution: update is_orig_repo to true for roots of fork trees')
				self.db.cursor.execute('''
						UPDATE commit_repos SET is_orig_repo=true
							WHERE repo_id = (SELECT ccp.repo_id
									FROM commit_repos ccp
									INNER JOIN forks f
									ON ccp.commit_id=commit_repos.commit_id AND f.forked_repo_id=ccp.repo_id
									INNER JOIN commit_repos ccp2
									ON ccp2.commit_id=commit_repos.commit_id AND f.forking_repo_id=ccp2.repo_id
									ORDER BY f.fork_rank DESC
									LIMIT 1
									)
						;''')

				# update is_orig_repo to true for repos that are the only ones owning the commit

				self.logger.info('Filling commit origin repository attribution: update is_orig_repo to true for repos that are the only ones owning the commit')
				self.db.cursor.execute('''
						UPDATE commit_repos SET is_orig_repo=true
							WHERE (SELECT COUNT(*) FROM commit_repos ccp
									WHERE ccp.commit_id=commit_repos.commit_id) = 1
						;''')

				# update is_orig_repo to false for repos elsewhere in fork trees
				self.logger.info('Filling commit origin repository attribution: update is_orig_repo to false for repos elsewhere in fork trees')
				self.db.cursor.execute('''
						UPDATE commit_repos SET is_orig_repo=false
							WHERE repo_id IN (SELECT ccp2.repo_id
									FROM commit_repos ccp
									INNER JOIN forks f
									ON ccp.commit_id=commit_repos.commit_id AND f.forked_repo_id=ccp.repo_id
									INNER JOIN commit_repos ccp2
									ON ccp2.commit_id=commit_repos.commit_id AND f.forking_repo_id=ccp2.repo_id
									)
						;''')


			# self.logger.info('Filling commit origin repository attribution: checking that no commit has 2 orig repos')
			# self.db.cursor.execute('''
			# 		SELECT r.owner,r.name,COUNT(*) FROM repositories r
			# 			INNER JOIN commit_repos cr2
			# 				ON r.id=cr2.repo_id AND cr2.is_orig_repo
			# 			INNER JOIN
			# 				(SELECT cr.commit_id,COUNT(*) AS cnt FROM commit_repos cr
			# 					WHERE cr.is_orig_repo
			# 					GROUP BY cr.commit_id
			# 					HAVING COUNT(*)>1) AS c
			# 				ON c.commit_id=cr2.commit_id
			# 			GROUP BY r.owner,r.name
			# 		;
			# 		''')
			# results = list(self.db.cursor.fetchall())
			# if len(results):
			# 	error_str = 'Several repos are considered origin repos of the same commits. Repos in this situation: {}, First ten: {}'.format(len(results),['{}/{}:{}'.format(*r) for r in results[:10]])
			# 	raise ValueError(error_str)

			self.logger.info('Filling commit origin repository attribution: updating commits table')
			self.db.cursor.execute('''
					UPDATE commits SET repo_id=(
							SELECT cp.repo_id FROM commit_repos cp
								WHERE cp.commit_id=commits.id
								AND cp.is_orig_repo)
					;
					''')

			self.db.cursor.execute('''INSERT INTO full_updates(update_type) VALUES('commits orig repos');''')

			if autocommit:
				self.db.connection.commit()

	def fill_commit_created_at(self,force=False,autocommit=True,batch_size=1000):
		'''
		Setting the created_at field so that it respects the condition of always being after (in time) the timestamp(s) of the parent(s)
		'''


		self.db.cursor.execute('''SELECT MAX(updated_at) FROM full_updates WHERE update_type='commits created_at';''')
		last_fu = self.db.cursor.fetchone()[0]
		self.db.cursor.execute('''SELECT MAX(updated_at) FROM full_updates WHERE update_type='commits';''')
		last_fu_commits = self.db.cursor.fetchone()[0]
		if not force and last_fu is not None and last_fu_commits<=last_fu:
			self.logger.info('Skipping commit created_at fix')
		else:
			self.logger.info('Fixing commits created_at')
			updated_count = None
			step = 1

			self.db.cursor.execute('''
					UPDATE commits SET created_at=CURRENT_TIMESTAMP,original_created_at=COALESCE(original_created_at,commits.created_at)
					WHERE created_at>CURRENT_TIMESTAMP
					;
					''')
			updated_count_currenttime = self.db.cursor.rowcount
			self.logger.info('Fixed created_at field for {} commits (using current time)'.format(updated_count_currenttime))

			self.db.cursor.execute('''
					SELECT COUNT(*) FROM commits c
							INNER JOIN commit_parents cp
							ON cp.child_id =c.id
							INNER JOIN commits c2
							ON c2.id=cp.parent_id
							AND c2.created_at >c.created_at
					;''')


			self.logger.info('Identified {} conflicting commit pairs concerning the created_at field (batch_size {})'.format(self.db.cursor.fetchone()[0],batch_size))
			while updated_count != 0:
				# Create table of conflictual parent/child pairs
				self.db.cursor.execute('''
					CREATE TEMPORARY TABLE temp_conflict_pairs(
						child_id BIGINT,
						child_timestamp TIMESTAMP,
						parent_id BIGINT,
						parent_timestamp TIMESTAMP,
						PRIMARY KEY(child_id,parent_id)
					)
					;''')

				self.db.cursor.execute('''
					INSERT INTO temp_conflict_pairs(child_id,child_timestamp,parent_id,parent_timestamp)
						SELECT c.id,c.created_at ,c2.id,c2.created_at  FROM commits c
							INNER JOIN commit_parents cp
							ON cp.child_id =c.id
							INNER JOIN commits c2
							ON c2.id=cp.parent_id
							AND c2.created_at >c.created_at
						ORDER BY c2.id,c.created_at DESC
						LIMIT {batch_size}
					;'''.format(batch_size=int(batch_size)))

				conflicting_pairs = self.db.cursor.rowcount
				self.logger.info('Identified {} conflicting commit pairs concerning the created_at field at step {} (batch_size {})'.format(conflicting_pairs,step,batch_size))

				# Create table of conflict levels for all (parents and children)

				self.db.cursor.execute('''
					CREATE TEMPORARY TABLE temp_conflict_levels(
						commit_id BIGINT PRIMARY KEY,
						as_child INT,
						as_parent INT
					)
					;''')

				self.db.cursor.execute('''
					WITH RECURSIVE parent_tree(child_id,ref_time,ancestor_id,ancestor_time) AS (
							SELECT tcp.child_id,tcp.child_timestamp,tcp.child_id,tcp.child_timestamp
								FROM temp_conflict_pairs tcp
						UNION
							SELECT tcp.parent_id,tcp.parent_timestamp,tcp.parent_id,tcp.parent_timestamp
								FROM temp_conflict_pairs tcp
						UNION
							SELECT pt.child_id,pt.ref_time,cp.parent_id,c.created_at FROM commit_parents cp
							INNER JOIN parent_tree pt
							ON cp.child_id=pt.ancestor_id
							INNER JOIN commits c
							ON c.id=cp.parent_id
						)
					INSERT INTO temp_conflict_levels(commit_id,as_child)
						SELECT pt.child_id,SUM(CASE WHEN pt.ref_time < pt.ancestor_time THEN 1 ELSE 0 END) AS cnt FROM parent_tree pt
								GROUP BY pt.child_id
					;''')

				if self.db.db_type == 'postgres':
					self.db.cursor.execute('''
						WITH RECURSIVE child_tree(parent_id,ref_time,offspring_id,offspring_time) AS (
								SELECT tcp.child_id,tcp.child_timestamp,tcp.child_id,tcp.child_timestamp
									FROM temp_conflict_pairs tcp
							UNION
								SELECT tcp.parent_id,tcp.parent_timestamp,tcp.parent_id,tcp.parent_timestamp
									FROM temp_conflict_pairs tcp
							UNION
								SELECT ct.parent_id,ct.ref_time,cp.child_id,c.created_at FROM commit_parents cp
								INNER JOIN child_tree ct
								ON cp.parent_id=ct.offspring_id
								INNER JOIN commits c
								ON c.id=cp.child_id
							)
						UPDATE temp_conflict_levels
							SET as_parent=children.cnt
							FROM (SELECT ct.parent_id,SUM(CASE WHEN ct.ref_time > ct.offspring_time THEN 1 ELSE 0 END) AS cnt FROM child_tree ct
									--WHERE ct.ref_time > ct.offspring_time
									GROUP BY ct.parent_id) AS children
							WHERE commit_id=children.parent_id
						;''')
				else:
					self.db.cursor.execute('''
						WITH RECURSIVE child_tree(parent_id,ref_time,offspring_id,offspring_time) AS (
								SELECT tcp.child_id,tcp.child_timestamp,tcp.child_id,tcp.child_timestamp
									FROM temp_conflict_pairs tcp
							UNION
								SELECT tcp.parent_id,tcp.parent_timestamp,tcp.parent_id,tcp.parent_timestamp
									FROM temp_conflict_pairs tcp
							UNION
								SELECT ct.parent_id,ct.ref_time,cp.child_id,c.created_at FROM commit_parents cp
								INNER JOIN child_tree ct
								ON cp.parent_id=ct.offspring_id
								INNER JOIN commits c
								ON c.id=cp.child_id
							),
						aggreg_children AS (SELECT ct.parent_id,SUM(CASE WHEN ct.ref_time > ct.offspring_time THEN 1 ELSE 0 END) AS cnt FROM child_tree ct
									GROUP BY ct.parent_id)
						UPDATE temp_conflict_levels
							SET as_parent=(SELECT cnt FROM aggreg_children WHERE commit_id=aggreg_children.parent_id)
							WHERE EXISTS (SELECT cnt FROM aggreg_children WHERE commit_id=aggreg_children.parent_id)
						;''')

				self.db.cursor.execute('SELECT MAX(as_child),MAX(as_parent) FROM temp_conflict_levels;')
				self.logger.info('Max depth of conflict in batch: {} as child, {} as parent'.format(*self.db.cursor.fetchone()))
				# Solve the conflicts

				self.db.cursor.execute('''
					CREATE TEMPORARY TABLE temp_conflict_new_values(
					commit_id BIGINT PRIMARY KEY,
					created_at TIMESTAMP
					)
					;''')

				self.db.cursor.execute('''
					INSERT INTO temp_conflict_new_values(commit_id,created_at)
						SELECT tcp.parent_id,MIN(tcp.child_timestamp)
							FROM temp_conflict_pairs tcp
							INNER JOIN temp_conflict_levels tclp
							ON tclp.commit_id=tcp.parent_id
							INNER JOIN temp_conflict_levels tclc
							ON tclc.commit_id=tcp.child_id
							--AND tclp.as_parent+tclp.as_child > tclc.as_parent+tclc.as_child
							AND (tclp.as_parent > tclc.as_child OR tcp.parent_timestamp<'2000-01-01')
						GROUP BY tcp.parent_id
					ON CONFLICT DO NOTHING
					;''')

				self.db.cursor.execute('''
					INSERT INTO temp_conflict_new_values(commit_id,created_at)
						SELECT tcp.child_id,MAX(tcp.parent_timestamp)
							FROM temp_conflict_pairs tcp
							INNER JOIN temp_conflict_levels tclp
							ON tclp.commit_id=tcp.parent_id
							INNER JOIN temp_conflict_levels tclc
							ON tclc.commit_id=tcp.child_id
							--AND tclp.as_parent+tclp.as_child <= tclc.as_parent+tclc.as_child
							AND (tclp.as_parent <= tclc.as_child OR tcp.child_timestamp<'2000-01-01')
						GROUP BY tcp.child_id
					ON CONFLICT DO NOTHING
					;''')

				# Update
				if self.db.db_type == 'postgres':
					self.db.cursor.execute('''
						UPDATE commits SET created_at=nv.created_at,original_created_at=COALESCE(original_created_at,commits.created_at)
						FROM temp_conflict_new_values nv
						WHERE commits.id=nv.commit_id
						AND commits.created_at != nv.created_at
						;
						''')
				else:
					self.db.cursor.execute('''
						UPDATE commits SET created_at=(SELECT nv.created_at FROM temp_conflict_new_values nv
													WHERE commits.id=nv.commit_id
													AND commits.created_at != nv.created_at),
										original_created_at=COALESCE(original_created_at,commits.created_at)
						WHERE EXISTS (SELECT nv.created_at FROM temp_conflict_new_values nv
										WHERE commits.id=nv.commit_id
										AND commits.created_at != nv.created_at)
						;
						''')
				updated_count_parents = self.db.cursor.rowcount
				updated_count = updated_count_parents

				# Drop tables

				self.db.cursor.execute('''
					DROP TABLE temp_conflict_pairs
					;''')

				self.db.cursor.execute('''
					DROP TABLE temp_conflict_levels
					;''')

				self.db.cursor.execute('''
					DROP TABLE temp_conflict_new_values
					;''')

				self.logger.info('Fixed created_at field for {} commits on step {}'.format(updated_count,step))
				step += 1

				# Reset where original_created_at=created_at

				self.db.cursor.execute('''
					UPDATE commits SET original_created_at=NULL
					WHERE original_created_at IS NOT NULL
					AND created_at=original_created_at
					;
					''')
				self.logger.info('Reset to NULL original_created_at for {} commits because matching created_at'.format(self.db.cursor.rowcount))

				self.db.connection.commit()

				self.db.cursor.execute('''
					SELECT COUNT(*) FROM commits c
							INNER JOIN commit_parents cp
							ON cp.child_id =c.id
							INNER JOIN commits c2
							ON c2.id=cp.parent_id
							AND c2.created_at >c.created_at
					;''')

				self.logger.info('Remaining overall {} conflicting commit pairs concerning the created_at field (batch_size {})'.format(self.db.cursor.fetchone()[0],batch_size))


			self.db.cursor.execute('''INSERT INTO full_updates(update_type) VALUES('commits created_at');''')

			self.logger.info('Fixed commits created_at')
			if autocommit:
				self.db.connection.commit()
