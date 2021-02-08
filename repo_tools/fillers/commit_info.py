import datetime
import os
import psycopg2
from psycopg2 import extras
import pygit2
import json

from repo_tools import fillers
from repo_tools.fillers import generic
import repo_tools as rp

class CommitsFiller(fillers.Filler):
	"""
	global commit parser
	"""


	def __init__(self,
			only_null_commit_origs=True,
			all_commits=False,
			force=False,
					**kwargs):
		self.force=force
		self.only_null_commit_origs = only_null_commit_origs
		self.all_commits = all_commits
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
					self.fill_authors(self.list_commits(basic_info_only=True,**repo_info))
				except:
					self.logger.error('Error with {}'.format(repo_info))
					raise
			self.logger.info('Filling in commits')

			for repo_info in self.get_repo_list(all_commits=all_commits,option='commits'):
				self.logger.info('Filling commits info for {source}/{owner}/{name}'.format(**repo_info))
				try:
					self.fill_commits(self.list_commits(basic_info_only=False,**repo_info))
				except:
					self.logger.error('Error with {}'.format(repo_info))
					raise


			self.logger.info('Filling in repository commit ownership')

			for repo_info in self.get_repo_list(all_commits=all_commits,option='commit_repos'):
				self.logger.info('Filling commit ownership info for {source}/{owner}/{name}'.format(**repo_info))
				try:
					self.fill_commit_repos(self.list_commits(basic_info_only=False,**repo_info))
				except:
					self.logger.error('Error with {}'.format(repo_info))
					raise

			self.logger.info('Filling in commit parents')

			for repo_info in self.get_repo_list(all_commits=all_commits,option='commit_parents'):
				self.logger.info('Filling commit parenthood info for {source}/{owner}/{name}'.format(**repo_info))
				try:
					self.fill_commit_parents(self.list_commits(basic_info_only=True,**repo_info))
				except:
					self.logger.error('Error with {}'.format(repo_info))
					raise

			self.db.cursor.execute('''INSERT INTO full_updates(update_type,updated_at) VALUES('commits',(SELECT CURRENT_TIMESTAMP));''')
			self.db.connection.commit()
		else:
			self.logger.info('Skipping filling of commits info')

	def list_commits(self,name,source,owner,basic_info_only=False,repo_id=None,after_time=None):
		'''
		Listing the commits of a repository
		if after time is set to an int (unix time def) or datetime.datetime instead of None, only commits strictly after given time. Commits are listed by default from most recent to least.
		'''
		if isinstance(after_time,datetime.datetime):
			after_time = datetime.datetime.timestamp(after_time)

		repo_obj = self.get_repo(source=source,name=name,owner=owner)
		if repo_id is None: # Letting the possibility to preset repo_id to avoid cursor recursive usage
			repo_id = self.db.get_repo_id(source=source,name=name,owner=owner)
		# repo_obj.walk(repo.head.target, pygit2.GIT_SORT_TOPOLOGICAL | pygit2.GIT_SORT_REVERSE)
		# for commit in repo_obj.walk(repo_obj.head.target, pygit2.GIT_SORT_TIME | pygit2.GIT_SORT_REVERSE):

		if not repo_obj.is_empty:
			for commit in repo_obj.walk(repo_obj.head.target, pygit2.GIT_SORT_TIME):
				if after_time is not None and commit.commit_time<after_time:
					break
				if basic_info_only:
					yield {
							'author_email':commit.author.email,
							'author_name':commit.author.name,
							'time':commit.commit_time,
							'time_offset':commit.commit_time_offset,
							'sha':commit.hex,
							'parents':[pid.hex for pid in commit.parent_ids],
							'repo_id':repo_id,
							}
				else:
					if commit.parents:
						diff_obj = repo_obj.diff(commit.parents[0],commit)# Inverted order wrt the expected one, to have expected values for insertions and deletions
						insertions = diff_obj.stats.insertions
						deletions = diff_obj.stats.deletions
					else:
						diff_obj = commit.tree.diff_to_tree()
						# re-inverting insertions and deletions, to get expected values
						deletions = diff_obj.stats.insertions
						insertions = diff_obj.stats.deletions
					yield {
							'author_email':commit.author.email,
							'author_name':commit.author.name,
							'time':commit.commit_time,
							'time_offset':commit.commit_time_offset,
							'sha':commit.hex,
							'parents':[pid.hex for pid in commit.parent_ids],
							'insertions':insertions,
							'deletions':deletions,
							'total':insertions+deletions,
							'repo_id':repo_id,
							}

	def get_repo(self,name,source,owner):
		'''
		Returns the pygit2 repository object
		'''
		repo_folder = os.path.join(self.data_folder,'cloned_repos',source,owner,name)
		if not os.path.exists(repo_folder):
			raise ValueError('Repository {}/{}/{} not found in cloned_repos folder'.format(source,owner,name))
		else:
			return pygit2.Repository(os.path.join(repo_folder,'.git'))


	def fill_authors(self,commit_info_list,autocommit=True):
		'''
		Filling authors in table.

		Defining a wrapper around the commit list generator to keep track of data
		Using generator and not lists to be able to deal with high volumes, and lets choice to caller to provide a list or generator.
		'''


		tracked_data = {'latest_commit_time':0,'empty':True}
		def tracked_gen(orig_gen):
			for c in orig_gen:
				tracked_data['empty'] = False
				tracked_data['last_commit'] = c
				tracked_data['latest_commit_time'] = max(tracked_data['latest_commit_time'],c['time'])
				yield c

		# tr_gen = tracked_gen(commit_info_list)
		tr_gen = list(tracked_gen(commit_info_list)) # temporary solution, because the list is used twice: one to prefill users table, once for identities. For large number of commits this can trigger enormous memory consumption. Solution would be to call list_commits() twice.


		if self.db.db_type == 'postgres':

			self.db.cursor.execute('''
				INSERT INTO identity_types(name) VALUES('email')
				ON CONFLICT DO NOTHING
				;''')

			extras.execute_batch(self.db.cursor,'''
				INSERT INTO users(
						creation_identity,
						creation_identity_type_id) VALUES(%s,(SELECT id FROM identity_types WHERE name='email'))
				ON CONFLICT DO NOTHING;
				''',((c['author_email'],) for c in tr_gen))

			extras.execute_batch(self.db.cursor,'''
				INSERT INTO identities(
						attributes,
						identity,
						user_id,
						identity_type_id) VALUES(%s,%s,
						(SELECT id FROM users WHERE creation_identity=%s AND creation_identity_type_id=(SELECT id FROM identity_types WHERE name='email')),
						(SELECT id FROM identity_types WHERE name='email'))
				ON CONFLICT DO NOTHING;
				''',((json.dumps({'name':c['author_name']}),c['author_email'],c['author_email']) for c in tr_gen))



		else:
			self.db.cursor.execute('''
				INSERT OR IGNORE INTO identity_types(name) VALUES('email')
				;''')

			self.db.cursor.executemany('''
				INSERT OR IGNORE INTO users(
						creation_identity,
						creation_identity_type_id) VALUES(?,(SELECT id FROM identity_types WHERE name='email'))
				;
				''',((c['author_email'],) for c in tr_gen))

			self.db.cursor.executemany('''
				INSERT OR IGNORE INTO identities(
						attributes,
						identity,
						user_id,
						identity_type_id) VALUES(?,?,
						(SELECT id FROM users WHERE creation_identity=? AND creation_identity_type_id=(SELECT id FROM identity_types WHERE name='email')),
						(SELECT id FROM identity_types WHERE name='email'))
				;
				''',((json.dumps({'name':c['author_name']}),c['author_email'],c['author_email']) for c in tr_gen))

		# self.complete_id_users()

		if not tracked_data['empty']:
			repo_id = tracked_data['last_commit']['repo_id']
			latest_commit_time = datetime.datetime.fromtimestamp(tracked_data['latest_commit_time'])
			if self.db.db_type == 'postgres':
				self.db.cursor.execute('''INSERT INTO table_updates(repo_id,table_name,latest_commit_time) VALUES(%s,'identities',%s) ;''',(repo_id,latest_commit_time))
			else:
				self.db.cursor.execute('''INSERT INTO table_updates(repo_id,table_name,latest_commit_time) VALUES(?,'identities',?) ;''',(repo_id,latest_commit_time))


		if autocommit:
			self.db.connection.commit()





	def fill_commits(self,commit_info_list,autocommit=True):
		'''
		Filling commits in table.
		'''

		tracked_data = {'latest_commit_time':0,'empty':True}
		def tracked_gen(orig_gen):
			for c in orig_gen:
				tracked_data['last_commit'] = c
				tracked_data['empty'] = False
				tracked_data['latest_commit_time'] = max(tracked_data['latest_commit_time'],c['time'])
				yield c

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
			repo_id = tracked_data['last_commit']['repo_id']
			latest_commit_time = datetime.datetime.fromtimestamp(tracked_data['latest_commit_time'])
			if self.db.db_type == 'postgres':
				self.db.cursor.execute('''INSERT INTO table_updates(repo_id,table_name,latest_commit_time) VALUES(%s,'commits',%s) ;''',(repo_id,latest_commit_time))
			else:
				self.db.cursor.execute('''INSERT INTO table_updates(repo_id,table_name,latest_commit_time) VALUES(?,'commits',?) ;''',(repo_id,latest_commit_time))



		if autocommit:
			self.db.connection.commit()

	def fill_commit_repos(self,commit_info_list,autocommit=True):
		'''
		Filling commit/repo ownership table.
		'''

		tracked_data = {'latest_commit_time':0,'empty':True}
		def tracked_gen(orig_gen):
			for c in orig_gen:
				tracked_data['last_commit'] = c
				tracked_data['empty'] = False
				tracked_data['latest_commit_time'] = max(tracked_data['latest_commit_time'],c['time'])
				yield c

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
			repo_id = tracked_data['last_commit']['repo_id']
			latest_commit_time = datetime.datetime.fromtimestamp(tracked_data['latest_commit_time'])
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

	def fill_commit_parents(self,commit_info_list,autocommit=True):
		'''
		Creating table if necessary.
		Filling commit parenthood in table.
		'''

		tracked_data = {'latest_commit_time':0,'empty':True}
		def transformed_list(orig_gen):
			for c in orig_gen:
				tracked_data['last_commit'] = c
				tracked_data['empty'] = False
				tracked_data['latest_commit_time'] = max(tracked_data['latest_commit_time'],c['time'])
				c_id = c['sha']
				for r,p_id in enumerate(c['parents']):
					yield (c_id,p_id,r)

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
			repo_id = tracked_data['last_commit']['repo_id']
			latest_commit_time = datetime.datetime.fromtimestamp(tracked_data['latest_commit_time'])
			if self.db.db_type == 'postgres':
				self.db.cursor.execute('''INSERT INTO table_updates(repo_id,table_name,latest_commit_time) VALUES(%s,'commit_parents',%s) ;''',(repo_id,latest_commit_time))
				self.db.cursor.execute('''UPDATE repositories SET latest_commit_time=%s WHERE id=%s;''',(latest_commit_time,repo_id))
			else:
				self.db.cursor.execute('''INSERT INTO table_updates(repo_id,table_name,latest_commit_time) VALUES(?,'commit_parents',?) ;''',(repo_id,latest_commit_time))
				self.db.cursor.execute('''UPDATE repositories SET latest_commit_time=? WHERE id=?;''',(latest_commit_time,repo_id))


		if autocommit:
			self.db.connection.commit()

	def fill_commit_orig_repo(self,only_null=True,autocommit=True):
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
		if last_fu is not None and last_fu_commits>last_fu:
			self.logger.info('Skipping commit origin repository attribution')
		else:
			if only_null:
				# update is_orig_repo to true for roots of fork trees
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

				self.db.cursor.execute('''
						UPDATE commit_repos SET is_orig_repo=true
							WHERE is_orig_repo IS NULL
								AND (SELECT COUNT(*) FROM commit_repos ccp
									WHERE ccp.commit_id=commit_repos.commit_id) = 1
						;''')

				# set to null where twice true for is_orig_repo
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

				self.db.cursor.execute('''
						UPDATE commit_repos SET is_orig_repo=true
							WHERE is_orig_repo IS NULL
								AND (SELECT COUNT(*) FROM commit_repos ccp
									WHERE ccp.commit_id=commit_repos.commit_id) = 1
						;''')


				# update is_orig_repo to false for repos elsewhere in fork trees
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
				self.db.cursor.execute('''
						UPDATE commit_repos SET is_orig_repo=NULL;''')

				# update is_orig_repo to true for roots of fork trees
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

				self.db.cursor.execute('''
						UPDATE commit_repos SET is_orig_repo=true
							WHERE (SELECT COUNT(*) FROM commit_repos ccp
									WHERE ccp.commit_id=commit_repos.commit_id) = 1
						;''')

				# update is_orig_repo to false for repos elsewhere in fork trees
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


			self.db.cursor.execute('''
					SELECT r.owner,r.name,COUNT(*) FROM repositories r
						INNER JOIN commit_repos cr2
							ON r.id=cr2.repo_id AND cr2.is_orig_repo
						INNER JOIN
							(SELECT cr.commit_id,COUNT(DISTINCT cr.repo_id) AS cnt FROM commit_repos cr
								WHERE cr.is_orig_repo
								GROUP BY cr.commit_id
								HAVING COUNT(DISTINCT cr.repo_id)>1) AS c
							ON c.commit_id=cr2.commit_id
						GROUP BY r.owner,r.name
					;
					''')
			results = list(self.db.cursor.fetchall())
			if len(results):
				error_str = 'Several repos are considered origin repos of the same commits. Repos in this situation: {}, First ten: {}'.format(len(results),['{}/{}:{}'.format(*r) for r in results[:10]])
				raise ValueError(error_str)

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
