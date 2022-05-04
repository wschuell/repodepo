
				CREATE TABLE IF NOT EXISTS _dbinfo(
				info_type TEXT PRIMARY KEY,
				info_content TEXT
				);

				CREATE TABLE IF NOT EXISTS sources(
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL UNIQUE,
				url_root TEXT
				);

				CREATE TABLE IF NOT EXISTS _error_logs(
				id INTEGER PRIMARY KEY,
				error TEXT,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);

				CREATE TABLE IF NOT EXISTS urls(
				id INTEGER PRIMARY KEY,
				source INTEGER REFERENCES sources(id) ON DELETE CASCADE,
				source_root INTEGER REFERENCES sources(id) ON DELETE CASCADE,
				url TEXT NOT NULL UNIQUE,
				cleaned_url INTEGER REFERENCES urls(id) ON DELETE CASCADE,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);

				CREATE TABLE IF NOT EXISTS repositories(
				id INTEGER PRIMARY KEY,
				source INTEGER REFERENCES sources(id) ON DELETE CASCADE,
				owner TEXT,
				name TEXT,
				url_id INTEGER REFERENCES urls(id) ON DELETE CASCADE,
				created_at TIMESTAMP DEFAULT NULL,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				latest_commit_time TIMESTAMP DEFAULT NULL,
				cloned BOOLEAN DEFAULT 0,
				UNIQUE(source,owner,name)
				);

				CREATE TABLE IF NOT EXISTS identity_types(
				id INTEGER PRIMARY KEY,
				name TEXT UNIQUE
				);

				CREATE TABLE IF NOT EXISTS users(
				id INTEGER PRIMARY KEY,
				creation_identity_type_id INTEGER REFERENCES identity_types(id) ON DELETE CASCADE,
				creation_identity TEXT,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				is_bot BOOLEAN DEFAULT false,
				UNIQUE(creation_identity_type_id,creation_identity)
				);
				CREATE INDEX IF NOT EXISTS users_isbot_idx ON users(is_bot);


				CREATE TABLE IF NOT EXISTS identities(
				id INTEGER PRIMARY KEY,
				identity_type_id INTEGER REFERENCES identity_types(id) ON DELETE CASCADE,
				user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
				identity TEXT,
				created_at TIMESTAMP,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				attributes TEXT,
				is_bot BOOLEAN DEFAULT false,
				UNIQUE(identity_type_id,identity)
				);

				CREATE INDEX IF NOT EXISTS identities_idx ON identities(identity);
				CREATE INDEX IF NOT EXISTS identity_users_idx ON identities(user_id);
				CREATE INDEX IF NOT EXISTS identity_isbot_idx ON identities(is_bot);

				CREATE TABLE IF NOT EXISTS merged_identities(
				id INTEGER PRIMARY KEY,
				main_identity_id INTEGER NOT NULL REFERENCES identities(id) ON DELETE CASCADE,
				secondary_identity_id INTEGER NOT NULL REFERENCES identities(id) ON DELETE CASCADE,
				main_user_id INTEGER NOT NULL,
				secondary_user_id INTEGER NOT NULL,
				affected_identities INTEGER,
				reason TEXT,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);

				CREATE INDEX IF NOT EXISTS merged_id_idx1 ON merged_identities(main_identity_id);
				CREATE INDEX IF NOT EXISTS merged_id_idx2 ON merged_identities(secondary_identity_id);
				CREATE INDEX IF NOT EXISTS merged_id_idx3 ON merged_identities(main_user_id);
				CREATE INDEX IF NOT EXISTS merged_id_idx4 ON merged_identities(secondary_user_id);

				CREATE TABLE IF NOT EXISTS commits(
				id INTEGER PRIMARY KEY,
				sha TEXT,
				author_id INTEGER REFERENCES identities(id) ON DELETE CASCADE,
				repo_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
				created_at TIMESTAMP,
				original_created_at TIMESTAMP,
				insertions INTEGER,
				deletions INTEGER,
				UNIQUE(sha)
				);

				CREATE INDEX IF NOT EXISTS commits_ac_idx ON commits(author_id,created_at);
				CREATE INDEX IF NOT EXISTS commits_rc_idx ON commits(repo_id,created_at);
				CREATE INDEX IF NOT EXISTS commits_rc_idx ON commits(original_created_at) WHERE original_created_at IS NOT NULL;
				CREATE INDEX IF NOT EXISTS commits_cra_idx ON commits(created_at,repo_id,author_id);

				CREATE TABLE IF NOT EXISTS commit_repos(
				commit_id INTEGER REFERENCES commits(id) ON DELETE CASCADE,
				repo_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
				is_orig_repo BOOLEAN DEFAULT NULL,
				PRIMARY KEY(commit_id,repo_id)
				);

				CREATE INDEX IF NOT EXISTS commit_repo_idx_rc ON commit_repos(repo_id,commit_id);
				CREATE INDEX IF NOT EXISTS commit_repo_idx_isorigrc ON commit_repos(repo_id,is_orig_repo);
				CREATE INDEX IF NOT EXISTS commit_repo_isorig_idx ON commit_repos(is_orig_repo,commit_id);

				CREATE TABLE IF NOT EXISTS commit_parents(
				child_id INTEGER REFERENCES commits(id) ON DELETE CASCADE,
				parent_id INTEGER REFERENCES commits(id) ON DELETE CASCADE,
				rank INTEGER,
				PRIMARY KEY(child_id,parent_id),
				UNIQUE(parent_id,child_id,rank)
				);

				CREATE TABLE IF NOT EXISTS forks(
				forking_repo_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
				forking_repo_url TEXT,
				forked_repo_id INTEGER NOT NULL REFERENCES repositories(id) ON DELETE CASCADE,
				forked_at TIMESTAMP DEFAULT NULL,
				fork_rank INTEGER DEFAULT 1,
				PRIMARY KEY(forking_repo_url,forked_repo_id)
				);
				CREATE INDEX IF NOT EXISTS forks_reverse_idx ON forks(forked_repo_id,forking_repo_id);
				CREATE INDEX IF NOT EXISTS forks_idx ON forks(forking_repo_id,forked_repo_id);

				CREATE TABLE IF NOT EXISTS table_updates(
				id INTEGER PRIMARY KEY,
				repo_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
				identity_id INTEGER REFERENCES identities(id) ON DELETE CASCADE,
				table_name TEXT,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				success BOOLEAN DEFAULT 1,
				latest_commit_time TIMESTAMP DEFAULT NULL,
				info TEXT
				);

				CREATE INDEX IF NOT EXISTS table_updates_idx ON table_updates(repo_id,table_name,updated_at);
				CREATE INDEX IF NOT EXISTS table_updates_idx2 ON table_updates(table_name,success,updated_at);
				CREATE INDEX IF NOT EXISTS table_updates_idx3 ON table_updates(table_name,updated_at);
				CREATE INDEX IF NOT EXISTS table_updates_idx4 ON table_updates(table_name,success,repo_id,identity_id);
				CREATE INDEX IF NOT EXISTS table_updates_identity_idx ON table_updates(identity_id,table_name,updated_at);

				CREATE TABLE IF NOT EXISTS full_updates(
				update_type TEXT,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);

				CREATE TABLE IF NOT EXISTS followers(
				follower_identity_type_id INTEGER REFERENCES identity_types(id) ON DELETE CASCADE,
				follower_login TEXT,
				follower_id INTEGER REFERENCES identities(id) ON DELETE CASCADE,
				followee_id INTEGER REFERENCES identities(id) ON DELETE CASCADE,
				created_at TIMESTAMP DEFAULT NULL,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY(followee_id,follower_identity_type_id,follower_login)
				);

				CREATE INDEX IF NOT EXISTS followers_idx ON followers(follower_id,followee_id);

				CREATE TABLE IF NOT EXISTS stars(
				repo_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
				identity_type_id INTEGER REFERENCES identity_types(id) ON DELETE CASCADE,
				login TEXT NOT NULL,
				identity_id INTEGER REFERENCES identities(id) ON DELETE CASCADE,
				starred_at TIMESTAMP NOT NULL,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY(repo_id,login,identity_type_id)
				);

				CREATE INDEX IF NOT EXISTS stars_idx ON stars(repo_id,starred_at);
				CREATE INDEX IF NOT EXISTS stars_idx2 ON stars(repo_id,created_at);
				CREATE INDEX IF NOT EXISTS stars_idx3 ON stars(identity_id,starred_at);

				CREATE TABLE IF NOT EXISTS packages(
				id INTEGER PRIMARY KEY,
				source_id INTEGER REFERENCES sources(id) ON DELETE CASCADE,
				insource_id INTEGER DEFAULT NULL,
				name TEXT,
				url_id INTEGER REFERENCES urls(id) ON DELETE CASCADE,
				repo_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				created_at TIMESTAMP DEFAULT NULL,
				UNIQUE(source_id,insource_id)
				);

				CREATE INDEX IF NOT EXISTS packages_idx ON packages(source_id,name);
				CREATE INDEX IF NOT EXISTS packages_date_idx ON packages(created_at);
				CREATE INDEX IF NOT EXISTS packages_repo_idx ON packages(repo_id);

				CREATE TABLE IF NOT EXISTS merged_repositories(
				id INTEGER PRIMARY KEY,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				merged_at TIMESTAMP DEFAULT NULL,
				new_id TEXT,
				new_source TEXT,
				new_owner TEXT,
				new_name TEXT,
				obsolete_id TEXT,
				obsolete_source TEXT,
				obsolete_owner TEXT,
				obsolete_name TEXT,
				merging_reason_source TEXT
				);
				CREATE INDEX IF NOT EXISTS mergerepo_idx ON merged_repositories(merged_at);

				CREATE TABLE IF NOT EXISTS sponsors_repo(
				id INTEGER PRIMARY KEY,
				repo_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
				sponsor_identity_type_id INTEGER REFERENCES identity_types(id) ON DELETE CASCADE,
				sponsor_login TEXT,
				sponsor_id INTEGER REFERENCES identities(id) ON DELETE CASCADE,
				created_at TIMESTAMP NOT NULL,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				is_onetime_payment BOOLEAN DEFAULT false,
				external_id TEXT UNIQUE,
				tier TEXT
				);

				CREATE INDEX IF NOT EXISTS sponsors_repo_idx ON sponsors_repo(repo_id,created_at,sponsor_login,sponsor_id);
				CREATE INDEX IF NOT EXISTS sponsors_repo_idx2 ON sponsors_repo(sponsor_id,created_at);

				CREATE TABLE IF NOT EXISTS sponsors_user(
				id INTEGER PRIMARY KEY,
				sponsored_id INTEGER REFERENCES identities(id) ON DELETE CASCADE,
				sponsor_identity_type_id INTEGER REFERENCES identity_types(id) ON DELETE CASCADE,
				sponsor_id INTEGER REFERENCES identities(id) ON DELETE CASCADE,
				sponsor_login TEXT,
				created_at TIMESTAMP NOT NULL,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				is_onetime_payment BOOLEAN DEFAULT false,
				external_id TEXT UNIQUE,
				tier TEXT
				);

				CREATE INDEX IF NOT EXISTS sponsors_user_idx ON sponsors_user(sponsored_id,created_at,sponsor_login,sponsor_id);
				CREATE INDEX IF NOT EXISTS sponsors_user_idx2 ON sponsors_user(sponsor_id,created_at);

				CREATE TABLE IF NOT EXISTS sponsors_listings(
				id INTEGER PRIMARY KEY,
				login TEXT NOT NULL,
				identity_type_id INTEGER REFERENCES identity_types(id) ON DELETE CASCADE,
				created_at TIMESTAMP NOT NULL,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				UNIQUE(identity_type_id,login,created_at)
				);
				CREATE INDEX IF NOT EXISTS sponsors_listings_idx ON sponsors_listings(created_at);

				CREATE TABLE IF NOT EXISTS releases(
				id INTEGER PRIMARY KEY,
				repo_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
				name TEXT,
				tag_name TEXT,
				created_at TIMESTAMP DEFAULT NULL,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);


				CREATE TABLE IF NOT EXISTS issues(
				id INTEGER PRIMARY KEY,
				repo_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
				issue_number TEXT,
				issue_title TEXT,
				created_at TIMESTAMP DEFAULT NULL,
				closed_at TIMESTAMP DEFAULT NULL,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);

				CREATE TABLE IF NOT EXISTS package_versions(
				id INTEGER PRIMARY KEY,
				package_id INTEGER NOT NULL REFERENCES packages(id) ON DELETE CASCADE,
				version_str TEXT,
				created_at TIMESTAMP
				);

				CREATE INDEX IF NOT EXISTS package_versions_idx ON package_versions(package_id,created_at);
				CREATE UNIQUE INDEX IF NOT EXISTS package_versions_str_idx ON package_versions(package_id,version_str);
				CREATE INDEX IF NOT EXISTS package_versions_date_idx ON package_versions(created_at);

				CREATE TABLE IF NOT EXISTS package_dependencies(
				depending_version INTEGER NOT NULL REFERENCES package_versions(id) ON DELETE CASCADE,
				depending_on_package INTEGER NOT NULL REFERENCES packages(id) ON DELETE CASCADE,
				semver_str TEXT,
				PRIMARY KEY(depending_version,depending_on_package)
				);

				CREATE INDEX IF NOT EXISTS reverse_package_deps_idx ON package_dependencies(depending_on_package,depending_version);

				CREATE TABLE IF NOT EXISTS package_version_downloads(
				downloaded_at DATE,
				downloads INTEGER DEFAULT 1,
				package_version INTEGER NOT NULL REFERENCES package_versions(id) ON DELETE CASCADE,
				PRIMARY KEY(package_version,downloaded_at)
				);

				CREATE INDEX IF NOT EXISTS package_version_downloads_date_idx ON package_version_downloads(downloaded_at);

				CREATE TABLE IF NOT EXISTS repo_languages(
					repo_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
					language TEXT NOT NULL,
					size INTEGER,
					share REAL,
					queried_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
					PRIMARY KEY(repo_id,language)
					);

				CREATE INDEX IF NOT EXISTS lang_idx ON repo_languages(language,repo_id);

				CREATE TABLE IF NOT EXISTS user_languages(
					user_identity INTEGER REFERENCES identities(id) ON DELETE CASCADE,
					language TEXT NOT NULL,
					size REAL,
					share REAL,
					queried_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
					PRIMARY KEY(user_identity,language)
					);

				CREATE INDEX IF NOT EXISTS lang_id_idx ON user_languages(language,user_identity);

				CREATE TABLE IF NOT EXISTS filtered_deps_package(
					package_id INTEGER PRIMARY KEY REFERENCES packages(id) ON DELETE CASCADE,
					reason TEXT,
					created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
					);

				CREATE TABLE IF NOT EXISTS filtered_deps_repo(
					repo_id INTEGER PRIMARY KEY REFERENCES repositories(id) ON DELETE CASCADE,
					reason TEXT,
					created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
					);

				CREATE TABLE IF NOT EXISTS filtered_deps_repoedges(
					repo_source_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
					repo_dest_id INTEGER REFERENCES repositories(id) ON DELETE CASCADE,
					reason TEXT,
					created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
					PRIMARY KEY(repo_source_id,repo_dest_id)
					);
				CREATE INDEX IF NOT EXISTS reverse_fdre ON filtered_deps_repoedges(repo_dest_id,repo_source_id);

				CREATE TABLE IF NOT EXISTS filtered_deps_packageedges(
					package_source_id INTEGER REFERENCES packages(id) ON DELETE CASCADE,
					package_dest_id INTEGER REFERENCES packages(id) ON DELETE CASCADE,
					reason TEXT,
					created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
					PRIMARY KEY(package_source_id,package_dest_id)
					);
				CREATE INDEX IF NOT EXISTS reverse_fdpe ON filtered_deps_packageedges(package_dest_id,package_source_id);

