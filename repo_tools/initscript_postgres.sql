
				CREATE TABLE IF NOT EXISTS _dbinfo(
				info_type TEXT PRIMARY KEY,
				info_content TEXT
				);

				CREATE TABLE IF NOT EXISTS sources(
				id BIGSERIAL PRIMARY KEY,
				name TEXT NOT NULL UNIQUE,
				url_root TEXT
				);

				CREATE TABLE IF NOT EXISTS _error_logs(
				id BIGSERIAL PRIMARY KEY,
				error TEXT,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);

				CREATE TABLE IF NOT EXISTS urls(
				id BIGSERIAL PRIMARY KEY,
				source BIGINT REFERENCES sources(id) ON DELETE CASCADE,
				source_root BIGINT REFERENCES sources(id) ON DELETE CASCADE,
				url TEXT NOT NULL UNIQUE,
				cleaned_url BIGINT REFERENCES urls(id) ON DELETE CASCADE,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);

				CREATE TABLE IF NOT EXISTS repositories(
				id BIGSERIAL PRIMARY KEY,
				source BIGINT REFERENCES sources(id) ON DELETE CASCADE,
				owner TEXT,
				name TEXT,
				url_id BIGINT REFERENCES urls(id) ON DELETE CASCADE,
				created_at TIMESTAMP,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				latest_commit_time TIMESTAMP DEFAULT NULL,
				cloned BOOLEAN DEFAULT false,
				UNIQUE(source,owner,name)
				);

				CREATE TABLE IF NOT EXISTS identity_types(
				id BIGSERIAL PRIMARY KEY,
				name TEXT UNIQUE
				);

				CREATE TABLE IF NOT EXISTS users(
				id BIGSERIAL PRIMARY KEY,
				creation_identity_type_id BIGINT REFERENCES identity_types(id) ON DELETE CASCADE,
				creation_identity TEXT,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				is_bot BOOLEAN DEFAULT false,
				UNIQUE(creation_identity_type_id,creation_identity)
				);

				CREATE TABLE IF NOT EXISTS identities(
				id BIGSERIAL PRIMARY KEY,
				identity_type_id BIGINT REFERENCES identity_types(id) ON DELETE CASCADE,
				user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
				identity TEXT,
				created_at TIMESTAMP,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				attributes JSONB,
				is_bot BOOLEAN DEFAULT false,
				UNIQUE(identity_type_id,identity)
				);


				CREATE INDEX IF NOT EXISTS identities_idx ON identities(identity);
				CREATE INDEX IF NOT EXISTS identity_users_idx ON identities(user_id);
				CREATE INDEX IF NOT EXISTS identity_isbot_idx ON identities(is_bot);

				CREATE TABLE IF NOT EXISTS merged_identities(
				id BIGSERIAL PRIMARY KEY,
				main_identity_id BIGINT NOT NULL REFERENCES identities(id) ON DELETE CASCADE,
				secondary_identity_id BIGINT NOT NULL REFERENCES identities(id) ON DELETE CASCADE,
				main_user_id BIGINT NOT NULL,
				secondary_user_id BIGINT NOT NULL,
				affected_identities BIGINT,
				reason TEXT,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);

				CREATE INDEX IF NOT EXISTS merged_id_idx1 ON merged_identities(main_identity_id);
				CREATE INDEX IF NOT EXISTS merged_id_idx2 ON merged_identities(secondary_identity_id);
				CREATE INDEX IF NOT EXISTS merged_id_idx3 ON merged_identities(main_user_id);
				CREATE INDEX IF NOT EXISTS merged_id_idx4 ON merged_identities(secondary_user_id);


				CREATE TABLE IF NOT EXISTS commits(
				id BIGSERIAL PRIMARY KEY,
				sha TEXT,
				author_id BIGINT REFERENCES identities(id) ON DELETE CASCADE,
				repo_id BIGINT REFERENCES repositories(id) ON DELETE CASCADE,
				created_at TIMESTAMP,
				original_created_at TIMESTAMP,
				insertions INT,
				deletions INT,
				UNIQUE(sha)
				);

				CREATE INDEX IF NOT EXISTS commits_ac_idx ON commits(author_id,created_at);
				CREATE INDEX IF NOT EXISTS commits_rc_idx ON commits(repo_id,created_at);
				CREATE INDEX IF NOT EXISTS commits_rc_idx ON commits(original_created_at) WHERE original_created_at IS NOT NULL;
				CREATE INDEX IF NOT EXISTS commits_cra_idx ON commits(created_at,repo_id,author_id);

				CREATE TABLE IF NOT EXISTS commit_repos(
				commit_id BIGINT REFERENCES commits(id) ON DELETE CASCADE,
				repo_id BIGINT REFERENCES repositories(id) ON DELETE CASCADE,
				is_orig_repo BOOLEAN DEFAULT NULL,
				PRIMARY KEY(commit_id,repo_id)
				);

				CREATE INDEX IF NOT EXISTS commit_repo_idx_rc ON commit_repos(repo_id,commit_id);
				CREATE INDEX IF NOT EXISTS commit_repo_idx_isorigrc ON commit_repos(repo_id,is_orig_repo);
				CREATE INDEX IF NOT EXISTS commit_repo_isorig_idx ON commit_repos(is_orig_repo,commit_id);

				CREATE TABLE IF NOT EXISTS commit_parents(
				child_id BIGINT REFERENCES commits(id) ON DELETE CASCADE,
				parent_id BIGINT REFERENCES commits(id) ON DELETE CASCADE,
				rank INT,
				PRIMARY KEY(child_id,parent_id),
				UNIQUE(parent_id,child_id,rank)
				);

				CREATE TABLE IF NOT EXISTS forks(
				forking_repo_id BIGINT REFERENCES repositories(id) ON DELETE CASCADE,
				forking_repo_url TEXT,
				forked_repo_id BIGINT NOT NULL REFERENCES repositories(id) ON DELETE CASCADE,
				forked_at TIMESTAMP DEFAULT NULL,
				fork_rank INTEGER DEFAULT 1,
				PRIMARY KEY(forking_repo_url,forked_repo_id)
				);
				CREATE INDEX IF NOT EXISTS forks_reverse_idx ON forks(forked_repo_id,forking_repo_id);
				CREATE INDEX IF NOT EXISTS forks_idx ON forks(forking_repo_id,forked_repo_id);

				CREATE TABLE IF NOT EXISTS table_updates(
				id BIGSERIAL PRIMARY KEY,
				repo_id BIGINT REFERENCES repositories(id) ON DELETE CASCADE,
				identity_id BIGINT REFERENCES identities(id) ON DELETE CASCADE,
				table_name TEXT,
				success BOOLEAN DEFAULT true,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				latest_commit_time TIMESTAMP DEFAULT NULL,
				info JSONB
				);

				CREATE INDEX IF NOT EXISTS table_updates_idx ON table_updates(repo_id,table_name,updated_at);
				CREATE INDEX IF NOT EXISTS table_updates_idx2 ON table_updates(table_name,success,updated_at);
				CREATE INDEX IF NOT EXISTS table_updates_idx3 ON table_updates(table_name,updated_at);
				CREATE INDEX IF NOT EXISTS table_updates_idx4 ON table_updates(table_name,success,repo_id,identity_id);
				CREATE INDEX IF NOT EXISTS table_updates_idx5 ON table_updates(table_name,success,identity_id,repo_id);
				CREATE INDEX IF NOT EXISTS table_updates_idx6 ON table_updates(table_name,identity_id);
				CREATE INDEX IF NOT EXISTS table_updates_idx7 ON table_updates(table_name,repo_id);
				CREATE INDEX IF NOT EXISTS table_updates_identity_idx ON table_updates(identity_id,table_name,updated_at);

				CREATE TABLE IF NOT EXISTS full_updates(
				update_type TEXT,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);


				CREATE TABLE IF NOT EXISTS stars(
				repo_id BIGINT REFERENCES repositories(id) ON DELETE CASCADE,
				identity_type_id BIGINT REFERENCES identity_types(id) ON DELETE CASCADE,
				login TEXT NOT NULL,
				identity_id BIGINT REFERENCES identities(id) ON DELETE CASCADE,
				starred_at TIMESTAMP NOT NULL,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY(repo_id,login,identity_type_id)
				);

				CREATE INDEX IF NOT EXISTS stars_idx ON stars(repo_id,starred_at);
				CREATE INDEX IF NOT EXISTS stars_idx2 ON stars(repo_id,created_at);
				CREATE INDEX IF NOT EXISTS stars_idx3 ON stars(identity_id,starred_at);

				CREATE TABLE IF NOT EXISTS followers(
				follower_identity_type_id BIGINT REFERENCES identity_types(id) ON DELETE CASCADE,
				follower_login TEXT,
				follower_id BIGINT REFERENCES identities(id) ON DELETE CASCADE,
				followee_id BIGINT REFERENCES identities(id) ON DELETE CASCADE,
				created_at TIMESTAMP DEFAULT NULL,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY(followee_id,follower_identity_type_id,follower_login)
				);

				CREATE INDEX IF NOT EXISTS followers_idx ON followers(follower_id,followee_id);


				CREATE TABLE IF NOT EXISTS packages(
				id BIGSERIAL PRIMARY KEY,
				source_id BIGINT REFERENCES sources(id) ON DELETE CASCADE,
				insource_id BIGINT DEFAULT NULL,
				name TEXT,
				url_id BIGINT REFERENCES urls(id) ON DELETE CASCADE,
				repo_id BIGINT REFERENCES repositories(id) ON DELETE CASCADE,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				created_at TIMESTAMP DEFAULT NULL,
				UNIQUE(source_id,insource_id)
				);

				CREATE INDEX IF NOT EXISTS packages_idx ON packages(source_id,name);
				CREATE INDEX IF NOT EXISTS packages_date_idx ON packages(created_at);
				CREATE INDEX IF NOT EXISTS packages_repo_idx ON packages(repo_id);

				CREATE TABLE IF NOT EXISTS merged_repositories(
				id BIGSERIAL PRIMARY KEY,
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
				id BIGSERIAL PRIMARY KEY, -- using a generated primary key because external_id or sponsor_login could be NULL depending on how sponsorship data is retrieved
				repo_id BIGINT REFERENCES repositories(id) ON DELETE CASCADE,
				sponsor_identity_type_id BIGINT REFERENCES identity_types(id) ON DELETE CASCADE,
				sponsor_login TEXT,
				sponsor_id BIGINT REFERENCES identities(id) ON DELETE CASCADE,
				created_at TIMESTAMP NOT NULL,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				is_onetime_payment BOOLEAN DEFAULT false,
				external_id TEXT UNIQUE,
				tier JSONB
				);

				CREATE INDEX IF NOT EXISTS sponsors_repo_idx ON sponsors_repo(repo_id,created_at,sponsor_login,sponsor_id);
				CREATE INDEX IF NOT EXISTS sponsors_repo_idx2 ON sponsors_repo(sponsor_id,created_at);

				CREATE TABLE IF NOT EXISTS sponsors_user(
				id BIGSERIAL PRIMARY KEY,
				sponsored_id BIGINT REFERENCES identities(id) ON DELETE CASCADE,
				sponsor_identity_type_id BIGINT REFERENCES identity_types(id) ON DELETE CASCADE,
				sponsor_id BIGINT REFERENCES identities(id) ON DELETE CASCADE,
				sponsor_login TEXT,
				created_at TIMESTAMP NOT NULL,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				is_onetime_payment BOOLEAN DEFAULT false,
				external_id TEXT UNIQUE,
				tier JSONB
				);

				CREATE INDEX IF NOT EXISTS sponsors_user_idx ON sponsors_user(sponsored_id,created_at,sponsor_login,sponsor_id);
				CREATE INDEX IF NOT EXISTS sponsors_user_idx2 ON sponsors_user(sponsor_id,created_at);

				CREATE TABLE IF NOT EXISTS sponsors_listings(
				id BIGSERIAL PRIMARY KEY,
				login TEXT NOT NULL,
				identity_type_id BIGINT REFERENCES identity_types(id) ON DELETE CASCADE,
				created_at TIMESTAMP NOT NULL,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				UNIQUE(identity_type_id,login,created_at)
				);
				CREATE INDEX IF NOT EXISTS sponsors_listings_idx ON sponsors_listings(created_at);

				CREATE TABLE IF NOT EXISTS releases(
				id BIGSERIAL PRIMARY KEY,
				repo_id BIGINT REFERENCES repositories(id) ON DELETE CASCADE,
				name TEXT,
				tag_name TEXT,
				created_at TIMESTAMP DEFAULT NULL,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);


				CREATE TABLE IF NOT EXISTS issues(
				id BIGSERIAL PRIMARY KEY,
				repo_id BIGINT REFERENCES repositories(id) ON DELETE CASCADE,
				issue_number TEXT,
				issue_title TEXT,
				created_at TIMESTAMP DEFAULT NULL,
				closed_at TIMESTAMP DEFAULT NULL,
				inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);

				CREATE TABLE IF NOT EXISTS package_versions(
				id BIGSERIAL PRIMARY KEY,
				package_id BIGINT NOT NULL REFERENCES packages(id) ON DELETE CASCADE,
				version_str TEXT,
				created_at TIMESTAMP
				);

				CREATE INDEX IF NOT EXISTS package_versions_idx ON package_versions(package_id,created_at);
				CREATE UNIQUE INDEX IF NOT EXISTS package_versions_str_idx ON package_versions(package_id,version_str);
				CREATE INDEX IF NOT EXISTS package_versions_date_idx ON package_versions(created_at);

				CREATE TABLE IF NOT EXISTS package_dependencies(
				depending_version BIGINT NOT NULL REFERENCES package_versions(id) ON DELETE CASCADE,
				depending_on_package BIGINT NOT NULL REFERENCES packages(id) ON DELETE CASCADE,
				semver_str TEXT,
				PRIMARY KEY(depending_version,depending_on_package)
				);

				CREATE INDEX IF NOT EXISTS reverse_package_deps_idx ON package_dependencies(depending_on_package,depending_version);

				CREATE TABLE IF NOT EXISTS package_version_downloads(
				downloaded_at DATE,
				downloads INT DEFAULT 1,
				package_version BIGINT NOT NULL REFERENCES package_versions(id) ON DELETE CASCADE,
				PRIMARY KEY(package_version,downloaded_at)
				);

				CREATE INDEX IF NOT EXISTS package_version_downloads_date_idx ON package_version_downloads(downloaded_at);

				CREATE TABLE IF NOT EXISTS repo_languages(
					repo_id BIGSERIAL REFERENCES repositories(id) ON DELETE CASCADE,
					language TEXT NOT NULL,
					size INT,
					share DOUBLE PRECISION,
					queried_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
					PRIMARY KEY(repo_id,language)
					);

				CREATE INDEX IF NOT EXISTS lang_idx ON repo_languages(language,repo_id);

				CREATE TABLE IF NOT EXISTS user_languages(
					user_identity BIGSERIAL REFERENCES identities(id) ON DELETE CASCADE,
					language TEXT NOT NULL,
					size DOUBLE PRECISION,
					share DOUBLE PRECISION,
					queried_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
					PRIMARY KEY(user_identity,language)
					);

				CREATE INDEX IF NOT EXISTS lang_id_idx ON user_languages(language,user_identity);

				CREATE TABLE IF NOT EXISTS filtered_deps_package(
					package_id BIGINT PRIMARY KEY REFERENCES packages(id) ON DELETE CASCADE,
					reason TEXT,
					created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
					);

				CREATE TABLE IF NOT EXISTS filtered_deps_repo(
					repo_id BIGINT PRIMARY KEY REFERENCES repositories(id) ON DELETE CASCADE,
					reason TEXT,
					created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
					);

				CREATE TABLE IF NOT EXISTS filtered_deps_repoedges(
					repo_source_id BIGINT REFERENCES repositories(id) ON DELETE CASCADE,
					repo_dest_id BIGINT REFERENCES repositories(id) ON DELETE CASCADE,
					reason TEXT,
					created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
					PRIMARY KEY(repo_source_id,repo_dest_id)
					);
				CREATE INDEX IF NOT EXISTS reverse_fdre ON filtered_deps_repoedges(repo_dest_id,repo_source_id);

				CREATE TABLE IF NOT EXISTS filtered_deps_packageedges(
					package_source_id BIGINT REFERENCES packages(id) ON DELETE CASCADE,
					package_dest_id BIGINT REFERENCES packages(id) ON DELETE CASCADE,
					reason TEXT,
					created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
					PRIMARY KEY(package_source_id,package_dest_id)
					);
				CREATE INDEX IF NOT EXISTS reverse_fdpe ON filtered_deps_packageedges(package_dest_id,package_source_id);
