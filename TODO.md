- [ ] on merge process of repos, detect potential forking_repo (assumption: the old name will never be used as a listed forking repo)
- [ ] check merge process of repo for no missing links to repo_id (new tables?)
- [ ] on is_orid_repo updates, check for repo_id updatesin commits table (eg if previously one was True, and is now supposed to be NULL; it should be updated)
- [ ] get rate limit in another way for github_rest fillers (saves one query per step)
- [ ] have filler from just url list, not packages
- [ ] github_gql fillers
  - [ ] endCursor values storing strategy (additional field in table_updates?)
  - [ ] sponsors
  - [ ] stars (for values > 40k)
- [ ] other elements
  - [ ] sponsors (user-user)
  - [ ] sponsors (user-repo)
  - [ ] issues
  - [ ] issue comments
  - [ ] PRs
  - [ ] PR comments
  - [ ] individual files + modifs per commit + class of file
- [ ] gitlab fillers
- [ ] improve list of identity merging fillers
  - [ ] based on email syntax
  - [ ] based on name + repo contributors
  - [ ] improve existing gh_login filler, one listing all commits of a given email (to be applied after the existing one, as it is more expensive computationally)
- [ ] bot users marking system
- [ ] update/postupdate checking fillers
	- [ ] check null forking_repo_id for presence in DB -> need relevant index?
	- [ ] check null follower_id for presence in DB
	- [ ] check null starrer_id for presence in DB
	- [ ] check null sponsor_id for presence in DB
- [ ] prefix all 'system' tables by an underscore (table updates, full updates, ...)