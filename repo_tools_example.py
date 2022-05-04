
import repodepo as rd
from repodepo.fillers import generic,commit_info,github_rest,meta_fillers,github_gql
import pytest
import datetime
import time
import os

testdb = rd.repo_database.Database(db_name='testdb_repotools',db_type='sqlite',data_folder='dummy_clones')
testdb.init_db()

print('Make sure you have a github API key (with permission read:user for GraphQL) in $HOME/.repo_tools/github_api_keys.txt. Continuing in 3s.')
time.sleep(3)

workers = 3 # Number of parallel threads for querying the github APIs

testdb.add_filler(generic.SourcesFiller(source=['GitHub',],source_urlroot=['github.com',]))
testdb.add_filler(generic.PackageFiller(package_list_file='packages.csv',data_folder=os.path.join(os.path.dirname(os.path.dirname(rd.__file__)),'tests','testmodule','dummy_data')))
testdb.add_filler(generic.RepositoriesFiller()) # Parses the URLs of the packages to attribute them to the available sources (here only github.com)
testdb.add_filler(github_rest.ForksFiller(fail_on_wait=True,workers=workers))
testdb.add_filler(generic.ClonesFiller(data_folder='dummy_clones')) # Clones after forks to have up-to-date repo URLS (detect redirects)
testdb.add_filler(commit_info.CommitsFiller(data_folder='dummy_clones')) # Commits after forks because fork info needed for repo commit ownership ran at the end.
testdb.add_filler(generic.RepoCommitOwnershipFiller()) # associating repositories as owners of commits (for those who could not be disambiguated using forks) based on creation date of associated package
testdb.add_filler(github_rest.GHLoginsFiller(fail_on_wait=True,workers=workers)) # Disambiguating emails by associating them to their GH logins.
testdb.add_filler(github_gql.StarsGQLFiller(fail_on_wait=True,workers=workers))
testdb.add_filler(github_gql.FollowersGQLFiller(fail_on_wait=True,workers=workers))
testdb.add_filler(github_gql.SponsorsUserFiller(fail_on_wait=True,workers=workers))
testdb.fill_db()
