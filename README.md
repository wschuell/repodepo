# REPO TOOLS

Set of classes and methods to collect information from selected github repositories, update them and track failures.

An example of usage can be found in the `repo_tools_example.py`.
Two main objects:
 - RepoDatabase, an interface to manage a database (sqlite or postgres) of the collected info
 - RepoCrawler, an interface to crawl/collect the information, for example from github.
