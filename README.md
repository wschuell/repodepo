# # REPODEPO (previously named repo_tools)

Set of classes and methods to collect information from selected github repositories, update them and track failures.

An example of usage can be found in the `repo_tools_example.py`.
Two main objects:
 - RepoDatabase, an interface to manage a database (sqlite or postgres) of the collected info
 - Fillers, objects that can be used to fill the database.
