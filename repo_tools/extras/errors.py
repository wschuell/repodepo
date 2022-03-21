

class RepoToolsError(Exception):
	pass

class RepoToolsExportSameDBError(RepoToolsError):
	pass

class RepoToolsDumpSQLiteError(RepoToolsError):
	pass

class RepoToolsDumpPGError(RepoToolsError):
	pass
