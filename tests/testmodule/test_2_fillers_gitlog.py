import repodepo
from repodepo.fillers import (
    generic,
    commit_info,
    commit_info_new,
    github_rest,
    meta_fillers,
    bot_fillers,
    deps_filters_fillers,
    metacran,
    npm,
)
import pytest
import datetime
import time
import os

#### Parameters
dbtype_list = ["sqlite", "postgres"]


@pytest.fixture(params=dbtype_list)
def dbtype(request):
    return request.param


workers_list = [
    1,
]
# workers_list = [1, 2, None]


@pytest.fixture(params=workers_list)
def workers_count(request):
    return request.param


@pytest.fixture(params=dbtype_list)
def testdb(request):
    db = repodepo.repo_database.Database(
        db_name="travis_ci_test_repo_tools",
        db_type=request.param,
        data_folder="dummy_clones",
    )
    db.clean_db()
    db.init_db()
    yield db
    db.connection.close()
    del db


workers = 5

##############

#### Tests


# @pytest.mark.timeout(300)
# def test_commits(testdb, workers_count):
#     testdb.add_filler(
#         generic.SourcesFiller(
#             source=[
#                 "GitHub",
#             ],
#             source_urlroot=[
#                 "github.com",
#             ],
#         )
#     )
#     testdb.add_filler(
#         generic.PackageFiller(
#             package_list_file="packages.csv",
#             data_folder=os.path.join(os.path.dirname(__file__), "dummy_data"),
#         )
#     )
#     testdb.add_filler(generic.RepositoriesFiller())
#     testdb.add_filler(generic.ClonesFiller(data_folder="dummy_clones"))
#     testdb.add_filler(
#         commit_info_new.CommitsGitLogFiller(
#             data_folder="dummy_clones", workers=workers_count
#         )
#     )
#     testdb.fill_db()
#     testdb.connection.commit()


@pytest.mark.timeout(300)
def test_commits_sequential(testdb, workers_count):
    testdb.add_filler(
        generic.SourcesFiller(
            source=[
                "GitHub",
            ],
            source_urlroot=[
                "github.com",
            ],
        )
    )
    testdb.add_filler(
        generic.PackageFiller(
            package_list_file="packages.csv",
            data_folder=os.path.join(os.path.dirname(__file__), "dummy_data"),
        )
    )
    testdb.add_filler(generic.RepositoriesFiller())
    # testdb.add_filler(generic.ClonesFiller(data_folder="dummy_clones"))
    testdb.add_filler(
        commit_info_new.CommitsGitLogFiller(
            data_folder="dummy_clones",
            workers=workers_count,
            sequential=True,
            clone_absent=True,
            clean_repo=True,
            temp_repodir=True,
        )
    )
    testdb.fill_db()
    testdb.connection.commit()
