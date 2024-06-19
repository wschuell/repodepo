import repodepo
from repodepo.fillers import (
    generic,
    commit_info,
    github_gql,
    meta_fillers,
    github_rest,
    snowball,
)
import pytest
import datetime
import time
import os
import subprocess

#### Parameters
dbtype_list = ["sqlite", "postgres"]


@pytest.fixture(params=dbtype_list)
def dbtype(request):
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


workers = 1

##############

#### Tests


@pytest.mark.timeout(300)
def test_contribs_gql(testdb):
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
        generic.IdentitiesFiller(
            identities_list=["wschuell", "hmludwig", "JanaLasser", "SimoneDaniotti"],
            identity_type="github_login",
            data_folder=os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "dummy_data"
            ),
        )
    )
    testdb.add_filler(
        github_gql.UserAllContribsGQLFiller(
            extra_args=dict(
                fail_on_wait=True,
                workers=workers,
                secondary_page_size=10,
                start="2023-01-01",
                end="2024-01-01",
            )
        )
    )
    testdb.fill_db()
