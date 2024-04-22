from .. import fillers
from ..fillers import generic, github_rest

import os
import gzip
import json
from psycopg2 import extras

# with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),'pwc_initscript.sql'),'r') as f:
#     INITDB_ADDITIONAL = f.read()

INITDB_ADDITIONAL = dict(
    sqlite="""
    CREATE TABLE IF NOT EXISTS scientific_papers(
        id INTEGER PRIMARY KEY,
        paper_url TEXT UNIQUE,
        arxiv_id TEXT UNIQUE,
        title TEXT,
        abstract TEXT,
        url_abs TEXT,
        url_pdf TEXT,
        proceeding TEXT,
        date DATE
        );
    CREATE TABLE IF NOT EXISTS scientific_papers_authors(
        author_id INTEGER REFERENCES identities(id) ON DELETE CASCADE,
        author_rank INTEGER,
        paper_id INTEGER REFERENCES scientific_papers ON DELETE CASCADE,
        PRIMARY KEY(author_id,paper_id)
        );
    CREATE INDEX IF NOT EXISTS sp_authors_reverse_idx ON scientific_papers_authors(paper_id,author_id);

    CREATE TABLE IF NOT EXISTS scientific_papers_repo_links(
        paper_id INTEGER REFERENCES scientific_papers ON DELETE CASCADE,
        url_id INTEGER REFERENCES urls ON DELETE CASCADE,
        is_official BOOLEAN,
        mentioned_in_paper BOOLEAN,
        mentioned_in_github BOOLEAN,
        framework TEXT,
        PRIMARY KEY(paper_id,url_id)
        );

    """,
    postgres="""
    
    CREATE TABLE IF NOT EXISTS scientific_papers(
        id BIGSERIAL PRIMARY KEY,
        paper_url TEXT UNIQUE,
        arxiv_id TEXT UNIQUE,
        title TEXT,
        abstract TEXT,
        url_abs TEXT,
        url_pdf TEXT,
        proceeding TEXT,
        date DATE
        );
    CREATE TABLE IF NOT EXISTS scientific_papers_authors(
        author_id BIGINT REFERENCES identities(id) ON DELETE CASCADE,
        author_rank INT,
        paper_id BIGINT REFERENCES scientific_papers ON DELETE CASCADE,
        PRIMARY KEY(author_id,paper_id)
        );
    CREATE INDEX IF NOT EXISTS sp_authors_reverse_idx ON scientific_papers_authors(paper_id,author_id);

    CREATE TABLE IF NOT EXISTS scientific_papers_repo_links(
        paper_id BIGINT REFERENCES scientific_papers ON DELETE CASCADE,
        url_id BIGINT REFERENCES urls ON DELETE CASCADE,
        is_official BOOLEAN,
        mentioned_in_paper BOOLEAN,
        mentioned_in_github BOOLEAN,
        framework TEXT,
        PRIMARY KEY(paper_id,url_id)
        );
    """,
)


class PWCFiller(fillers.Filler):
    """
    Gathering data from papers with code
    """

    def __init__(
        self,
        source="paperswithcode",
        item_limit=None,
        force=False,
        main_url="https://production-media.paperswithcode.com/about",
        **kwargs,
    ):
        self.source = source
        self.item_limit = item_limit
        self.force = force
        self.main_url = main_url
        fillers.Filler.__init__(self, **kwargs)

    def prepare(self):
        if self.data_folder is None:
            self.data_folder = self.db.data_folder
        for filename in [
            "papers-with-abstracts",
            "links-between-papers-and-code",
            "evaluation-tables",
            "methods",
            "datasets",
        ]:
            filepath = os.path.join(
                self.data_folder, "paperswithcode", f"{filename}.json.gz"
            )
            self.download(
                f"{self.main_url}/{filename}.json.gz",
                destination=filepath,
                force=self.force,
            )

        for s in INITDB_ADDITIONAL[self.db.db_type].split(";"):
            if len(s.strip("\n\t \r")):
                self.db.cursor.execute(s)
        self.db.connection.commit()

    def apply(self):
        for filename in [
            "papers-with-abstracts",
            "links-between-papers-and-code",
            # "evaluation-tables",
            # "methods",
            # "datasets",
        ]:
            prefix = filename.split("""-""")[0]
            filepath = os.path.join(
                self.data_folder, "paperswithcode", f"{filename}.json.gz"
            )
            with gzip.open(filepath, "rt") as gf:
                if hasattr(self, "parse_" + prefix):
                    data = getattr(self, "parse_" + prefix)(data=json.load(gf))
                else:
                    data = json.load(gf)
                getattr(self, f"fill_{prefix}")(data=data)

    def parse_papers(self, data):
        ans = []
        for d in data:
            if d["date"] == "":
                d["date"] = None
            ans.append(d)
        return ans

    def fill_papers(self, data):
        if self.db.db_type == "sqlite":
            self.db.cursor.executemany(
                """
                INSERT INTO scientific_papers(paper_url,
                                arxiv_id,
                                title,
                                abstract,
                                url_abs,
                                url_pdf,
                                proceeding,
                                date)
                    VALUES (
                                :paper_url,
                                :arxiv_id,
                                :title,
                                :abstract,
                                :url_abs,
                                :url_pdf,
                                :proceeding,
                                :date
                                )
                ON CONFLICT DO NOTHING
                ;""",
                data,
            )
        else:
            extras.execute_batch(
                self.db.cursor,
                """
                 INSERT INTO scientific_papers(paper_url,
                                arxiv_id,
                                title,
                                abstract,
                                url_abs,
                                url_pdf,
                                proceeding,
                                date)
                    VALUES (
                                %(paper_url)s,
                                %(arxiv_id)s,
                                %(title)s,
                                %(abstract)s,
                                %(url_abs)s,
                                %(url_pdf)s,
                                %(proceeding)s,
                                %(date)s
                                )
                ON CONFLICT DO NOTHING
                    """,
                data,
            )

    def fill_links(self, data):
        if self.db.db_type == "sqlite":
            self.db.cursor.executemany(
                """
                INSERT INTO urls(url)
                SELECT :repo_url
                WHERE NOT EXISTS (SELECT 1 FROM urls WHERE url=:repo_url)
                ;""",
                data,
            )
            self.db.cursor.executemany(
                """
                INSERT INTO scientific_papers_repo_links(paper_id,
                                url_id,
                                is_official,
                                mentioned_in_paper,
                                mentioned_in_github,
                                framework)
                    SELECT
                                p.id,
                                u.id,
                                :is_official,
                                :mentioned_in_paper,
                                :mentioned_in_github,
                                :framework
                        FROM scientific_papers p
                        INNER JOIN urls u
                        ON p.paper_url=:paper_url
                        AND u.url=:repo_url
                ON CONFLICT DO NOTHING
                ;""",
                data,
            )
        else:
            extras.execute_batch(
                self.db.cursor,
                """
                INSERT INTO urls(url)
                SELECT %(repo_url)s
                WHERE NOT EXISTS (SELECT 1 FROM urls WHERE url=%(repo_url)s)
                ;
                INSERT INTO scientific_papers_repo_links(paper_id,
                                url_id,
                                is_official,
                                mentioned_in_paper,
                                mentioned_in_github,
                                framework)
                    SELECT
                                p.id,
                                u.id,
                                %(is_official)s,
                                %(mentioned_in_paper)s,
                                %(mentioned_in_github)s,
                                %(framework)s
                        FROM scientific_papers p
                        INNER JOIN urls u
                        ON p.paper_url=%(paper_url)s
                        AND u.url=%(repo_url)s
                ON CONFLICT DO NOTHING
                ;""",
                data,
            )

        self.db.connection.commit()
