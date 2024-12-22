import string
import random
import sqlite3
import copy
import csv
import os
from collections import OrderedDict
import oyaml as yaml
from sh import pg_dump, psql
import psycopg2
import psycopg2.extras
from . import errors
from . import check_sqlname_safe
import string
import re


def sizeof_fmt(num, suffix="B"):
    """
    From https://stackoverflow.com/questions/1094841/get-human-readable-version-of-file-size
    """
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def check_db_equal(db, other_db):
    """
    Checks whether the databases are the same or not by inserting a random value in a new table
    True: they are the same
    False: they are not
    """
    value = "".join(random.choice(string.ascii_letters) for i in range(20))

    db.cursor.execute("CREATE TABLE _temp_check_{}(value TEXT);".format(value))
    db.connection.commit()

    try:
        other_db.cursor.execute(
            "CREATE TABLE _temp_check_{}(value TEXT);".format(value)
        )
    except psycopg2.errors.DuplicateTable:
        ans = True
    except sqlite3.OperationalError:
        ans = True
    else:
        ans = False
    finally:
        db.connection.commit()
        other_db.connection.commit()
        db.cursor.execute("DROP TABLE IF EXISTS _temp_check_{};".format(value))
        other_db.cursor.execute("DROP TABLE IF EXISTS _temp_check_{};".format(value))
        db.connection.commit()
        other_db.connection.commit()

    return ans


def get_tables_info(db, as_yml=False, keep_dbinfo=False):
    """
    Returns a dict {tablename:[attr_list]} from either postgres or sqlite DB
    """
    ans = OrderedDict()
    if db.db_type == "postgres":
        db.cursor.execute(
            """SELECT c.table_name,c.column_name FROM information_schema.columns c
                            WHERE c.table_schema = (SELECT current_schema())
                            ORDER BY c.table_name,c.ordinal_position ;"""
        )
        for tab, col in db.cursor.fetchall():
            try:
                ans[tab].append(col)
            except KeyError:
                ans[tab] = [col]
    else:
        db.cursor.execute(
            """SELECT name FROM sqlite_master
            WHERE type='table';
            """
        )
        ans = {t[0]: [] for t in db.cursor.fetchall()}
        for t in ans.keys():
            check_sqlname_safe(t)
            db.cursor.execute(
                """PRAGMA table_info({table_name});""".format(table_name=t)
            )
            ans[t] = [r[1] for r in db.cursor.fetchall()]

    if not keep_dbinfo and "_dbinfo" in ans.keys():
        del ans["_dbinfo"]
    if as_yml:
        return yaml.dump(ans)
    else:
        return ans


def get_table_data(
    table,
    columns,
    db,
    batch_size=None,
    use_multiqueries=False,
    page_size=None,
    query="""
            SELECT {columns} FROM {table}
            """,
):
    """
    gets a generator outputing the rows of the table
    """
    check_sqlname_safe(table)

    if (not use_multiqueries) and db.db_type == "postgres":
        cursor = db.connection.cursor(name="cursor_{}".format(table))
        if page_size is not None:
            cursor.itersize = page_size
    else:
        cursor = db.cursor
        if db.db_type == "postgres":
            orig_itersize = cursor.itersize
            if page_size is not None:
                cursor.itersize = page_size

    for c in columns:
        check_sqlname_safe(c)

    if batch_size is None:
        cursor.execute(query.format(columns=",".join(columns), table=table))
        return cursor.fetchall()
    else:

        def ans_gen():
            counter = 0
            if not use_multiqueries:
                cursor.execute(query.format(columns=",".join(columns), table=table))
            while True:
                if use_multiqueries:
                    cursor.execute(
                        (
                            query
                            + """LIMIT {limit} OFFSET {offset}
                                                ;"""
                        ).format(
                            columns=",".join(columns),
                            table=table,
                            limit=batch_size,
                            offset=counter,
                        )
                    )
                    rows = list(cursor.fetchall())
                else:
                    rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                else:
                    if counter != 0 or len(rows) == batch_size:
                        db.logger.info(
                            "Fetched {} rows of table {}".format(
                                counter + len(rows), table
                            )
                        )
                        counter += len(rows)
                    for r in rows:
                        yield r
                    if len(rows) < batch_size:
                        break
            if db.db_type == "postgres" and use_multiqueries:
                cursor.itersize = orig_itersize

        return ans_gen()


def insert_table_data(table, columns, db, table_data, page_size=10**5):
    check_sqlname_safe(table)
    for c in columns:
        check_sqlname_safe(c)
    if db.db_type == "postgres":
        psycopg2.extras.execute_batch(
            db.cursor,
            """
            INSERT INTO {table}({columns}) VALUES ({separators}) ON CONFLICT DO NOTHING
            ;""".format(
                columns=",".join(columns),
                table=table,
                separators=",".join(["%s" for _ in columns]),
            ),
            (td for td in table_data),
            page_size=page_size,
        )
    else:
        db.cursor.executemany(
            """
            INSERT OR IGNORE INTO {table}({columns}) VALUES ({separators})
            ;""".format(
                columns=",".join(columns),
                table=table,
                separators=",".join(["?" for _ in columns]),
            ),
            (td for td in table_data),
        )


def fix_sequences(db):
    if db.db_type == "postgres":
        db.logger.info("Fixing sequences")
        db.cursor.execute(
            """
                SELECT 'SELECT SETVAL(' ||
                        quote_literal(quote_ident(PGT.schemaname) || '.' || quote_ident(S.relname)) ||
                        ', COALESCE(MAX(' ||quote_ident(C.attname)|| '), 1) ) FROM ' ||
                        quote_ident(PGT.schemaname)|| '.'||quote_ident(T.relname)|| ';'
                FROM pg_class AS S,
                    pg_namespace AS NS,
                    pg_depend AS D,
                    pg_class AS T,
                    pg_attribute AS C,
                    pg_tables AS PGT
                WHERE S.relkind = 'S'
                    AND S.relnamespace = NS.oid
                    AND NS.nspname = (SELECT current_schema())
                    AND S.oid = D.objid
                    AND D.refobjid = T.oid
                    AND D.refobjid = C.attrelid
                    AND D.refobjsubid = C.attnum
                    AND T.relname = PGT.tablename
                    AND PGT.schemaname=(SELECT current_schema())
                ORDER BY S.relname;
                --adapted from https://wiki.postgresql.org/wiki/Fixing_Sequences
            """
        )
        commands = [r[0] for r in db.cursor.fetchall()]
        for c in commands:
            db.cursor.execute(c)
        db.connection.commit()


def export(
    orig_db,
    dest_db,
    page_size=10**5,
    ignore_error=False,
    force=False,
    batch_size=10**6,
):
    """
    Exporting data from one database to another, being SQLite or PostgreSQL for both
    """
    if check_db_equal(orig_db, dest_db):
        # orig_db.logger.info('Cannot export to self, skipping')
        raise errors.RepoToolsExportSameDBError
    else:
        dest_t_info = get_tables_info(dest_db, keep_dbinfo=True)
        orig_t_info = get_tables_info(orig_db, keep_dbinfo=True)
        if len(dest_t_info) == 0:
            dest_db.init_db()
        elif "_dbinfo" not in dest_t_info.keys():
            if ignore_error:
                dest_db.logger.info(
                    """Skipping export from {orig_db}({orig_db_type}) to {destdb}({destdb_type}):
destination is not empty but has no _dbinfo table""".format(
                        orig_db=orig_db.db_name,
                        orig_db_type=orig_db.db_type,
                        destdb=dest_db.db_name,
                        destdb_type=dest_db.db_type,
                    )
                )
                return
            else:
                raise errors.RepoToolsDBStructError(
                    "The destination database (for export) does not have the proper structure."
                )
        elif "_dbinfo" not in orig_t_info.keys():
            if ignore_error:
                orig_db.logger.info(
                    """Skipping export from {orig_db}({orig_db_type}) to {destdb}({destdb_type}):
origin has no _dbinfo table""".format(
                        orig_db=orig_db.db_name,
                        orig_db_type=orig_db.db_type,
                        destdb=dest_db.db_name,
                        destdb_type=dest_db.db_type,
                    )
                )
                return
            else:
                raise errors.RepoToolsDBStructError(
                    "The origin database (for export) does not have the proper structure."
                )

        orig_db.cursor.execute(
            """SELECT info_content FROM _dbinfo WHERE info_type='uuid';"""
        )
        orig_uuid = orig_db.cursor.fetchone()[0]

        dest_db.cursor.execute(
            """SELECT info_content FROM _dbinfo WHERE info_type='exported_from';"""
        )
        exportedfrom_uuid = dest_db.cursor.fetchone()
        if exportedfrom_uuid is not None:
            exportedfrom_uuid = exportedfrom_uuid[0]

        dest_db.cursor.execute(
            """SELECT info_content FROM _dbinfo WHERE info_type='finished_exported_from';"""
        )
        finished_exportedfrom_uuid = dest_db.cursor.fetchone()
        if finished_exportedfrom_uuid is not None:
            finished_exportedfrom_uuid = finished_exportedfrom_uuid[0]

        if orig_uuid is None:
            raise errors.RepoToolsError("No UUID for origin database")
        elif exportedfrom_uuid == orig_uuid and finished_exportedfrom_uuid == orig_uuid:
            orig_db.logger.info("Export already done, skipping")
        elif exportedfrom_uuid is not None and exportedfrom_uuid != orig_uuid:
            raise errors.RepoToolsError(
                "Trying to export in a non empty DB, already result of an export but from a different source DB"
            )
        else:
            if dest_db.db_type == "postgres":
                dest_db.cursor.execute(
                    """INSERT INTO _dbinfo(info_type,info_content) VALUES ('exported_from',%(orig_uuid)s) ON CONFLICT DO NOTHING;""",
                    {"orig_uuid": orig_uuid},
                )
            else:
                dest_db.cursor.execute(
                    """INSERT OR IGNORE INTO _dbinfo(info_type,info_content) VALUES ('exported_from',:orig_uuid);""",
                    {"orig_uuid": orig_uuid},
                )
            tables_info = get_tables_info(db=orig_db)
            tables_info_dest = get_tables_info(db=dest_db)
            if dest_db.db_type == "postgres":
                dest_db.cursor.execute(
                    disable_triggers_cmd(db=dest_db, tables_info=tables_info_dest)
                )
                if dest_db.connection.server_version >= 90600:
                    dest_db.cursor.execute(
                        """SET SESSION idle_in_transaction_session_timeout = 0;"""
                    )
                else:
                    dest_db.logger.warning(
                        "You may experience failure of export due to parameter idle_in_transaction_session_timeout not existing in PostgreSQL<9.6"
                    )
            try:
                for t, columns in tables_info.items():
                    check_sqlname_safe(t)
                    if t in tables_info_dest.keys():
                        dest_db.cursor.execute("SELECT 1 FROM {} LIMIT 1;".format(t))
                        if dest_db.cursor.fetchone() == (1,) and not force:
                            dest_db.logger.info(
                                "Skipping table {}, already exported".format(t)
                            )
                            continue
                        dest_db.logger.info("Exporting table {}".format(t))
                        table_data = get_table_data(
                            table=t,
                            columns=columns,
                            db=orig_db,
                            batch_size=batch_size,
                            page_size=page_size,
                        )  # as a generator
                        insert_table_data(
                            table=t,
                            columns=columns,
                            db=dest_db,
                            table_data=table_data,
                            page_size=page_size,
                        )
                        dest_db.connection.commit()
                    else:
                        dest_db.logger.info(
                            "Skipping table {}, not in schema of destination DB".format(
                                t
                            )
                        )
                if dest_db.db_type == "postgres":
                    dest_db.cursor.execute(
                        enable_triggers_cmd(db=dest_db, tables_info=tables_info_dest)
                    )
                    fix_sequences(db=dest_db)
                if dest_db.db_type == "postgres":
                    dest_db.cursor.execute(
                        """INSERT INTO _dbinfo(info_type,info_content) VALUES ('finished_exported_from',%(orig_uuid)s);""",
                        {"orig_uuid": orig_uuid},
                    )
                else:
                    dest_db.cursor.execute(
                        """INSERT INTO _dbinfo(info_type,info_content) VALUES ('finished_exported_from',:orig_uuid);""",
                        {"orig_uuid": orig_uuid},
                    )
            except:
                # closing connection manually because idle_in_transaction_session_timeout is infinite
                try:
                    dest_db.connection.close()
                except:
                    pass
                raise
            dest_db.connection.commit()


def disable_triggers_cmd(db, tables_info=None):
    if tables_info is None:
        tables_info = get_tables_info(db=db)
    for t in tables_info.keys():
        check_sqlname_safe(t)
    return "\n".join(
        [
            """ALTER TABLE {table} DISABLE TRIGGER ALL;\n""".format(table=t)
            for t in tables_info.keys()
        ]
    )


def enable_triggers_cmd(db, tables_info=None):
    if tables_info is None:
        tables_info = get_tables_info(db=db)
    for t in tables_info.keys():
        check_sqlname_safe(t)
    return "\n".join(
        [
            """ALTER TABLE {table} ENABLE TRIGGER ALL;\n""".format(table=t)
            for t in tables_info.keys()
        ]
    )


def clean_table(db, table, autocommit=True):
    check_sqlname_safe(table)
    db.cursor.execute("""DROP TABLE IF EXISTS {table} ;""".format(table=table))
    if autocommit:
        db.connection.commit()


def attr_script_removal(sql_script, attr):
    if isinstance(attr, str):
        attr_list = [attr]
    else:
        attr_list = attr
    prefix = sql_script.split("(")[0]
    suffix = sql_script.split(")")[-1]
    main = "(".join(")".join(sql_script.split(")")[:-1]).split("(")[1:])
    lines = main.split(",")
    new_lines = []
    for l in lines:
        flagged = False
        for a in attr_list:
            trimmed_l = l.replace("\n", "").replace("\t", "")
            while trimmed_l.startswith(" "):
                trimmed_l = trimmed_l[1:]
            if trimmed_l.startswith("{} ".format(a)):
                flagged = True
                break
        if not flagged:
            new_lines.append(l)
    return "{prefix}({middle}){suffix}".format(
        prefix=prefix, suffix=suffix, middle=",".join(new_lines)
    )


def clean_attr(db, table, attr, autocommit=True):
    check_sqlname_safe(table)
    if isinstance(attr, str):
        attr_list = [attr]
    else:
        attr_list = attr
    for a in attr_list:
        check_sqlname_safe(a)

    t_info = get_tables_info(db=db)
    to_remove = set(attr_list) & set(t_info[table])
    if table in t_info.keys() and to_remove:
        if db.db_type == "sqlite" and sqlite3.sqlite_version < "3.37":
            # raise NotImplementedError('ALTER TABLE t DROP COLUMN not implemented for this version of SQLite, upgrade to >=3.37.0, drop whole tables, or clean in postgres and export')

            db.cursor.execute(
                """SELECT sql FROM sqlite_master WHERE tbl_name = '{table}';""".format(
                    table=table
                )
            )
            table_sql = db.cursor.fetchone()[0]

            db.cursor.execute(
                """SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name='{table}' AND sql IS NOT NULL;""".format(
                    table=table
                )
            )
            idx_list = [r[0] for r in db.cursor.fetchall()]

            db.cursor.execute(
                """ALTER TABLE {table} RENAME TO __old__{table};""".format(table=table)
            )

            clean_table_sql = table_sql
            for a in attr_list:
                clean_table_sql = attr_script_removal(
                    sql_script=clean_table_sql, attr=a
                )  # spot and remove target attribute

            db.cursor.execute(clean_table_sql)

            orig_attr_list = copy.deepcopy(t_info[table])
            remaining_attr_list = [a for a in orig_attr_list if a not in to_remove]
            db.cursor.execute(
                """INSERT INTO {table}({attr_list}) SELECT {attr_list} FROM __old__{table};""".format(
                    table=table, attr_list=",".join(remaining_attr_list)
                )
            )

            db.cursor.execute("""DROP TABLE __old__{table};""".format(table=table))

            for idx_sql in idx_list:
                cleaned_idx_sql = idx_sql
                to_discard = False
                for a in attr_list:
                    cleaned_idx_sql = cleaned_idx_sql.replace(
                        "{},".format(a), ""
                    ).replace(",{}".format(a), "")
                    if "({})".format(a) in cleaned_idx_sql:
                        to_discard = True
                if to_discard:
                    db.logger.info("Skipping index: {}".format(idx_sql))
                else:
                    db.cursor.execute(cleaned_idx_sql)

        else:
            for a in attr_list:
                db.cursor.execute(
                    """ALTER TABLE {table} DROP COLUMN {attr} ;""".format(
                        table=table, attr=a
                    )
                )
    if autocommit:
        db.connection.commit()


def clean(
    db,
    *,
    inclusion_list=None,
    exclusion_list=None,
    autocommit=False,
    vacuum_sqlite=True,
):
    """
    Certain number of steps to prepare the dataset for release, not including pseudonymization
    """

    db.clean_users()

    if autocommit is False:
        db.cursor.execute("BEGIN TRANSACTION;")
    if exclusion_list is not None and inclusion_list is not None:
        raise SyntaxError(
            "Both exclusion_list and inclusion_list args cannot be provided, pick one method"
        )
    t_info = get_tables_info(db=db, keep_dbinfo=True)

    if exclusion_list is not None:
        for t in exclusion_list.keys():
            if t in t_info.keys():
                if (
                    len(set(t_info[t]) - set(exclusion_list[t])) == 0
                    or len(exclusion_list[t]) == 0
                ):
                    clean_table(db=db, table=t, autocommit=autocommit)
                else:
                    clean_attr(
                        db=db, table=t, attr=exclusion_list[t], autocommit=autocommit
                    )

    elif inclusion_list is not None:
        for t in set(t_info.keys()) - set(inclusion_list.keys()):
            clean_table(db=db, table=t, autocommit=autocommit)
        for t in inclusion_list.keys():
            if t in t_info.keys():
                if len(inclusion_list[t]) != 0:
                    clean_attr(
                        db=db,
                        table=t,
                        attr=set(t_info[t]) - set(inclusion_list[t]),
                        autocommit=autocommit,
                    )

    if not autocommit:
        # db.cursor.execute('COMMIT;')
        db.connection.commit()
    if db.db_type == "sqlite" and vacuum_sqlite:
        db.logger.info(
            "Vacuuming SQLite DB {}, initial file size: {}".format(
                db.db_name, sizeof_fmt(os.path.getsize(db.db_path))
            )
        )
        db.cursor.execute("VACUUM;")
        db.logger.info(
            "Vacuumed SQLite DB {}, end file size: {}".format(
                db.db_name, sizeof_fmt(os.path.getsize(db.db_path))
            )
        )


def dump_pg_csv(
    db,
    output_folder,
    import_dump=True,
    schema_dump=True,
    csv_dump=True,
    csv_psql=True,
    force=False,
    quiet_error=True,
):
    """
    Dumping a postgres DB to schema.sql, import.sql and one CSV per table
    """

    if not db.db_type == "postgres":
        raise errors.RepoToolsDumpSQLiteError(
            "Trying to dump to schema and CSV from a SQLite DB, should be PostgreSQL"
        )

    if not os.path.exists(os.path.join(output_folder, "data")):
        os.makedirs(os.path.join(output_folder, "data"))

    tables_info = get_tables_info(db=db)

    for filename, bool_var in [
        ("schema.sql", schema_dump),
        ("import.sql", import_dump),
    ] + [("data/{}.csv".format(t), csv_dump) for t in sorted(tables_info.keys())]:
        filepath = os.path.join(output_folder, filename)
        if os.path.exists(filepath) and bool_var:
            if force:
                db.logger.warning("Removing {} for replacement".format(filename))
                os.remove(filepath)
            elif quiet_error:
                db.logger.warning(
                    "While dumping: {} already exists. Use force=True to replace existing files.".format(
                        filename
                    )
                )
                return
            else:
                raise errors.RepoToolsDumpPGError(
                    "Error while dumping: {} already exists. Use force=True to replace.".format(
                        filename
                    )
                )

    db.logger.info("Dumping DB to folder")

    ###### schema.sql ######
    if schema_dump:
        with open(os.path.join(output_folder, "schema.sql"), "w") as f:
            pg_dump(
                "-h",
                db.db_conninfo["host"],
                "-U",
                db.db_conninfo["db_user"],
                db.db_conninfo["db_name"],
                "-p",
                db.db_conninfo["port"],
                "--schema-only",
                "--no-owner",
                "--no-privileges",
                "--no-security-labels",
                "--no-tablespaces",
                _out=f,
            )

    ###### import.sql ######
    if import_dump:
        copy_tables_str = "\n".join(
            [
                """\\copy {table} ({columns}) FROM 'data/{table}.csv' WITH CSV HEADER;""".format(
                    table=t, columns=",".join(col)
                )
                for t, col in sorted(tables_info.items())
            ]
        )

        import_str = """
BEGIN;

-- Disabling Triggers
{disable_trig}

-- Inserting data
{copy_tables}

-- Reenabling Triggers
{enable_trig}

COMMIT;
    """.format(
            disable_trig=disable_triggers_cmd(db=db, tables_info=tables_info),
            enable_trig=enable_triggers_cmd(db=db, tables_info=tables_info),
            copy_tables=copy_tables_str,
        )

        with open(os.path.join(output_folder, "import.sql"), "w") as f:
            f.write(import_str)

    ###### CSV files ######
    # header, then each line.
    if csv_dump:
        if csv_psql:
            copy_tables_str = "\n".join(
                [
                    """\\copy {table} ({columns}) TO '{folder_table}.csv' WITH CSV HEADER;""".format(
                        table=t,
                        columns=",".join(col),
                        folder_table=os.path.join(output_folder, "data", t),
                    )
                    for t, col in sorted(tables_info.items())
                ]
            )
            psql(
                "-h",
                db.db_conninfo["host"],
                "-U",
                db.db_conninfo["db_user"],
                db.db_conninfo["db_name"],
                "-p",
                db.db_conninfo["port"],
                _in=copy_tables_str,
            )
        else:
            for t, col in tables_info.items():
                with open(
                    os.path.join(output_folder, "data", "{}.csv".format(t)), "w"
                ) as f:
                    writer = csv.writer(f)
                    writer.writerow(col)
                    db.cursor.execute(
                        "SELECT {columns} FROM {table};".format(
                            table=t, columns=",".join(col)
                        )
                    )
                    for r in db.cursor.fetchall():
                        writer.writerow(r)

    ####### script.sh
    script_sh_content = """#!/bin/bash
set -e
echo 'Database name? (default rust_repos)'
read DBNAME
if [ -z "$DBNAME" ]
then
    DBNAME="rust_repos"
fi

echo 'Database host? (default localhost)'
read DBHOST
if [ -z "$DBHOST" ]
then
    DBHOST="localhost"
fi

echo 'Database port? (default 5432)'
read DBPORT
if [ -z "$DBPORT" ]
then
    DBPORT="5432"
fi

echo 'Database user? (default postgres)'
read DBUSER
if [ -z "$DBUSER" ]
then
    DBUSER="postgres"
fi

echo "Create database $DBNAME? (empty=yes)"
read DBCREATE
if [ -z "$DBCREATE" ]
then
    psql -q -h $DBHOST --port=$DBPORT --user=$DBUSER -c "create database $DBNAME;"
fi

psql -q -h $DBHOST --port=$DBPORT --user=$DBUSER $DBNAME < schema.sql
psql -q -h $DBHOST --port=$DBPORT --user=$DBUSER $DBNAME < import.sql

echo "Finishing importing data into $DBNAME"
"""
    with open(os.path.join(output_folder, "script.sh"), "w") as f:
        f.write(script_sh_content)

    db.logger.info("Dumped DB to folder")


def export_filters(db, folder=None, overwrite=False):
    if folder is None:
        folder = db.data_folder

    if not os.path.exists(folder):
        os.makedirs(folder)

    # packages
    db.cursor.execute(
        """
        SELECT s.name,p.name FROM filtered_deps_package fdp
        INNER JOIN packages p
        ON p.id=fdp.package_id
        INNER JOIN sources s
        ON s.id=p.source_id
        ;"""
    )

    res = list(db.cursor.fetchall())
    filepath = os.path.join(folder, "filtered_packages.csv")
    if not overwrite and os.path.exists(filepath):
        with open(filepath, "r") as f:
            reader = csv.reader(f)
            next(reader)
            previous_res = [tuple(r) for r in reader]
            res = sorted(list(set(previous_res + res)))

    with open(filepath, "w") as f:
        f.write("source,package\n")
        for s, p in res:
            f.write('"{}","{}"\n'.format(s, p))

    # repos
    db.cursor.execute(
        """
        SELECT s.name,r.owner,r.name FROM filtered_deps_repo fdr
        INNER JOIN repositories r
        ON r.id=fdr.repo_id
        INNER JOIN sources s
        ON s.id=r.source
        ;"""
    )

    res = list(db.cursor.fetchall())
    filepath = os.path.join(folder, "filtered_repos.csv")
    if not overwrite and os.path.exists(filepath):
        with open(filepath, "r") as f:
            reader = csv.reader(f)
            next(reader)
            previous_res = [
                (s, r.split("/")[0], "/".join(r.split("/")[1:])) for s, r in reader
            ]
            res = sorted(list(set(previous_res + res)))

    with open(filepath, "w") as f:
        f.write("source,repo\n")
        for s, o, n in res:
            f.write('"{}","{}/{}"\n'.format(s, o, n))

    # repo_edges
    db.cursor.execute(
        """
        SELECT ss.name,rs.owner,rs.name,sd.name,rd.owner,rd.name FROM filtered_deps_repoedges fdre
        INNER JOIN repositories rs
        ON rs.id=fdre.repo_source_id 
        INNER JOIN sources ss
        ON ss.id=rs.source
        INNER JOIN repositories rd
        ON rd.id=fdre.repo_dest_id 
        INNER JOIN sources sd
        ON sd.id=rd.source
        ;"""
    )

    res = list(db.cursor.fetchall())
    filepath = os.path.join(folder, "filtered_repoedges.csv")
    if not overwrite and os.path.exists(filepath):
        with open(filepath, "r") as f:
            reader = csv.reader(f)
            next(reader)
            previous_res = [
                (
                    ss,
                    rs.split("/")[0],
                    "/".join(rs.split("/")[1:]),
                    sd,
                    rd.split("/")[0],
                    "/".join(rd.split("/")[1:]),
                )
                for ss, rs, sd, rd in reader
            ]
            res = sorted(list(set(previous_res + res)))

    with open(filepath, "w") as f:
        f.write("source_source,repo_source,source_dest,repo_dest\n")
        for s, o, n, s2, o2, n2 in res:
            f.write('"{}","{}/{}","{}","{}/{}"\n'.format(s, o, n, s2, o2, n2))

    # package_edges
    db.cursor.execute(
        """
        SELECT ss.name,ps.name,sd.name,pd.name FROM filtered_deps_packageedges fdpe
        INNER JOIN packages ps
        ON ps.id=fdpe.package_source_id 
        INNER JOIN sources ss
        ON ss.id=ps.source_id
        INNER JOIN packages pd
        ON pd.id=fdpe.package_dest_id 
        INNER JOIN sources sd
        ON sd.id=pd.source_id
        ;"""
    )

    res = list(db.cursor.fetchall())
    filepath = os.path.join(folder, "filtered_packageedges.csv")
    if not overwrite and os.path.exists(filepath):
        with open(filepath, "r") as f:
            reader = csv.reader(f)
            next(reader)
            previous_res = [tuple(r) for r in reader]
            res = sorted(list(set(previous_res + res)))

    with open(filepath, "w") as f:
        f.write("source_source,package_source,source_dest,package_dest\n")
        for s, n, s2, n2 in res:
            f.write('"{}","{}","{}","{}"\n'.format(s, n, s2, n2))


def export_bots(db, folder=None):
    if folder is None:
        folder = db.data_folder

    if not os.path.exists(folder):
        os.makedirs(folder)

    db.cursor.execute(
        """
        SELECT it.name,i.identity FROM identities i
        INNER JOIN identity_types it
        ON it.id=i.identity_type_id
        AND i.is_bot
        ;"""
    )

    with open(os.path.join(folder, "bots.csv"), "w") as f:
        f.write("identity_type,identity\n")
        for s, p in db.cursor.fetchall():
            f.write('"{}","{}"\n'.format(s, p))


def generate_tables_file(filepath, db):
    """
    generates a yml file with the list of tables and their columns
    parses the file if it exists already, especially the commented lines, to only add missing columns and tables, and keep commented lines commented.
    If a table is commented, its new columns will be commented as well
    Comments are combinatiosn of an arbitrary number of '#' or ' ' at the beginning of the line. Resulting empty lines discarded
    """
    tables_info = get_tables_info(db=db, as_yml=False)

    if os.path.exists(filepath):
        with open(filepath, "r") as f:
            previous_content = f.read()
        filtered_previous = yaml.load(previous_content, Loader=yaml.SafeLoader)
    else:
        previous_content = ""
        filtered_previous = OrderedDict()

    def clean_line(l):
        while len(l) > 0 and l[0] in ("#", " "):
            l = l[1:]
        return l

    uncommented_previous_content = "\n".join(
        [clean_line(l) for l in previous_content.split("\n") if clean_line(l) != ""]
    )
    unfiltered_previous = yaml.load(
        uncommented_previous_content, Loader=yaml.SafeLoader
    )
    if unfiltered_previous is None:
        unfiltered_previous = OrderedDict()
    elif isinstance(unfiltered_previous, str):
        raise SyntaxError(f"Error when parsing yaml file: {unfiltered_previous}")

    def all_columns(t):
        ans = []
        for d in [tables_info, unfiltered_previous]:
            if t in d.keys():
                for c in d[t]:
                    if c not in ans:
                        ans.append(c)
        return sorted(ans)

    total_dict = {
        t: all_columns(t)
        for t in sorted(
            list(set(list(tables_info.keys()) + list(unfiltered_previous.keys())))
        )
    }

    tables_mask = set()
    columns_mask = set()
    for t in total_dict.keys():
        if t not in filtered_previous.keys() and t in unfiltered_previous.keys():
            tables_mask.add(t)
        else:
            for c in total_dict[t]:
                if (
                    t in unfiltered_previous.keys()
                    and c in unfiltered_previous[t]
                    and (
                        t not in filtered_previous.keys()
                        or c not in filtered_previous[t]
                    )
                ):
                    columns_mask.add((t, c))

    with open(filepath, "w") as f:
        for t, c_l in total_dict.items():
            if t in tables_mask:
                f.write(f"# {t}:\n")
                for c in c_l:
                    f.write(f"# - {c}\n")
            else:
                f.write(f"{t}:\n")
                for c in c_l:
                    if (t, c) in columns_mask:
                        f.write(f"# - {c}\n")
                    else:
                        f.write(f" - {c}\n")


class Merger(object):
    def __init__(
        self,
        orig_db,
        dest_db,
        page_size=10**5,
        ignore_error=False,
        force=False,
        batch_size=10**6,
        disable_trig=False,
        fix_seq=False,
    ):
        self.orig_db = orig_db
        self.dest_db = dest_db
        self.page_size = page_size
        self.ignore_error = ignore_error
        self.force = force
        self.batch_size = batch_size
        self.disable_trig = disable_trig
        self.fix_seq = fix_seq

    def get_allowed_tables(self):
        if self.orig_db.db_type == "postgres":
            self.orig_db.cursor.execute(
                """SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_type = 'BASE TABLE';"""
            )
        else:
            self.orig_db.cursor.execute(
                """SELECT name
                    FROM sqlite_master
                    WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                ;"""
            )
        self.ALLOWED_TABLES = [r[0] for r in self.orig_db.cursor.fetchall()]

    def create_temp_table(self, original_table):
        """
        Create a new table with the same defaults and separate sequences for serial columns.

        The new table will have the name temp_{self.uuid_val}_{original_table}.

        Args:
            original_table (str): Name of the original table.
        """
        if not hasattr(self, "ALLOWED_TABLES"):
            self.get_allowed_tables()

        if original_table not in self.ALLOWED_TABLES:
            raise ValueError(
                f"Table '{original_table}' is not in the allowed tables list."
            )

        db_type = self.dest_db.db_type
        connection = self.dest_db.connection

        new_table = f"temp_{self.uuid_val}_{original_table}"

        if db_type == "postgres":
            with connection.cursor() as cursor:
                # Get column definitions with defaults
                cursor.execute(
                    f"""
                    SELECT column_name, data_type, column_default, is_nullable
                    FROM information_schema.columns
                    WHERE table_name = %s;
                """,
                    (original_table,),
                )
                columns = cursor.fetchall()

                # Build CREATE TABLE statement
                create_table_query = f"CREATE TEMP TABLE {new_table} (\n"
                for col in columns:
                    column_name, data_type, column_default, is_nullable = col

                    # Handle defaults
                    default_clause = (
                        f" DEFAULT {column_default}" if column_default else ""
                    )
                    nullable_clause = " NOT NULL" if is_nullable == "NO" else ""

                    create_table_query += f"    {column_name} {data_type}{default_clause}{nullable_clause},\n"

                create_table_query = create_table_query.rstrip(",\n") + "\n);"
                try:
                    cursor.execute(create_table_query)
                except:
                    print(create_table_query)
                    raise

                # Handle SERIAL columns (create new sequences)
                for col in columns:
                    column_name, data_type, column_default, _ = col
                    if column_default and "nextval" in column_default:
                        match = re.search(
                            r"nextval\('(.+?)'::regclass\)", column_default
                        )
                        if match:
                            original_sequence = match.group(1)
                            new_sequence = f"{new_table}_{column_name}_seq"

                            cursor.execute(f"CREATE SEQUENCE {new_sequence};")
                            cursor.execute(
                                f"ALTER TABLE {new_table} ALTER COLUMN {column_name} SET DEFAULT nextval('{new_sequence}');"
                            )

        elif db_type == "sqlite":
            cursor = connection.cursor()

            # Get the table schema
            cursor.execute(f"PRAGMA table_info({original_table});")
            columns = cursor.fetchall()

            # Build CREATE TABLE statement
            create_table_query = f"CREATE TABLE {new_table} (\n"
            for col in columns:
                cid, column_name, column_type, not_null, default_value, pk = col

                # Handle defaults
                default_clause = f" DEFAULT {default_value}" if default_value else ""
                nullable_clause = " NOT NULL" if not_null else ""

                create_table_query += f"    {column_name} {column_type}{default_clause}{nullable_clause},\n"

            create_table_query = create_table_query.rstrip(",\n") + "\n);"
            try:
                cursor.execute(create_table_query)
            except:
                print(create_table_query)
                raise

        else:
            raise ValueError("Unsupported database type. Use 'postgres' or 'sqlite'.")

    def flush_temp_table_to_original(
        self,
        original_table,
        conflict_column=None,
        conflict_update_columns=None,
        ignore_columns=None,
    ):
        """
        Flush the data from a temporary table into the original table using an INSERT ... SELECT query.

        This method avoids inserting into serial primary key columns for both PostgreSQL and SQLite.
        If a conflict occurs, it resolves using the conflict_column and updates specified columns with values from the new row.
        If conflict_column is None, it performs an ON CONFLICT DO NOTHING.

        Args:
            original_table (str): The name of the original table to flush data into.
            conflict_column (str, optional): The column to check for conflicts.
            conflict_update_columns (list, optional): A list of column names to update in case of a conflict.
            ignore_columns (list, optional): A list of column names to exclude from the INSERT statement.

        Returns:
            int: The number of rows inserted into the original table.
        """
        if original_table not in self.ALLOWED_TABLES:
            raise ValueError(
                f"Table '{original_table}' is not in the allowed tables list."
            )

        db_type = self.dest_db.db_type
        connection = self.dest_db.connection

        temp_table = f"temp_{self.uuid_val}_{original_table}"

        if db_type == "postgres":
            with connection.cursor() as cursor:
                # Get column names excluding serial primary key columns
                cursor.execute(
                    f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = %s AND (column_default IS NULL OR column_default NOT LIKE 'nextval%%');
                """,
                    (original_table,),
                )
                columns = [row[0] for row in cursor.fetchall()]

                if not columns:
                    raise ValueError(
                        f"No columns available for insertion into '{original_table}'."
                    )
                if ignore_columns:
                    columns = [col for col in columns if col not in ignore_columns]

                if conflict_update_columns:
                    invalid_columns = [
                        col for col in conflict_update_columns if col not in columns
                    ]
                    if invalid_columns:
                        raise ValueError(
                            f"Invalid conflict update columns: {invalid_columns}. They are not in the list of columns for '{original_table}'."
                        )

                column_list = ", ".join(columns)

                if conflict_column and conflict_update_columns:
                    update_set = ", ".join(
                        [
                            f"{col} = CASE WHEN EXCLUDED.{col} IS NOT NULL THEN EXCLUDED.{col} ELSE {original_table}.{col} END"
                            for col in conflict_update_columns
                        ]
                    )
                    insert_query = f"""
                        INSERT INTO {original_table} ({column_list})
                        SELECT {column_list} FROM {temp_table}
                        ON CONFLICT ({conflict_column}) DO UPDATE SET {update_set};
                    """
                elif conflict_column:
                    insert_query = f"""
                        INSERT INTO {original_table} ({column_list})
                        SELECT {column_list} FROM {temp_table}
                        ON CONFLICT ({conflict_column}) DO NOTHING;
                    """
                else:
                    insert_query = f"""
                        INSERT INTO {original_table} ({column_list})
                        SELECT {column_list} FROM {temp_table}
                        ON CONFLICT DO NOTHING;
                    """

                cursor.execute(insert_query)
                connection.commit()
                return cursor.rowcount

        elif db_type == "sqlite":
            # !!! SQLite does not support on conflict do statements: defaulting to ignore
            cursor = self.dest_db.cursor

            # Get column names excluding AUTOINCREMENT primary key columns
            cursor.execute(f"PRAGMA table_info({original_table});")
            columns = [row[1] for row in cursor.fetchall() if not row[-1]]

            if not columns:
                raise ValueError(
                    f"No columns available for insertion into '{original_table}'."
                )
            if ignore_columns:
                columns = [col for col in columns if col not in ignore_columns]

            if conflict_update_columns:
                invalid_columns = [
                    col for col in conflict_update_columns if col not in columns
                ]
                if invalid_columns:
                    raise ValueError(
                        f"Invalid conflict update columns: {invalid_columns}. They are not in the list of columns for '{original_table}'."
                    )

            column_list = ", ".join(columns)

            if conflict_column and conflict_update_columns:
                update_set = ", ".join(
                    [
                        f"{col} = excluded.{col} WHERE EXCLUDED.{col} IS NOT NULL"
                        for col in conflict_update_columns
                    ]
                )
                insert_query = f"""
                    INSERT OR IGNORE INTO {original_table} ({column_list})
                    SELECT {column_list} FROM {temp_table}
                    --ON CONFLICT ({conflict_column}) DO UPDATE SET {update_set}
                    ;
                """
            else:
                insert_query = f"""
                    INSERT OR IGNORE INTO {original_table} ({column_list})
                    SELECT {column_list} FROM {temp_table}
                   
                """

            cursor.execute(insert_query)
            connection.commit()
            return cursor.rowcount

        else:
            raise ValueError("Unsupported database type. Use 'postgres' or 'sqlite'.")

    def gen_uuid(self, length=10):
        characters = string.ascii_lowercase + string.digits
        self.uuid_val = "".join(random.choice(characters) for _ in range(length))

    def merge(self):
        self.gen_uuid()
        tables_info = dict()
        if check_db_equal(self.orig_db, self.dest_db):
            raise errors.RepoToolsExportSameDBError
        else:
            self.orig_db.cursor.execute(
                """SELECT info_content FROM _dbinfo WHERE info_type='uuid';"""
            )
            orig_uuid = self.orig_db.cursor.fetchone()[0]

            self.dest_db.cursor.execute(
                """SELECT info_content FROM _dbinfo WHERE info_type='exported_from';"""
            )
            exportedfrom_uuid = self.dest_db.cursor.fetchone()
            if exportedfrom_uuid is not None:
                exportedfrom_uuid = exportedfrom_uuid[0]
            tables_info = get_tables_info(db=self.dest_db)
            if orig_uuid is None:
                raise errors.RepoToolsError("No UUID for origin database")
            else:
                if self.dest_db.db_type == "postgres":
                    if self.disable_trig:
                        self.dest_db.cursor.execute(
                            disable_triggers_cmd(
                                db=self.dest_db, tables_info=tables_info
                            )
                        )
                    if self.dest_db.connection.server_version >= 90600:
                        self.dest_db.cursor.execute(
                            """SET SESSION idle_in_transaction_session_timeout = 0;"""
                        )
                    else:
                        self.dest_db.logger.warning(
                            "You may experience failure of export due to parameter idle_in_transaction_session_timeout not existing in PostgreSQL<9.6"
                        )
                try:
                    self.merge_steps()
                    # TODO tables_info
                    # for t, columns in tables_info.items():
                    #     check_sqlname_safe(t)

                    #     self.dest_db.logger.info("Merging table {}".format(t))

                    if self.dest_db.db_type == "postgres":
                        if self.disable_trig:
                            self.dest_db.cursor.execute(
                                enable_triggers_cmd(
                                    db=self.dest_db, tables_info=tables_info
                                )
                            )
                        if self.fix_seq:
                            fix_sequences(db=self.dest_db)
                except:
                    # closing connection manually because idle_in_transaction_session_timeout is infinite
                    try:
                        self.dest_db.connection.close()
                    except:
                        pass
                    raise
                self.dest_db.connection.commit()

    def merge_steps(self):
        self.merge_sources()
        self.merge_urls()
        self.merge_repos()
        self.merge_identities()
        self.merge_commits()
        self.merge_updates()
        self.merge_errors()

    def merge_sources(self):
        self.dest_db.logger.info("Merging sources")
        self.orig_db.cursor.execute(
            """
            SELECT name,url_root
            FROM sources
            ;
            """
        )
        sources = list(self.orig_db.cursor.fetchall())
        self.create_temp_table(original_table="sources")
        if self.dest_db.db_type == "postgres":
            psycopg2.extras.execute_batch(
                self.dest_db.cursor,
                f"""
                INSERT INTO temp_{self.uuid_val}_sources(name,url_root)
                SELECT %(source)s,%(url_root)s
                EXCEPT
                SELECT s.name,s.url_root FROM sources s WHERE name=%(source)s
                """,
                [dict(source=s, url_root=u) for s, u in sources],
            )
        else:
            self.dest_db.cursor.executemany(
                f"""
                INSERT INTO temp_{self.uuid_val}_sources(name,url_root)
                SELECT :source,:url_root
                EXCEPT
                SELECT s.name,s.url_root FROM sources s WHERE name=:source
                """,
                [dict(source=s, url_root=u) for s, u in sources],
            )
        self.flush_temp_table_to_original(
            original_table="sources", conflict_column="id"
        )
        self.dest_db.cursor.execute(
            f"""
            DROP TABLE temp_{self.uuid_val}_sources
            ;"""
        )

    def merge_urls(self):
        self.dest_db.logger.info("Merging URLs")
        self.orig_db.cursor.execute(
            """
            SELECT u.url,us.name,usr.url_root,u.inserted_at,uc.url
            FROM urls u
            LEFT OUTER JOIN sources us
            ON us.id=u.source
            LEFT OUTER JOIN sources usr
            ON usr.id=u.source_root
            LEFT OUTER JOIN urls uc
            ON uc.id=u.cleaned_url
            ;
            """
        )
        info = [
            dict(
                url=url,
                usource=usource,
                usroot=usroot,
                uinsert=uinsert,
                uclean=uclean,
            )
            for (
                url,
                usource,
                usroot,
                uinsert,
                uclean,
            ) in self.orig_db.cursor.fetchall()
        ]
        self.create_temp_table(original_table="urls")
        if self.dest_db.db_type == "postgres":
            psycopg2.extras.execute_batch(
                self.dest_db.cursor,
                f"""
                INSERT INTO temp_{self.uuid_val}_urls(url,source,source_root,inserted_at)
                SELECT %(url)s,us.id,usr.id,%(uinsert)s
                FROM sources us
                INNER JOIN sources usr
                ON us.name=%(usource)s
                AND usr.url_root=%(usroot)s
                LEFT OUTER JOIN urls u
                ON u.url=%(url)s
                WHERE u.id IS NULL
                ; 
                """,
                info,
            )
            # psycopg2.extras.execute_batch(
            #     self.dest_db.cursor,
            #     """
            #     UPDATE urls SET cleaned_url=uc.id
            #     FROM urls uc
            #     WHERE urls.url=%(url)s AND uc.url=%(uclean)s
            #     """,
            #     info,
            # )
        else:
            self.dest_db.cursor.executemany(
                f"""
                INSERT INTO temp_{self.uuid_val}_urls(url,source,source_root,inserted_at)
                SELECT :url,us.id,usr.id,:uinsert
                FROM sources us
                INNER JOIN sources usr
                ON us.name=:usource
                AND usr.url_root=:usroot
                LEFT OUTER JOIN urls u
                ON u.url=:url
                WHERE u.id IS NULL
                ;
                """,
                info,
            )
            # self.dest_db.cursor.executemany(
            #     """
            #     UPDATE urls SET cleaned_url=uc.id
            #     FROM temp_{self.uuid_val}_urls tu
            #     WHERE urls.url=tu.url AND uc.url=tu.cleaned_url
            #     """,
            #     info,
            # )
        self.flush_temp_table_to_original(
            original_table="urls",
            conflict_column="id",
            ignore_columns=["cleaned_url"],
        )
        self.dest_db.cursor.execute(
            f"""
            DROP TABLE temp_{self.uuid_val}_urls
            ;"""
        )

    def merge_repos(self):
        self.dest_db.logger.info("Merging repositories")

        self.orig_db.cursor.execute(
            """
            SELECT s.name,r.owner,r.name,r.created_at,r.updated_at,r.latest_commit_time,r.cloned,u.url
            FROM repositories r
            INNER JOIN sources s
            ON s.id=r.source
            LEFT OUTER JOIN urls u
            ON r.url_id=u.id
            ;
            """
        )
        info = [
            dict(
                source=source,
                rowner=rowner,
                rname=rname,
                rcreatedat=rcreatedat,
                rupdatedat=rupdatedat,
                rlatest=rlatest,
                rcloned=rcloned,
                url=url,
            )
            for (
                source,
                rowner,
                rname,
                rcreatedat,
                rupdatedat,
                rlatest,
                rcloned,
                url,
            ) in self.orig_db.cursor.fetchall()
        ]
        self.create_temp_table(original_table="repositories")
        if self.dest_db.db_type == "postgres":
            psycopg2.extras.execute_batch(
                self.dest_db.cursor,
                f"""
                INSERT INTO temp_{self.uuid_val}_repositories(owner,name,source,url_id,created_at,updated_at,cloned,latest_commit_time)
                SELECT %(rowner)s,%(rname)s,s.id,u.id,%(rcreatedat)s,%(rupdatedat)s,%(rcloned)s,%(rlatest)s
                FROM sources s
                LEFT OUTER JOIN urls u
                ON u.url=%(url)s
                LEFT OUTER JOIN repositories r 
                ON r.source=s.id AND r.owner=%(rowner)s AND r.name=%(rname)s
                WHERE r.id IS NULL AND s.name=%(source)s
                ;
                """,
                info,
            )
            # psycopg2.extras.execute_batch(
            #     self.dest_db.cursor,
            #     """
            #     UPDATE repositories SET
            #         created_at=%(rcreatedat)s,
            #         updated_at=%(rupdatedat)s,
            #         cloned=%(rcloned)s,
            #         latest_commit_time=%(rlatest)s
            #     FROM sources s
            #     WHERE s.name=%(source)s
            #     AND repositories.source=s.id
            #     AND repositories.owner=%(rowner)s
            #     AND repositories.name=%(rname)s
            #     """,
            #     info,
            # )
        else:
            self.dest_db.cursor.executemany(
                f"""
                INSERT INTO temp_{self.uuid_val}_repositories(owner,name,source,url_id,created_at,updated_at,cloned,latest_commit_time)
                SELECT :rowner,:rname,s.id,u.id,:rcreatedat,:rupdatedat,:rcloned,:rlatest
                FROM sources s
                LEFT OUTER JOIN urls u
                ON u.url=:url
                LEFT OUTER JOIN repositories r 
                ON r.source=s.id AND r.owner=:rowner AND r.name=:rname
                WHERE r.id IS NULL AND s.name=:source
                ;
                """,
                info,
            )
            # self.dest_db.cursor.executemany(
            #     """
            #     UPDATE repositories SET
            #         created_at=:rcreatedat,
            #         updated_at=:rupdatedat,
            #         cloned=:rcloned,
            #         latest_commit_time=:rlatest
            #     FROM sources s
            #     WHERE s.name=:source
            #     AND repositories.source=s.id
            #     AND repositories.owner=:rowner
            #     AND repositories.name=:rname
            #     """,
            #     info,
            # )
        self.flush_temp_table_to_original(
            original_table="repositories",
            conflict_column="id",
            conflict_update_columns=[
                "created_at",
                "updated_at",
                "cloned",
                "latest_commit_time",
            ],
        )
        self.dest_db.cursor.execute(
            f"""
            DROP TABLE temp_{self.uuid_val}_repositories
            ;"""
        )

    def merge_identities(self):
        self.dest_db.logger.info("Merging identities")

        # insert identity types
        self.orig_db.cursor.execute(
            """
            SELECT it.name FROM identity_types it
            ;
            """
        )
        info = list(self.orig_db.cursor.fetchall())
        self.create_temp_table(original_table="identity_types")
        if self.dest_db.db_type == "postgres":
            psycopg2.extras.execute_batch(
                self.dest_db.cursor,
                f"""
                INSERT INTO temp_{self.uuid_val}_identity_types(name)
                SELECT %(it)s
                EXCEPT
                SELECT it.name FROM identity_types it WHERE it.name=%(it)s
                ;
                """,
                [dict(it=a[0]) for a in info],
            )

        else:
            self.dest_db.cursor.executemany(
                f"""
                INSERT INTO temp_{self.uuid_val}_identity_types(name)
                SELECT :it
                EXCEPT
                SELECT it.name FROM identity_types it WHERE it.name=:it
                ;
                """,
                [dict(it=a[0]) for a in info],
            )
        self.flush_temp_table_to_original(
            original_table="identity_types",
            conflict_column="id",
            conflict_update_columns=[],
        )
        # insert all identities with their own user
        self.orig_db.cursor.execute(
            """
            SELECT i.identity,it.name,i.attributes,i.created_at,i.inserted_at,i.is_bot FROM identities i
            INNER JOIN identity_types it
            ON it.id=i.identity_type_id
            ;
            """
        )
        info = [
            dict(identity=i, it=it, att=att, cat=cat, iat=iat, bot=bot)
            for i, it, att, cat, iat, bot in self.orig_db.cursor.fetchall()
        ]
        self.create_temp_table(original_table="users")
        self.create_temp_table(original_table="identities")

        if self.dest_db.db_type == "postgres":
            psycopg2.extras.execute_batch(
                self.dest_db.cursor,
                f"""
                INSERT INTO temp_{self.uuid_val}_users(
                        creation_identity,
                        creation_identity_type_id)
                            SELECT %(identity)s,id FROM identity_types WHERE name=%(it)s
                    EXCEPT 
                    SELECT i.identity,it.id FROM identities i
                        INNER JOIN identity_types it
                        ON i.identity=%(identity)s AND i.identity_type_id=it.id AND it.name=%(it)s
                ON CONFLICT DO NOTHING
                ;""",
                info,
            )
            self.flush_temp_table_to_original(
                original_table="users", conflict_column="id"
            )
            psycopg2.extras.execute_batch(
                self.dest_db.cursor,
                f"""
                INSERT INTO temp_{self.uuid_val}_identities(identity,identity_type_id,attributes,created_at,inserted_at,is_bot,user_id)
                SELECT %(identity)s,
                    it.id,
                    %(att)s,
                    %(cat)s,
                    %(iat)s,
                    %(bot)s,
                    u.id
                FROM identity_types it
                INNER JOIN users u
                ON  u.creation_identity=%(identity)s AND u.creation_identity_type_id=it.id
                    AND it.name=%(it)s AND NOT EXISTS(
                        SELECT 1 FROM identities i INNER JOIN identity_types it
                        ON it.name=%(it)s AND it.id=i.identity_type_id
                        AND i.identity=%(identity)s)
                """,
                info,
            )
        else:
            self.dest_db.cursor.executemany(
                f"""
                INSERT OR IGNORE INTO temp_{self.uuid_val}_users(
                        creation_identity,
                        creation_identity_type_id)
                            SELECT :identity,id FROM identity_types WHERE name=:it
                    EXCEPT 
                    SELECT i.identity,it.name FROM identities i
                        INNER JOIN identity_types it
                        ON i.identity=:identity AND i.identity_type_id=it.id AND it.name=:it
                ;""",
                info,
            )
            self.flush_temp_table_to_original(
                original_table="users",  # conflict_column="id"
            )
            self.dest_db.cursor.executemany(
                f"""
                INSERT INTO temp_{self.uuid_val}_identities(identity,identity_type_id,attributes,created_at,inserted_at,is_bot,user_id)
                SELECT :identity,
                    it.id,
                    :att,
                    :cat,
                    :iat,
                    :bot,
                    u.id
                FROM identity_types it
                INNER JOIN users u
                ON  u.creation_identity=:identity AND u.creation_identity_type_id=it.id
                    AND it.name=:it AND NOT EXISTS(
                        SELECT 1 FROM identities i INNER JOIN identity_types it
                        ON it.name=:it AND it.id=i.identity_type_id
                        AND i.identity=:identity)
                """,
                info,
            )
        self.flush_temp_table_to_original(
            original_table="identities",
            conflict_column="id",
            conflict_update_columns=[],
        )
        # redo all identity merges
        self.orig_db.cursor.execute(
            """
            SELECT i1.identity,it1.name,i2.identity,it2.name,mi.reason
            FROM merged_identities mi 
            INNER JOIN identities i1
            ON i1.id=mi.main_identity_id
            INNER JOIN identity_types it1
            ON it1.id=i1.identity_type_id
            INNER JOIN identities i2
            ON i2.id=mi.secondary_identity_id
            INNER JOIN identity_types it2
            ON it2.id=i2.identity_type_id
            ;
            """
        )
        info = [
            dict(i1=i1, it1=it1, i2=i2, it2=it2, reason=reason)
            for i1, it1, i2, it2, reason in self.orig_db.cursor.fetchall()
        ]
        for i, d in enumerate(info):
            self.dest_db.logger.info(f"Merge identity {i+1}/{len(info)}")
            self.dest_db.merge_identities(
                identity1=d["i1"],
                it1=d["it1"],
                identity2=d["i2"],
                it2=d["it2"],
                reason=d["reason"],
            )

        self.dest_db.cursor.execute(
            f"""
            DROP TABLE temp_{self.uuid_val}_identities
            ;"""
        )
        self.dest_db.cursor.execute(
            f"""
            DROP TABLE temp_{self.uuid_val}_users
            ;"""
        )
        self.dest_db.cursor.execute(
            f"""
            DROP TABLE temp_{self.uuid_val}_identity_types
            ;"""
        )

    def merge_commits(self):
        self.dest_db.logger.info("Merging commits")
        # commits
        self.orig_db.cursor.execute(
            """
            SELECT c.sha,
                    c.insertions,
                    c.deletions,
                    c.message,
                    c.created_at,
                    c.local_created_at,
                    c.time_offset,
                    c.original_created_at,
                    c.committed_at,
                    c.local_committed_at,
                    c.time_offset_committed,
                    c.original_committed_at,
                    r.owner,
                    r.name,
                    s.name,
                    ai.identity,
                    ait.name,
                    ci.identity,
                    cit.name
            FROM commits c
            INNER JOIN identities ai
            ON ai.id=c.author_id
            INNER JOIN identity_types ait
            ON ait.id=ai.identity_type_id
            INNER JOIN identities ci
            ON ci.id=c.committer_id
            INNER JOIN identity_types cit
            ON cit.id=ci.identity_type_id
            LEFT OUTER JOIN repositories r
            ON r.id=c.repo_id
            LEFT OUTER JOIN sources s
            ON s.id=r.source
            ;
            """
        )
        info = [
            dict(
                csha=csha,
                cinsertions=cinsertions,
                cdeletions=cdeletions,
                cmessage=cmessage,
                ccreated_at=ccreated_at,
                clocal_created_at=clocal_created_at,
                ctime_offset=ctime_offset,
                coriginal_created_at=coriginal_created_at,
                ccommitted_at=ccommitted_at,
                clocal_committed_at=clocal_committed_at,
                ctime_offset_committed=ctime_offset_committed,
                coriginal_committed_at=coriginal_committed_at,
                rowner=rowner,
                rname=rname,
                sname=sname,
                aiidentity=aiidentity,
                aitname=aitname,
                ciidentity=ciidentity,
                citname=citname,
            )
            for (
                csha,
                cinsertions,
                cdeletions,
                cmessage,
                ccreated_at,
                clocal_created_at,
                ctime_offset,
                coriginal_created_at,
                ccommitted_at,
                clocal_committed_at,
                ctime_offset_committed,
                coriginal_committed_at,
                rowner,
                rname,
                sname,
                aiidentity,
                aitname,
                ciidentity,
                citname,
            ) in self.orig_db.cursor.fetchall()
        ]
        self.create_temp_table(original_table="commits")

        if self.dest_db.db_type == "postgres":
            psycopg2.extras.execute_batch(
                self.dest_db.cursor,
                f"""
                INSERT INTO temp_{self.uuid_val}_commits(
                    sha,
                    insertions,
                    deletions,
                    message,
                    created_at,
                    local_created_at,
                    time_offset,
                    original_created_at,
                    committed_at,
                    local_committed_at,
                    time_offset_committed,
                    original_committed_at,
                    repo_id,
                    author_id,
                    committer_id)
                SELECT 
                    %(csha)s,
                    %(cinsertions)s,
                    %(cdeletions)s,
                    %(cmessage)s,
                    %(ccreated_at)s,
                    %(clocal_created_at)s,
                    %(ctime_offset)s,
                    %(coriginal_created_at)s,
                    %(ccommitted_at)s,
                    %(clocal_committed_at)s,
                    %(ctime_offset_committed)s,
                    %(coriginal_committed_at)s,
                    r.id,
                    ai.id,
                    ci.id
                FROM identity_types ait
                INNER JOIN identities ai
                    ON NOT EXISTS (SELECT 1 FROM commits WHERE sha=%(csha)s)
                    AND ai.identity=%(aiidentity)s
                    AND %(aitname)s=ait.name
                INNER JOIN identity_types cit
                    ON %(citname)s=cit.name
                INNER JOIN identities ci
                    ON ci.identity=%(ciidentity)s
                LEFT OUTER JOIN sources s
                    ON s.name=%(sname)s
                LEFT OUTER JOIN repositories r
                    ON r.owner=%(rowner)s
                    AND r.name=%(rname)s
                    AND s.id=r.source
                    ;
                """,
                info,
            )
        else:
            self.dest_db.cursor.executemany(
                f"""
                INSERT INTO temp_{self.uuid_val}_commits(
                    sha,
                    insertions,
                    deletions,
                    message,
                    created_at,
                    local_created_at,
                    time_offset,
                    original_created_at,
                    committed_at,
                    local_committed_at,
                    time_offset_committed,
                    original_committed_at,
                    repo_id,
                    author_id,
                    committer_id)
                SELECT 
                    :csha,
                    :cinsertions,
                    :cdeletions,
                    :cmessage,
                    :ccreated_at,
                    :clocal_created_at,
                    :ctime_offset,
                    :coriginal_created_at,
                    :ccommitted_at,
                    :clocal_committed_at,
                    :ctime_offset_committed,
                    :coriginal_committed_at,
                    r.id,
                    ai.id,
                    ci.id
                FROM identity_types ait
                INNER JOIN identities ai
                    ON NOT EXISTS (SELECT 1 FROM commits WHERE sha=:csha)
                    AND ai.identity=:aiidentity
                    AND :aitname=ait.name
                INNER JOIN identity_types cit
                    ON :citname=cit.name
                INNER JOIN identities ci
                    ON ci.identity=:ciidentity
                LEFT OUTER JOIN sources s
                    ON s.name=:sname
                LEFT OUTER JOIN repositories r
                    ON r.owner=:rowner
                    AND r.name=:rname
                    AND s.id=r.source
                    ;
                """,
                info,
            )
        self.flush_temp_table_to_original(
            original_table="commits",
            conflict_column="id",
            conflict_update_columns=[],
        )
        # commit parents
        self.dest_db.logger.info("Merging commit parenthood")
        self.orig_db.cursor.execute(
            """
            SELECT ch.sha,pa.sha
            FROM commit_parents cp
            INNER JOIN commits ch
            ON ch.id=cp.child_id
            INNER JOIN commits pa
            ON pa.id=cp.parent_id
            ;
            """
        )
        info = [
            dict(child_sha=child_sha, parent_sha=parent_sha)
            for (child_sha, parent_sha) in self.orig_db.cursor.fetchall()
        ]
        self.create_temp_table(original_table="commit_parents")
        if self.dest_db.db_type == "postgres":
            psycopg2.extras.execute_batch(
                self.dest_db.cursor,
                f"""
                INSERT INTO temp_{self.uuid_val}_commit_parents(
                    child_id,parent_id)
                SELECT ch.id,pa.id
                FROM commits ch
                INNER JOIN commits pa
                ON ch.sha=%(child_sha)s
                AND pa.sha=%(parent_sha)s
                ON CONFLICT DO NOTHING
                    ;
                """,
                info,
            )
        else:
            self.dest_db.cursor.executemany(
                """
                INSERT OR IGNORE INTO commit_parents(
                    child_id,parent_id)
                SELECT ch.id,pa.id
                FROM commits ch
                INNER JOIN commits pa
                ON ch.sha=:child_sha
                AND pa.sha=:parent_sha
                    ;
                """,
                info,
            )
        self.flush_temp_table_to_original(
            original_table="commit_parents",
            # conflict_column="id",
            conflict_update_columns=[],
        )
        # commit repos
        self.dest_db.logger.info("Merging commit repo links")

        self.orig_db.cursor.execute(
            """
            SELECT c.sha,r.owner,r.name,s.name
            FROM commit_repos cr
            INNER JOIN commits c
            ON c.id=cr.commit_id
            INNER JOIN repositories r
            ON r.id=cr.repo_id
            INNER JOIN sources s
            ON s.id=r.source
            ;
            """
        )
        info = [
            dict(sha=sha, owner=owner, name=name, source=source)
            for (sha, owner, name, source) in self.orig_db.cursor.fetchall()
        ]
        self.create_temp_table(original_table="commit_repos")
        if self.dest_db.db_type == "postgres":
            psycopg2.extras.execute_batch(
                self.dest_db.cursor,
                f"""
                INSERT INTO temp_{self.uuid_val}_commit_repos(
                    commit_id,repo_id)
                SELECT c.id,r.id
                FROM sources s
                INNER JOIN commits c
                ON c.sha=%(sha)s
                AND s.name=%(source)s
                INNER JOIN repositories r
                ON r.owner=%(owner)s AND r.name=%(name)s
                AND s.id=r.source
                ON CONFLICT DO NOTHING
                    ;
                """,
                info,
            )
        else:
            self.dest_db.cursor.executemany(
                """
                INSERT OR IGNORE INTO commit_repos(
                    commit_id,repo_id)
                SELECT c.id,r.id
                FROM sources s
                INNER JOIN commits c
                ON c.sha=:sha
                AND s.name=:source
                INNER JOIN repositories r
                ON r.owner=:owner AND r.name=:name
                AND s.id=r.source
                    ;
                """,
                info,
            )
        self.flush_temp_table_to_original(
            original_table="commit_repos",
            # conflict_column="id",
            conflict_update_columns=[],
        )
        self.dest_db.cursor.execute(
            f"""
            DROP TABLE temp_{self.uuid_val}_commits
            ;"""
        )
        self.dest_db.cursor.execute(
            f"""
            DROP TABLE temp_{self.uuid_val}_commit_parents
            ;"""
        )
        self.dest_db.cursor.execute(
            f"""
            DROP TABLE temp_{self.uuid_val}_commit_repos
            ;"""
        )

    def merge_updates(self):
        self.dest_db.logger.info("Merging updates")
        # table updates
        self.orig_db.cursor.execute(
            """
            SELECT table_name,
                success,
                tu.updated_at,
                info,
                r.owner,
                r.name,
                s.name,
                i.identity,
                it.name
            FROM table_updates tu
            LEFT OUTER JOIN repositories r
            ON r.id=tu.repo_id
            LEFT OUTER JOIN sources s
            ON r.source=s.id
            LEFT OUTER JOIN identities i
            ON i.id=tu.identity_id
            LEFT OUTER JOIN identity_types it
            ON it.id=i.identity_type_id
            ;
            """
        )
        info = [
            dict(
                table_name=table_name,
                success=success,
                updated_at=updated_at,
                info=info,
                rowner=rowner,
                rname=rname,
                sname=sname,
                identity=identity,
                it=it,
            )
            for (
                table_name,
                success,
                updated_at,
                info,
                rowner,
                rname,
                sname,
                identity,
                it,
            ) in self.orig_db.cursor.fetchall()
        ]
        self.create_temp_table(original_table="table_updates")
        if self.dest_db.db_type == "postgres":
            psycopg2.extras.execute_batch(
                self.dest_db.cursor,
                f"""
                INSERT INTO temp_{self.uuid_val}_table_updates(
                    table_name,success,updated_at,info,repo_id,identity_id)
                SELECT 
                    %(table_name)s,
                    %(success)s,
                    %(updated_at)s,
                    %(info)s,
                    (SELECT r.id FROM sources s
                INNER JOIN repositories r
                ON s.name=%(sname)s
                AND r.owner=%(rowner)s AND r.name=%(rname)s
                AND s.id=r.source),
                    (SELECT i.id FROM identity_types it
                        INNER JOIN identities i
                    ON i.identity=%(identity)s
                    AND %(it)s=it.name)
                    ;
                """,
                info,
            )
        else:
            self.dest_db.cursor.executemany(
                f"""
                INSERT INTO temp_{self.uuid_val}_table_updates(
                    table_name,success,updated_at,info,repo_id,identity_id)
                SELECT 
                    :table_name,
                    :success,
                    :updated_at,
                    :info,
                    (SELECT r.id FROM sources s
                INNER JOIN repositories r
                ON s.name=:sname
                AND r.owner=:rowner AND r.name=:rname
                AND s.id=r.source),
                    (SELECT i.id FROM identity_types it
                        INNER JOIN identities i
                    ON i.identity=:identity
                    AND :it=it.name)
                    ;
                """,
                info,
            )
        self.flush_temp_table_to_original(
            original_table="table_updates",
            conflict_column="id",
            conflict_update_columns=[],
        )
        # full updates
        self.orig_db.cursor.execute(
            """
            SELECT update_type,updated_at FROM full_updates
            ;
            """
        )
        info = [
            dict(utype=utype, uat=uat)
            for (utype, uat) in self.orig_db.cursor.fetchall()
        ]
        if self.dest_db.db_type == "postgres":
            psycopg2.extras.execute_batch(
                self.dest_db.cursor,
                """
                INSERT INTO full_updates(update_type,updated_at)
                SELECT %(utype)s,%(uat)s
                    ;
                """,
                info,
            )
        else:
            self.dest_db.cursor.executemany(
                """
                INSERT INTO full_updates(update_type,updated_at)
                SELECT :utype,:uat
                    ;
                """,
                info,
            )
        self.dest_db.cursor.execute(
            f"""
            DROP TABLE temp_{self.uuid_val}_table_updates
            ;"""
        )

    def merge_errors(self):
        self.dest_db.logger.info("Merging errors")
        self.orig_db.cursor.execute(
            """
            SELECT error,created_at FROM _error_logs
            ;
            """
        )
        info = [
            dict(error=error, cat=cat)
            for (error, cat) in self.orig_db.cursor.fetchall()
        ]
        if self.dest_db.db_type == "postgres":
            psycopg2.extras.execute_batch(
                self.dest_db.cursor,
                """
                INSERT INTO _error_logs(error,created_at)
                SELECT %(error)s,%(cat)s
                    ;
                """,
                info,
            )
        else:
            self.dest_db.cursor.executemany(
                """
                INSERT INTO _error_logs(error,created_at)
                SELECT :error,:cat
                    ;
                """,
                info,
            )
