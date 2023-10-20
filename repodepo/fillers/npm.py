from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import datetime
import os
import psycopg2
import json
import glob
import toml
import oyaml as yaml
import pygit2
import subprocess
import copy
import time
import csv
import semantic_version
import sqlite3
import pandas as pd
import gzip
import requests
import ijson
import itertools
import io

from .. import fillers
from ..fillers import generic, github_rest


class NPMFiller(generic.PackageFiller):
    """
    wrapper around generic.PackageFiller for data from NPM registry
    """

    def __init__(
        self,
        source="npm",
        source_urlroot=None,
        force=False,
        start_date_dl=None,
        end_date_dl=None,
        batch_size=100,
        registry_url="https://replicate.npmjs.com/registry",
        registry_filename="registry.json.gz",
        dlstats_url="https://api.npmjs.org/downloads",
        dlstats_folder="registry_dlstats",
        buffer_size=2**23,
        log_step=10**4,
        **kwargs,
    ):
        self.source = source
        self.buffer_size = buffer_size
        self.source_urlroot = source_urlroot
        self.registry_url = registry_url
        self.registry_filename = registry_filename
        self.dlstats_url = dlstats_url
        self.dlstats_folder = dlstats_folder
        self.batch_size = batch_size
        self.force = force
        self.log_step = log_step

        if start_date_dl is None:
            self.start_date_dl = datetime.datetime(2010, 1, 1)
        else:
            self.start_date_dl = start_date_dl
        if end_date_dl is None:
            self.end_date_dl = datetime.datetime.now()
        else:
            self.end_date_dl = end_date_dl
        generic.PackageFiller.__init__(self, **kwargs)

    def prepare(self):
        if self.data_folder is None:
            self.data_folder = self.db.data_folder
        data_folder = self.data_folder

        # create folder if needed
        if not os.path.exists(data_folder):
            os.makedirs(data_folder)

        self.db.register_source(source=self.source, source_urlroot=self.source_urlroot)
        if self.source_urlroot is None:
            self.source_url_root = self.db.get_source_info(source=self.source)[1]

        self.get_registry_info()
        self.parse_registry_info()
        self.get_dlstats()
        self.parse_dlstats()

    def get_registry_info(self):
        url = self.registry_url + "/_all_docs?include_docs=true"
        if self.package_limit is not None:
            url += f"&limit={self.package_limit}"
        registry_file = os.path.join(self.data_folder, self.registry_filename)
        self.download(url=url, destination=registry_file, autogzip=True)

    def parse_registry_info(self):
        registry_file = os.path.join(self.data_folder, self.registry_filename)

        def registry_info(scope="default", step=self.log_step):
            with gzip.open(registry_file, "rb") as f:
                iterator = ijson.items(
                    io.BufferedReader(f, buffer_size=self.buffer_size), "rows.item"
                )
                if self.package_limit is not None:
                    iterator = itertools.islice(iterator, self.package_limit)
                for i, j in enumerate(iterator):
                    if i % step == 0 and i != 0:
                        self.logger.info(
                            f"NPM registry info for scope {scope}: {i} packages parsed"
                        )
                    yield j

        self.package_list = (
            self.parse_package(p) for p in registry_info(scope="packages")
        )

        self.package_version_list = itertools.chain.from_iterable(
            (self.parse_versions(p) for p in registry_info(scope="versions"))
        )

        self.package_deps_list = itertools.chain.from_iterable(
            (self.parse_deps(p) for p in registry_info(scope="dependencies"))
        )

    def get_dlstats(self):
        start_date_txt = self.start_date_dl.strftime("%Y-%m-%d")
        end_date_txt = self.end_date_dl.strftime("%Y-%m-%d")
        current_offset = 0

        # if os.path.exists(os.path.join(self.data_folder,self.dlstats_folder)):
        # 	file_list = glob.glob(os.path.join(self.data_folder,self.dlstats_folder,'*_*.json.gz'))
        # 	if file_list:
        # 		current_offset = max([int(os.path.basename(r)[:-len('.json.gz')].split('_')[1]) for r in file_list])+1
        # while current_offset < len(self.package_names):
        # 	batch_size = self.batch_size
        # 	while True:
        # 		url = self.dlstats_url+f'daily/{start_date_txt}:{end_date_txt}/'+','.join(self.package_names[current_offset:current_offset+batch_size])
        # 		filename = f'{current_offset}_{min(current_offset+batch_size-1,len(self.package_names))}.json.gz'
        # 		dlstats_file = os.path.join(self.data_folder,self.dlstats_folder,filename)
        # 		self.logger.info(f'''Current query for R download stats: {current_offset}-{current_offset+batch_size-1}/{len(self.package_names)}''')
        # 		self.download(url=url,destination=dlstats_file,autogzip=True)
        # 		try:
        # 			with gzip.open(dlstats_file,'rb') as f:
        # 				r = json.load(f)
        # 				del r
        # 			break
        # 		except TypeError:
        # 			batch_size = int(batch_size/2)
        # 			if batch_size == 0:
        # 				self.logger.error('Batch size 1 did not work')
        # 				os.rename(dlstats_file,dlstats_file+'_error')
        # 				raise
        # 			time.sleep(10)
        # 			os.remove(dlstats_file)
        # 	current_offset += batch_size

    def parse_dlstats(self):
        pass

    def pick_url(self, p):
        if "repository" in p["doc"].keys() and len(p["doc"]["repository"]):
            if isinstance(p["doc"]["repository"], str):
                return p["doc"]["repository"]
            elif isinstance(p["doc"]["repository"], list):
                for u in p["doc"]["repository"]:
                    if isinstance(u, str):
                        return u
                    elif (
                        "type" in u.keys() and u["type"] == "git" and "url" in u.keys()
                    ):
                        return u["url"]
                    elif "url" in u.keys():
                        return u["url"]
                return p["doc"]["repository"][0]["url"]

            elif "url" in p["doc"]["repository"].keys():
                return p["doc"]["repository"]["url"]

        elif "url" in p["doc"].keys() and len(p["doc"]["url"]):
            return p["doc"]["url"]
        else:
            return None

    def parse_package(self, p):
        pid = p["doc"]["_id"]
        name = p["doc"]["_id"]
        p_url = self.pick_url(p)
        try:
            c_at = p["doc"]["time"]["created"]
        except KeyError:
            c_at = None
        archived_at = None
        return (pid, name, c_at, p_url, archived_at)

    def parse_versions(self, p):
        pid = p["doc"]["_id"]
        if "time" in p["doc"].keys():
            return [
                (pid, version, c_at)
                for version, c_at in p["doc"]["time"].items()
                if version not in ("created", "modified", "unpublished")
            ]
        else:
            err_message = f"""KeyError:'time' when parsing versions in {p['doc']}"""
            self.logger.error(err_message)
            self.db.log_error(err_message)
            return []

    def parse_deps(self, p):
        pid = p["doc"]["_id"]
        ans = []
        if "versions" in p["doc"].keys():
            for version, v_info in p["doc"]["versions"].items():
                if "dependencies" in v_info.keys() and isinstance(
                    v_info["dependencies"], dict
                ):
                    ans += [
                        (pid, version, dep, dep_semver)
                        for dep, dep_semver in v_info["dependencies"].items()
                    ]

        return ans

    def apply(self):
        self.fill_packages(force=self.force)
        self.fill_package_versions(force=self.force)
        self.fill_package_dependencies(force=self.force)
        self.fill_package_version_downloads(force=self.force)
        self.db.connection.commit()
