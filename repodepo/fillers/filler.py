import os
import requests
import zipfile
import pandas as pd
import logging
import csv
from psycopg2 import extras
import json
import subprocess
import gzip
import shutil
import pygit2
import tarfile
import gzip

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.INFO)


class Filler(object):
    """
    The Filler class and its children provide methods to fill the database, potentially from different sources.
    They can be linked to the database either by mentioning the database at creation of the instance (see __init__),
    or by calling the 'add_filler' method of the database. Caution, the order may be important, e.g. when foreign keys are involved.

    For writing children, just change the 'apply' method, and do not forget the commit at the end.
    This class is just an abstract 'mother' class
    """

    def __init__(
        self, db=None, name=None, data_folder=None, unique_name=False, max_reexec=0
    ):  # ,file_info=None):
        self.done = False
        if name is None:
            name = self.__class__.__name__
        self.name = name
        if db is not None:
            db.add_filler(self)
        self.data_folder = data_folder
        self.logger = logging.getLogger("fillers." + self.__class__.__name__)
        self.logger.addHandler(ch)
        self.logger.setLevel(logging.INFO)
        self.unique_name = unique_name
        self.reexec = 0
        self.max_reexec = max_reexec

        # if file_info is not None:
        #   self.set_file_info(file_info)

    # def set_file_info(self,file_info): # deprecated, files are managed at filler level only
    #   """set_file_info should add the filename in self.db.fillers_shareddata['files'][filecode] = filename
    #   while checking that the filecode is not present already in the relevant dict"""
    #   raise NotImplementedError

    def apply(self):
        # filling script here
        self.db.connection.commit()

    def prepare(self, **kwargs):
        """
        Method called to potentially do necessary preprocessings (downloading files, uncompressing, converting, ...)
        """

        if self.data_folder is None:
            self.data_folder = self.db.data_folder
        data_folder = self.data_folder

        # create folder if needed
        if not os.path.exists(data_folder):
            os.makedirs(data_folder)
        pass

    def check_requirements(self):
        return True

    def after_insert(self):
        pass

    def download(self, url, destination, wget=False, force=False, autogzip=False):
        if not force and os.path.exists(destination):
            self.logger.info("Skipping download {}".format(url))
        else:
            self.logger.info("Downloading {}".format(url))
            if not os.path.exists(os.path.dirname(destination)):
                os.makedirs(os.path.dirname(destination))
            if not wget:
                r = requests.get(url, allow_redirects=True)
                r.raise_for_status()
                if autogzip:
                    with gzip.open(destination, "wb") as f:
                        f.write(r.content)
                else:
                    with open(destination, "wb") as f:
                        f.write(r.content)
            else:
                if autogzip:
                    raise NotImplementedError(
                        "Gzipping downloaded files automatically when using wget/curl is not implemented yet"
                    )
                try:
                    subprocess.check_call(
                        "wget -O {} {}".format(destination, url).split(" ")
                    )
                except:
                    subprocess.check_call(
                        "curl -o {} -L {}".format(destination, url).split(" ")
                    )

    def unzip(self, orig_file, destination, clean_zip=False):
        self.logger.info("Unzipping {}".format(orig_file))
        with zipfile.ZipFile(orig_file, "r") as zip_ref:
            zip_ref.extractall(destination)
        if clean_zip:
            os.remove(orig_file)

    def convert_xlsx(self, orig_file, destination, clean_xlsx=False):
        self.logger.info("Converting {} to CSV".format(orig_file))
        data_xls = pd.read_excel(orig_file, index_col=None, engine="openpyxl")
        data_xls.to_csv(destination, index=False, header=None, encoding="utf-8")
        if clean_xlsx:
            os.remove(orig_file)

    def record_file(self, **kwargs):
        """
        Wrapper to solve data_folder mismatch with DB
        """
        self.db.record_file(folder=self.data_folder, **kwargs)

    def ungzip(self, orig_file, destination, clean_zip=False):
        if os.path.exists(destination):
            self.logger.info("Skipping UnGzipping of {}".format(orig_file))
        else:
            self.logger.info("UnGzipping {}".format(orig_file))
            with gzip.open(orig_file, "rb") as f_in:
                with open(destination, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

        if clean_zip and os.path.exists(orig_file):
            os.remove(orig_file)

    def untar(self, orig_file, destination, clean_zip=False):
        if os.path.exists(destination):
            self.logger.info("Skipping UnTaring of {}".format(orig_file))
        else:
            self.logger.info("UnTaring {}".format(orig_file))
            with tarfile.open(orig_file) as f_in:
                f_in.extractall(destination)

        if clean_zip and os.path.exists(orig_file):
            os.remove(orig_file)

    def clone_repo(
        self, repo_url, update=False, replace=False, repo_folder=None, **kwargs
    ):
        """
        Clones a repo locally.
        If update is True, will execute git pull. Beware, this can fail, and silently. Safe way to update is with replace, but more costly
        """
        if repo_folder is None:
            repo_folder = repo_url.split("/")[-1]
            if repo_folder.endswith(".git"):
                repo_folder = repo_folder[:-4]
        repo_folder = os.path.join(self.data_folder, repo_folder)
        if os.path.exists(repo_folder):
            if replace:
                self.logger.info(f"Removing folder {repo_folder}")
                shutil.rmtree(repo_folder)
            elif update:
                self.logger.info(f"Updating repo in {repo_folder}")
                cmd2 = "git pull --force --all"
                cmd_output2 = subprocess.check_output(
                    cmd2.split(" "),
                    cwd=repo_folder,
                    env=os.environ.update(dict(GIT_TERMINAL_PROMPT="0")),
                )
            else:
                self.logger.info(f"Folder {repo_folder} exists, skipping cloning")
        elif not os.path.exists(os.path.dirname(repo_folder)):
            os.makedirs(os.path.dirname(repo_folder))
        if not os.path.exists(repo_folder):
            self.logger.info(f"Cloning {repo_url} into {repo_folder}")
            pygit2.clone_repository(url=repo_url, path=repo_folder)

    def reexec_modif(self):
        pass
