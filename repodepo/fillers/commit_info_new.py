import datetime
import os
import psycopg2
from psycopg2 import extras
import pygit2
import json
import git
import itertools
import psutil
import subprocess
import string
import random

from .. import fillers
from ..fillers import generic, commit_info


import multiprocessing as mp

FIELDS = {
    "author_email": "%ae",
    "author_name": "%an",
    "gmt_time": "%at",
    "commit_gmt_time": "%ct",
    "sha": "%H",
    # "insertions": "%",
    # "deletions": "%",
    "message": "%s",
    "committer_email": "%ce",
    "committer_name": "%cn",
    "time_full": "%ai",
    "commit_time_full": "%ci",
    # "repo_id": "%",
    # "total": "%",
    # "parents": "%",
    # "commit_local_time": "%",
    # "local_time": "%",
    # "time_offset": "%",
    # "commit_time_offset": "%",
}


default_field_names = [
    "author_email",
    "author_name",
    "gmt_time",
    "commit_gmt_time",
    "time_full",
    "commit_time_full",
    "sha",
    "message",
    "committer_email",
    "committer_name",
    # "parents",
    # "insertions",
    # "deletions",
    # "total",
    # "repo_id",
]


def extract_time_offset(time_string):
    offset_string = time_string[-5:]
    if offset_string[-5] == "+":
        sig = 1
    elif offset_string[-5] == "-":
        sig = -1
    else:
        raise ValueError(f"Wrong offset string:{time_string}")
    minutes = (
        int(time_string[-1])
        + 10 * int(time_string[-2])
        + 60 * (int(time_string[-3]) + 10 * int(time_string[-4]))
    )
    return sig * minutes


def complete_commit_info(info, repo_id=None, insertion_info=None, parent_info=None):
    info["repo_id"] = repo_id
    info["time_offset"] = extract_time_offset(info["time_full"])
    info["commit_time_offset"] = extract_time_offset(info["commit_time_full"])
    info["local_time"] = float(info["gmt_time"]) + 60 * info["time_offset"]
    info["commit_local_time"] = (
        float(info["commit_gmt_time"]) + 60 * info["commit_time_offset"]
    )
    if insertion_info is not None:
        info["insertions"] = int(insertion_info[info["sha"]]["insertions"])
        info["deletions"] = int(insertion_info[info["sha"]]["deletions"])
        info["total"] = info["insertions"] + info["deletions"]
    if parent_info is not None:
        info["parents"] = parent_info[info["sha"]]


# git rev-list --parents --all (hash, p1,p2,...)


def git_parents(repo_folder):
    for ending in ("/.git", "/.git/"):
        if repo_folder.endswith(ending):
            repo_folder = repo_folder[: -len(ending)]

    cmd = f"""git rev-list --parents --all"""
    cmd_l = cmd.split(" ")
    cmd_output = subprocess.check_output(cmd_l, cwd=repo_folder).decode("utf-8")
    ans = {p.split(" ")[0]: p.split(" ")[1:] for p in cmd_output.split("\n") if p != ""}
    return ans


def parse_stat_line(line):
    line = (
        line.replace(" inser", "inser")
        .replace(" delet", "delet")
        .replace(" file", "file")
        .replace(" changed", "changed")
    )
    elts = line.split(" ")
    ans = {"insertions": 0, "deletions": 0}
    for e in elts[1:]:
        if e.strip(",").endswith("(+)"):
            ans["insertions"] = int(
                e.replace("insertions(+),", "")
                .replace("insertion(+),", "")
                .replace("insertions(+)", "")
                .replace("insertion(+)", "")
            )
        if e.strip(",").endswith("(-)"):
            ans["deletions"] = int(
                e.replace("deletions(-)", "").replace("deletion(-)", "")
            )
    return ans


def git_insertions(repo_folder):
    for ending in ("/.git", "/.git/"):
        if repo_folder.endswith(ending):
            repo_folder = repo_folder[: -len(ending)]

    cmd = f"""git log --pretty=tformat:%H --shortstat --all"""
    cmd_l = cmd.split(" ")

    cmd_output = subprocess.check_output(cmd_l, cwd=repo_folder).decode("utf-8")
    cmd_output = cmd_output.replace("\n\n", " ")
    # cmd_output = cmd_output.replace(" files changed, ", " ")
    # cmd_output = cmd_output.replace(" file changed, ", " ")
    # cmd_output = cmd_output.replace(" insertions(+), ", " ")
    # cmd_output = cmd_output.replace(" insertion(+), ", " ")
    # cmd_output = cmd_output.replace(" deletions(-)", " ")
    # cmd_output = cmd_output.replace(" deletion(-)", " ")
    cmd_output = cmd_output.replace("  ", " ").replace("'", "")

    ans = {
        p.split(" ")[0]: parse_stat_line(p) for p in cmd_output.split("\n") if p != ""
    }
    return ans


def extract_fields(cmd_output, nb_fields, delim_field, delim_line="\n"):
    partial_line = False
    for line in cmd_output.split(delim_line):
        if not partial_line:
            parsed_line = line.split(delim_field)
        else:
            extra_line = line.split(delim_field)
            parsed_line[-1] += delim_line
            parsed_line[-1] += extra_line[0]
            parsed_line += extra_line[1:]

        if len(parsed_line) == nb_fields:
            yield parsed_line
        else:
            partial_line = True


class CommitsGitLogFiller(commit_info.CommitsFiller):
    def git_log(
        self,
        repo_folder,
        nb_commits,
        delim_field=None,
        delim_line="\n",
        delim_length=10,
        rerun=True,
        field_names=default_field_names,
    ):
        if delim_field is None:
            delim_field = (
                "+"
                + "".join(
                    random.choices(
                        string.ascii_letters + string.digits,
                        k=delim_length,
                    )
                )
                + "+"
            )
        for ending in ("/.git", "/.git/"):
            if repo_folder.endswith(ending):
                repo_folder = repo_folder[: -len(ending)]

        cmd = f"""git log --pretty=format:{delim_field.join([FIELDS[f] for f in field_names])} --all"""
        cmd_l = cmd.split(" ")
        cmd_output = subprocess.check_output(cmd_l, cwd=repo_folder).decode("utf-8")
        ans = [
            {k: v for k, v in zip(field_names, l)}
            for l in extract_fields(
                cmd_output=cmd_output,
                nb_fields=len(field_names),
                delim_field=delim_field,
                delim_line=delim_line,
            )
        ]
        if len(ans) != nb_commits:
            if rerun:
                self.logger.info(
                    f"Wrong number of commits parsed: {len(ans)} instead of {nb_commits}, rerunning git log command with another delimiter"
                )
                return self.git_log(
                    repo_folder=repo_folder,
                    delim_field=None,
                    delim_line=delim_line,
                    delim_length=2 * delim_length,
                    rerun=False,
                    nb_commits=nb_commits,
                )
            else:
                raise IOError(
                    f"Wrong number of commits parsed: {len(ans)} instead of {nb_commits}, {[(a['sha'] if 'sha' in a.keys() else a) for a in ans]}"
                )
        return ans

    def list_commits(
        self,
        name,
        source,
        owner,
        basic_info_only=False,
        repo_id=None,
        after_time=None,
        allbranches=False,
        group_by="sha",
        remote_branches_only=True,
    ):
        """
        Listing the commits of a repository
        if after time is set to an int (unix time def) or datetime.datetime instead of None, only commits strictly after given time. Commits are listed by default from most recent to least.
        """
        if isinstance(after_time, datetime.datetime):
            after_time = datetime.datetime.timestamp(after_time)

        repo_obj = self.get_repo(source=source, name=name, owner=owner, engine="pygit2")
        if (
            repo_id is None
        ):  # Letting the possibility to preset repo_id to avoid cursor recursive usage
            repo_id = self.db.get_repo_id(source=source, name=name, owner=owner)

        if not repo_obj.is_empty:
            cmd = "git log --format=%H --all"
            cmd_l = cmd.split(" ")
            repo_folder = repo_obj.path
            for ending in ("/.git", "/.git/"):
                if repo_folder.endswith(ending):
                    repo_folder = repo_folder[: -len(ending)]

            cmd_output = subprocess.check_output(cmd_l, cwd=repo_folder).decode("utf-8")
            commit_sha_list = [s for s in cmd_output.split("\n") if s != ""]
            nb_commits = len(commit_sha_list)
            # def process_commit(commit):
            #     if isinstance(commit, dict):
            #         commit = repo_obj.get(commit["sha"])

            commits = self.git_log(repo_folder=repo_folder, nb_commits=nb_commits)
            parent_info = git_parents(repo_folder=repo_folder)
            if basic_info_only:
                for commit_info in commits:
                    complete_commit_info(
                        info=commit_info, repo_id=repo_id, parent_info=parent_info
                    )
            else:
                insertion_info = git_insertions(repo_folder=repo_folder)
                for commit_info in commits:
                    complete_commit_info(
                        info=commit_info,
                        repo_id=repo_id,
                        parent_info=parent_info,
                        insertion_info=insertion_info,
                    )
            return commits
