#!/usr/bin/env python

import re
import sys

from setuptools import setup, find_packages


def version():
    with open('repo_tools/_version.py') as f:
        return re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", f.read()).group(1)

def requirements():
  with open('requirements.txt') as f:
    return f.readlines()

setup(name='repo_tools',
      version=version(),
      packages=['repo_tools'],#find_packages(),
      install_requires=[requirements()],
      author='',
      author_email='',
      description='',
      url='',
      include_package_data=True,
      license='GNU AFFERO GENERAL PUBLIC LICENSE Version 3',
      )
