#!/usr/bin/env python

import re
import sys
import os

from setuptools import setup, find_packages


def version():
    with open('repodepo/_version.py') as f:
        return re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", f.read()).group(1)

def requirements():
  with open('requirements.txt') as f:
    return f.readlines()

if os.path.exists(os.path.join(os.path.dirname(__file__),'README.md')):
    with open(os.path.join(os.path.dirname(__file__),'README.md'),'r') as f:
        README = f.read()
else:
    README = ''

setup(name='repodepo',
      version=version(),
      packages=['repodepo'],#find_packages(),
      install_requires=[requirements()],
      author='William Schueller',
      author_email='',
      description='Set of tools to build datasets about repositories',
      long_description_content_type="text/markdown",
      long_description=README,
      url='https://github.com/wschuell/repodepo',
      include_package_data=True,
      license='GNU AFFERO GENERAL PUBLIC LICENSE Version 3',
      )
