import os
import importlib

def test_basic():
	assert True

def test_import():
	path = os.path.abspath(__file__)
	dir_name = os.path.dirname(os.path.dirname(os.path.dirname(path)))
	with open(os.path.join(dir_name,'setup.py'),'r') as f:
		file_content = f.read()
	libname = file_content.split('setup(')[1].split("name='")[1].split("'")[0]
	importlib.import_module(libname)

