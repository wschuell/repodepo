#!/bin/python
import subprocess
import os

libname = 


with open('setup.py','r') as f:
	txt = f.read()

txt2 = libname.join(txt.split('PYLIB'))

with open('setup.py','w') as f:
	f.write(txt2)



print('Edit setup.py if you want to provide URL, author info, and description')

cmd_list = [
		'git add PYLIB/__init__.py',
		'git add PYLIB/_version.py',
		'git mv PYLIB '+libname,
		'git add tests/testmodule/__init__.py',
		'git add tests/testmodule/test_basic.py',
		'git add .gitignore',
		'git add .travis.yml',
		'git add pytest.ini',
		'git add setup.py',
		'git add requirements.txt',
		"git commit -am 'Deploying_pylib'",

		]

for cmd in cmd_list:
	print('--------------')
	print(cmd)
	print('')
	cmd2 = cmd.split(' ')
	ans = subprocess.check_output(cmd2)
	print(ans)


print("To finish the process, you can execute 'git rm deploy_pylib.py'")