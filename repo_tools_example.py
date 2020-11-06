import repo_tools as rp
import os
import time

rp_folder= 'repo_tools_example'


repo_list_test =  [
	'https://github.com/serde-rs/serde',
'https://github.com/rust-random/rand',
'https://github.com/dtolnay/syn',
'https://github.com/rust-lang/libc',
'https://github.com/dtolnay/quote',
'https://github.com/bitflags/bitflags',
'https://github.com/rust-lang-nursery/lazy-static.rs',
'https://github.com/rust-lang/log',
'https://github.com/serde-rs/serde',
'https://github.com/unicode-rs/unicode-xid',
'https://github.com/rust-num/num-traits',

	]


rc = rp.repo_crawler.RepoCrawler(folder=rp_folder,ssh_mode=False)
#rc = rp.repo_crawler.RepoCrawler(folder=rb_folder,ssh_mode=True,ssh_key='{}/.ssh/github/id_rsa'.format(os.environ['HOME']))# if you have an SSH key to connect to github, may be faster for cloning
rc.db.register_source(source='GitHub',source_urlroot='github.com')
rc.add_all_from_folder() # checks if in the folder there are already cloned packages
rc.add_list(repo_list_test,source='GitHub',source_urlroot='github.com')
rc.clone_all()

if os.path.exists('github_api_keys.txt'):
	rc.set_github_requesters(api_keys_file='github_api_keys.txt') # create a file with this name with one API key per line
else:
	print("No API keys file found, using unauthenticated mode only. You can also put the API keys in a file called github_api_keys.txt, one per line. Look at where this message is in the code for more info. Will continue in 10s.")
	time.sleep(10)
	rc.set_github_requesters()


rc.fill_commit_info()
rc.fill_stars(workers=5)
rc.fill_gh_logins(workers=5)
rc.fill_followers(workers=5)

