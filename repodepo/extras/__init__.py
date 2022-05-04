

def check_sqlname_safe(s):
	assert s == ''.join( c for c in s if c.isalnum() or c in ('_',) ), '{} is not passing the check against SQL injection'.format(s)

from .anonymization import anonymize
from .stats import GlobalStats
