pandas_freq = {
	'month':'MS',
	'year':'YS',
	'day':'D',
	'week':'W-MON',
}

import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

from .generic_getters import Getter


def round_datetime_upper(dt,time_window,strict=False):
	if not isinstance(dt,datetime.datetime):
		dt = pd.to_datetime(dt).to_pydatetime()
	if time_window == 'year':
		if dt == datetime.datetime(dt.year,1,1) and not strict:
			return dt
		else:
			return datetime.datetime(dt.year,1,1) + relativedelta(years=1)
	elif time_window == 'month':
		if dt == datetime.datetime(dt.year,dt.month,1) and not strict:
			return dt
		else:
			return datetime.datetime(dt.year,dt.month,1) + relativedelta(months=1)
	elif time_window == 'week':
		if dt == datetime.datetime(dt.year,dt.month,dt.day) and not strict and dt.weekday()==0:
			return dt
		else:
			dt = dt + datetime.timedelta(days=1)
			while dt.weekday() != 0:
				dt = dt + datetime.timedelta(days=1)
			return dt
	elif time_window == 'day':
		if dt == datetime.datetime(dt.year,dt.month,dt.day) and not strict:
			return dt
		else:
			return datetime.datetime(dt.year,dt.month,dt.day) + datetime.timedelta(days=1)
	else:
		raise NotImplementedError('Rounding of datetime not implemented for time window: {}'.format(time_window))
