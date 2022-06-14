import os


def homepath():
    if os.name == 'nt':  # for a windows pc
        return 'HOMEPATH'
    else:
        return 'HOME'
