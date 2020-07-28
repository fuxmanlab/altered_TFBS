# Some useful functions

import time
import os
import multiprocessing

def timeit(method):
    ''' A timing decorator '''
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('%r  %2.2f sec' % (method.__name__, (te - ts) ))
        return result

    return timed


def get_n_cores(reserve=0):
    ''' Get a reasonable number of cores to run with'''
    if 'NSLOTS' in os.environ:
        ncores = int(os.environ['NSLOTS'])
    else:
        ncores = multiprocessing.cpu_count()
    ncores = ncores - reserve
    if ncores <= 0:
        return 1
    return ncores
