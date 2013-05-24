__author__ = 'jisun'

import pandas as pd
import subprocess
from glob import glob
import numpy as np
import zipfile
import os
import os.path as osp
import ast


def ch_time_id():
    datafile = '/home/jisun/workspace/pycode/TomTom/data/tmp/tt0514timeid.csv'
    df = pd.read_csv(datafile)
    rt = []
    for i in df.record_time:
        rt.append(i[-8:-6])
    print len(set(rt))
    rtcount = {}
    for i in set(rt):
        rtcount[i] = 0
        for j in df.record_time:
            if j[-8:-6] in i:
                rtcount[i] += 1
        rtcount[i] /= 6945
    print rtcount
    return 0

if __name__ == '__main__':
    ch_time_id()
