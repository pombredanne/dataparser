__author__ = 'jisun'

import numpy as np
import pandas as pd
from sqlalchemy import *
import pickle
import time


def idgenerate(datafile):

    df = pd.read_csv(datafile)
    idpairs = {'source': [], 'target': [],
               'slon': [], 'slat': [],
               'tlon': [], 'tlat': []}

    # pair = []
    # notpair = []
    point2id = pickle.load(open('/home/jisun/workspace/pycode/TomTom/data/tmp/point2id.pcl'))
    ids = {}
    for k, i in enumerate(list(set(point2id.values()))):
        ids[''.join(i)] = k + 1
    print 'All together id numbers: ', len(ids)
    # for datarow in df[['source_lon', 'source_lat', 'target_lon', 'target_lat', 'distance']].iterrows():
    #     ti = datarow[1]
    #     tmp1 = (repr(ti['source_lon']), repr(ti['source_lat']))
    #     tmp2 = (repr(ti['target_lon']), repr(ti['target_lat']))
    #     dis = ti['distance']
    #     if (dis < 1000) and (('{:.3f}'.format(float(tmp1[0])) in '{:.3f}'.format(float(tmp2[0]))) and
    #         ('{:.3f}'.format(float(tmp1[1])) in '{:.3f}'.format(float(tmp2[1])))):
    #         pair.append(tmp1)
    #         pair.append(tmp2)
    #     else:
    #         if tmp1 not in pair:
    #             notpair.append(tmp1)
    #         if tmp2 not in pair:
    #             notpair.append(tmp2)
    # keydict = {}

    # for x, y in set(pair):
    #     point2id[x + y] = (x, y)
    #
    # for x, y in (set(notpair) - set(pair)):
    #     tmpkey = '{:.3f}'.format(float(x)) + '{:.3f}'.format(float(y))
    #     if tmpkey not in keydict:
    #         keydict[tmpkey] = (x, y)
    #     point2id[x + y] = keydict[tmpkey]
    #
    # print len(point2id.values())
    # idsxy = np.unique(point2id.values())
    # print idsxy[1]

    # ids = {} #transfer the x y in to id
    # for i, (x, y) in enumerate(idsxy):
    #     ids[''.join((x, y))] = i+1
    # print len(ids.keys())

    for datarow in df[['source_lon', 'source_lat', 'target_lon', 'target_lat']].iterrows():
        ti = datarow[1]
        slon = '{:.5f}'.format(ti['source_lon'])
        slat = '{:.5f}'.format(ti['source_lat'])
        s = point2id[slon+slat]
        tlon = '{:.5f}'.format(ti['target_lon'])
        tlat = '{:.5f}'.format(ti['target_lat'])
        t = point2id[tlon+tlat]
        idpairs['source'].append(ids[''.join(s)])
        idpairs['target'].append(ids[''.join(t)])
        idpairs['slon'].append(s[0])
        idpairs['slat'].append(s[1])
        idpairs['tlon'].append(t[0])
        idpairs['tlat'].append(t[1])
    df1 = df.join(pd.DataFrame(idpairs))
    df1.to_csv(datafile[:-4] + '_ids.csv')


def build_sidtable():
    """
    The id in side point2id is {:.5f} format string for both lon and lat
    """
    engine = create_engine('postgresql://postgres:dsim@localhost:5432/TomTom', echo=True)
    metadata = MetaData()
    short_id = Table('short_id5d', metadata,
                     Column('strId', String(60), primary_key=True),
                     Column('rlon', String(30), nullable=False),
                     Column('rlat', String(30), nullable=False),
                     Column('node_id', Integer)
                     )

    metadata.create_all(engine)

    #piont2id contains all the lon, lat -> (x, y) as id
    point2id = pickle.load(open('/home/jisun/workspace/pycode/TomTom/data/tmp/point2id.pcl', 'rb'))
    idsxy = np.unique(point2id.values())
    ids = {} #transfer the x y in to id
    for i, (x, y) in enumerate(idsxy):
        ids[x+y] = i+1

    # print np.min(ids.values())
    conn = engine.connect()
    count = 0
    for k in point2id:
        ins = short_id.insert()
        conn.execute(ins, strId=k,
                     rlon=point2id[k][0],
                     rlat=point2id[k][1],
                     node_id=ids[''.join(point2id[k])]
                     )
        count += 1
    conn.close()
    return count


def time2int(datafile):
    df = pd.read_csv(datafile)
    # rt = df['relative_time']
    # rtmin = rt.min()
    # rrt = (rt-rtmin).apply(round, convert_dtype=True)
    # rrtint = rrt.apply(int, convert_dtype=True)
    df1 = df[['source', 'target', 'averagespeed', 'traveltime', 'distance', 'relative_time']]
    df1.to_csv(datafile[:-4]+'v1.csv')


if __name__ == '__main__':
    start = time.time()
    # datafile = '/home/jisun/workspace/pycode/TomTom/data/TT0521_12to15_allpostive.csv'
    # idgenerate(datafile)
    # count = build_sidtable()

    datafile = '/home/jisun/workspace/pycode/TomTom/data/TT0521_12to15_allpostive_ids.csv'
    time2int(datafile)
    print('{:*^30}, all together.'.format('Done!'))
    end = time.time()
    print('Time used: {:.2f} seconds.'.format(end-start))

