__author__ = 'jisun'

from sqlalchemy import *
from sqlalchemy import types
from glob import glob
import os
import os.path as osp
import numpy as np
import pandas as pd
import ast
from datex2 import readfromzip
from xml.etree import ElementTree
from datetime import datetime
import pickle


def definetable(select, tablename):
    engine = create_engine('postgresql://postgres:dsim@localhost:5432/TomTom', echo=True)
    metadata = MetaData()
    if 'HD' in select:
        HD_Flow = Table(tablename, metadata,
                        Column('record_time', types.TIMESTAMP(timezone=False)),
                        Column('id', String(50), nullable=False),
                        Column('openlr', String(64), nullable=False),
                        Column('averageSpeed', Integer),
                        Column('travelTime', Integer),
                        Column('freeFlowSpeed', Integer),
                        Column('freeFlowTravelTime', Integer)
                        )

    if 'OpenLR' in select:
        openlrs = Table('OpenLR', metadata,
                        Column('id', String(50), primary_key=True),
                        Column('openlr', String(64), nullable=False),
                        Column('source_lon', String(30), nullable=False),
                        Column('source_lat', String(30), nullable=False),
                        Column('target_lon', String(30), nullable=False),
                        Column('target_lat', String(30), nullable=False),
                        Column('bear1', Integer),
                        Column('bear2', Integer),
                        Column('fow1', String(25)),
                        Column('fow2', String(25)),
                        Column('frc1', String(4)),
                        Column('frc2', String(4)),
                        Column('lfrcnp1', String(4)),
                        Column('dnp1', Integer)
                        )

    metadata.create_all(engine)
    return 0


def inserOpenlr(df):
    engine = create_engine('postgresql://postgres:dsim@localhost:5432/TomTom', echo=True)
    conn = engine.connect()
    metadata = MetaData()
    openlrs = Table('OpenLR', metadata,
                    Column('id', String(50), primary_key=True),
                    Column('openlr', String(64), nullable=False),
                    Column('source_lon', String(30), nullable=False),
                    Column('source_lat', String(30), nullable=False),
                    Column('target_lon', String(30), nullable=False),
                    Column('target_lat', String(30), nullable=False),
                    Column('bear1', Integer),
                    Column('bear2', Integer),
                    Column('fow1', String(25)),
                    Column('fow2', String(25)),
                    Column('frc1', String(4)),
                    Column('frc2', String(4)),
                    Column('lfrcnp1', String(4)),
                    Column('dnp1', Integer)
                    )

    for datarow in df.iterrows():
        ti = datarow[1]
        ins = openlrs.insert()
        conn.execute(ins, id=ti['id'],
                     openlr=ti['openlr'],
                     source_lon=ast.literal_eval(ti['p1'])[0],
                     source_lat=ast.literal_eval(ti['p1'])[1],
                     target_lon=ast.literal_eval(ti['p2'])[0],
                     target_lat=ast.literal_eval(ti['p2'])[1],
                     bear1=ti['bear1'],
                     bear2=ti['bear2'],
                     fow1=ti['fow1'],
                     fow2=ti['fow2'],
                     frc1=ti['frc1'],
                     frc2=ti['frc2'],
                     lfrcnp1=ti['lfrcnp1'],
                     dnp1=ti['ndp1']
                     )
    conn.close()
    return 0


def empty(x):
    if x:
        return int(ast.literal_eval(x))
    else:
        return -1


def insertHD(df, timestamp, tablename):
    engine = create_engine('postgresql://postgres:dsim@localhost:5432/TomTom')
    conn = engine.connect()
    metadata = MetaData()
    #alter table "HD_Flow_Real_Time_Traffic" add primary key (record_time, id);
    HD_Flow = Table(tablename, metadata,
                    Column('record_time', types.TIMESTAMP(timezone=False)),
                    Column('id', String(50), nullable=False),
                    Column('openlr', String(64), nullable=False),
                    Column('averageSpeed', Integer),
                    Column('travelTime', Integer),
                    Column('freeFlowSpeed', Integer),
                    Column('freeFlowTravelTime', Integer)
                    )
    for datarow in df.iterrows():
        ti = datarow[1]
        ins = HD_Flow.insert()

        conn.execute(ins, record_time=timestamp,
                     id=ti['id'],
                     openlr=ti['openlr'],
                     averageSpeed=empty(ti['averageSpeed']),
                     travelTime=empty(ti['travelTime']),
                     freeFlowSpeed=empty(ti['freeFlowSpeed']),
                     freeFlowTravelTime=empty(ti['freeFlowTravelTime'])
                     )
    conn.close()
    return 0


def extractCorruptTime(corrlist):
    corrlist = ['content.xml.130511_131305.zip',
                 'content.xml.130511_131005.zip',
                 'content.xml.130512_032317.zip',
                 'content.xml.130512_032020.zip',
                 'content.xml.130512_032341.zip',
                 'content.xml.130512_032043.zip',
                 'content.xml.130513_020834.zip',
                 'content.xml.130513_020812.zip',
                 'content.xml.130513_020836.zip',
                 'content.xml.130513_020856.zip',
                 'content.xml.130513_020800.zip',
                 'content.xml.130513_020525.zip',
                 'content.xml.130513_020855.zip']
    return [t.split('.')[-2].split('_')[-1] for t in corrlist]


def findCorruptlist(datafolder):
    """
    return the corruptfile base name to extract exact time
    At present corruptlist are:
    ['131305', '131005', '032317', '032020', '032341', '032043',
    '020834', '020812', '020836', '020856', '020800', '020525', '020855']
    """
    datafolder = '/home/jisun/workspace/nictamnt/tomtom/HD_Flow_Real_Time_Traffic'
    dates = [str(d) for d in range(511, 515)]
    corruptfiles = []
    for d in dates:
        fflistzip = set(glob(osp.join(datafolder, '*.130' + '%s' % d + '_*.zip')))
        ffsizezip = []
        for ff in fflistzip:
            ffsizezip.append(os.stat(ff).st_size)
        meansize = np.mean(ffsizezip)/3
        for ff, ss in zip(fflistzip, ffsizezip):
            if ss < meansize:
                corruptfiles.append(ff)
    return [osp.basename(bsname) for bsname in corruptfiles]


def datex2_content2(xmlcontent):
    """
    xmlfile is the file for content
    """
    # tree = ElementTree.parse('data/content2.xml')

    root = ElementTree.fromstring(xmlcontent)
    xmlns = '{http://datex2.eu/schema/1_0/1_0}'
    xsins = '{http://www.w3.org/2001/XMLSchema-instance}'
    ids = []
    openlrs = []

    travelTime = []
    averageSpeed = []

    freeFlowSpeed = []
    freeFlowTravelTime = []
    count = 1
    for i in root.findall('.//' + xmlns + 'elaboratedData'):
        ids.append(i.attrib['id'])
        for child in i:
            openlrs.append(child.findall('.//' + xmlns + 'binary')[0].text)
            # print 'DataQuality: %s' % child.findall('.//' + xmlns + 'supplierCalculatedDataQuality')[0].text
            if len(child.findall('.//' + xmlns + 'travelTime')) > 0:
                travelTime.append(child.findall('.//' + xmlns + 'travelTime')[0].text)
                averageSpeed.append(child.findall('.//' + xmlns + 'averageSpeed')[0].text)
            else:
                travelTime.append('')
                averageSpeed.append('')

            if len(child.findall('.//' + xmlns + 'freeFlowSpeed')) > 0:
                freeFlowSpeed.append(child.findall('.//' + xmlns + 'freeFlowSpeed')[0].text)
                freeFlowTravelTime.append(child.findall('.//' + xmlns + 'freeFlowTravelTime')[0].text)
            else:
                freeFlowSpeed.append('')
                freeFlowTravelTime.append('')
        count += 1
    df = pd.DataFrame({'id': ids, 'openlr': openlrs,
                       'travelTime': travelTime, 'averageSpeed': averageSpeed,
                       'freeFlowSpeed': freeFlowSpeed, 'freeFlowTravelTime': freeFlowTravelTime})
    return df

# move to idgen.py temporarily!!!
# def dataout():
#     point2id = pickle.load(open('/home/jisun/workspace/pycode/TomTom/data/tmp/point2id.pcl', 'rb'))
#     idsxy = np.unique(point2id.values())
#     print idsxy[1]
#     ids = {} #transfer the x y in to id
#     for i, (x, y) in enumerate(idsxy):
#         ids[''.join((x, y))] = i+1
#     print len(ids.keys())
#
#     df = pd.read_csv('/home/jisun/workspace/pycode/TomTom/data/TT0514_0800to0830.csv')
#     idpairs = {'source': [], 'target': [],
#                'slon': [], 'slat': [],
#                'tlon': [], 'tlat': []}
#     for datarow in df[['source_lon', 'source_lat', 'target_lon', 'target_lat']].iterrows():
#         ti = datarow[1]
#         slon = repr(ti['source_lon'])
#         slat = repr(ti['source_lat'])
#         s = point2id[slon+slat]
#         print s
#         tlon = repr(ti['target_lon'])
#         tlat = repr(ti['target_lat'])
#         t = point2id[tlon+tlat]
#         print t
#         idpairs['source'].append(ids[''.join(s)])
#         idpairs['target'].append(ids[''.join(t)])
#         idpairs['slon'].append(s[0])
#         idpairs['slat'].append(s[1])
#         idpairs['tlon'].append(t[0])
#         idpairs['tlat'].append(t[1])
#     df1 = df[['id', 'averageSpeed', 'travelTime', 'dnp1']].join(pd.DataFrame(idpairs))
#     df1.to_csv('/home/jisun/workspace/pycode/TomTom/data/TT0514_0800to0830_ids.csv')
#


if __name__ == '__main__':

    # selectdatafile()
    # datafolder = '/home/jisun/workspace/pycode/TomTom/data/c2select'
    # datafile = 'content.xml.130513_121829_locs.csv'
    # df = pd.DataFrame.from_csv(osp.join(datafolder, datafile))
    # print df.dtypes
    # for i in df[:5].iterrows():
    #
    #     print type(ast.literal_eval(i[1]['p1']))

    # print extractCorruptTime([])
    #inserOpenlr(df)

    datafolder = '/home/jisun/workspace/nictamnt/tomtom/HD_Flow_Real_Time_Traffic'
    # # dates = [str(d) for d in range(511, 515)]
    #
    # datafolder = '/home/jisun/workspace/pycode/TomTom/data/TT0514/*.zip'
    # fflistzip = glob(datafolder)
    tablename = 'TomtomGoodHD'
    #definetable('HD', tablename)
    # hr = ['12', '13', '14']
    # define the file names
    # datafolder = '/home/jisun/workspace/pycode/TomTom/data/TT0521_12to15'
    # fpieces = ['content.xml.130521_{:s}*.zip'.format(hri) for hri in hr]
    # fflistzip = []
    # for fpiece in fpieces:
    #     fflistzip.extend(glob(osp.join(datafolder, fpiece)))
    fflistzip = glob(osp.join(datafolder, '*.zip'))
    # print len(fflistzip)
    year = 2013
    for ff in fflistzip:
        basename = osp.basename(ff)
        timestr = basename.split('.')[-2].split('_')
        timestamp = datetime(year, int(timestr[0][2:4]),
                             int(timestr[0][4:]),
                             int(timestr[1][:2]),
                             int(timestr[1][2:4]),
                             int(timestr[1][4:]))
        if os.stat(ff).st_size > 0:
            try:
                xmlcontent = readfromzip(ff)
                df = datex2_content2(xmlcontent)
                insertHD(df, timestamp, tablename)
            except:
                print 'parsing error: {:s}'.format(ff)
                continue
        else:
            continue







