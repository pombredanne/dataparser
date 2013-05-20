__author__ = 'jisun'
from xml.etree import ElementTree
import pandas as pd
import subprocess
from glob import glob
import numpy as np
import zipfile
import os
import os.path as osp
import ast


def datex2_content2(xmlfile):
    """
    xmlfile is the file for content
    """
    # tree = ElementTree.parse('data/content2.xml')

    tree = ElementTree.parse(xmlfile)
    root = tree.getroot()
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


def datex2_content1(xmlfile):
    """
    Situation also have position
    """
    # cmd = ['/home/jisun/workspace/pycode/TomTom/dev/otk-1.4.0/otk.sh', 'convert', '-if', 'BINARY64', '-i']
    # rest = ['C2t2F+fmyRNYBf7eAF4TCg==', '-of', 'XML']
    # tmp = subprocess.check_output(cmd + rest)
    # print tmp
    # #tmp is xml string parse it with parselocXML
    tree = ElementTree.parse(xmlfile)
    root = tree.getroot()
    xmlns = '{http://datex2.eu/schema/1_0/1_0}'
    xsins = '{http://www.w3.org/2001/XMLSchema-instance}'
    ids = []
    openlrs = []
    for i in root.findall('.//' + xmlns + 'situationRecord'):
        ids.append(i.attrib['id'])
        # print i.findall('.//' + xmlns + 'openlr/' + xmlns + 'binary')[0].text
        openlrs.append(i.findall('.//' + xmlns + 'openlr/' + xmlns + 'binary')[0].text)
    return pd.DataFrame({'id': ids,
                         'openlr': openlrs})


def parselocXML(xmlstr):
    xmlns = '{http://www.openlr.org/openlr}'
    root = ElementTree.fromstring(xmlstr)
    lons = [lon.text for lon in root.findall('.//' + xmlns + 'Longitude')]
    lats = [lat.text for lat in root.findall('.//' + xmlns + 'Latitude')]
    frcs = [frc.text for frc in root.findall('.//' + xmlns + 'FRC')]
    fows = [fow.text for fow in root.findall('.//' + xmlns + 'FOW')]
    bears = [bear.text for bear in root.findall('.//' + xmlns + 'BEAR')]
    lfrcnp = root.findall('.//' + xmlns + 'LFRCNP')[0].text
    ndp = root.findall('.//' + xmlns + 'DNP')[0].text
    p1 = {}
    p2 = {}

    p1['coord'] = (lons[0], lats[0])
    p2['coord'] = (lons[1], lats[1])

    p1['frc'] = frcs[0]
    p2['frc'] = frcs[1]

    p1['fow'] = fows[0]
    p2['fow'] = fows[1]

    p1['bear'] = bears[0]
    p2['bear'] = bears[1]

    p1['lfrcnp'] = lfrcnp
    p1['ndp'] = ndp

    return p1, p2


def output_locNodes(df):
    cmd = ['/home/jisun/workspace/pycode/TomTom/dev/otk-1.4.0/otk.sh', 'convert', '-if', 'BINARY64', '-i']
    datadict = {'id': [],
                'openlr': [],
                'p1': [],
                'frc1': [],
                'fow1': [],
                'bear1': [],
                'lfrcnp1': [],
                'ndp1': [],
                'p2': [],
                'frc2': [],
                'fow2': [],
                'bear2': []}

    for i in df.index:
        try:
            openlrid = df.get_value(i, 'id')
            openlr = df.get_value(i, 'openlr')
            tmp = subprocess.check_output(cmd + [openlr, '-of', 'XML'])
            p1, p2 = parselocXML(tmp)
            datadict['id'].append(openlrid)
            datadict['openlr'].append(openlr)
            datadict['p1'].append(p1['coord'])
            datadict['frc1'].append(p1['frc'])
            datadict['fow1'].append(p1['fow'])
            datadict['bear1'].append(p1['bear'])
            datadict['lfrcnp1'].append(p1['lfrcnp'])
            datadict['dnp1'].append(p1['ndp'])
            datadict['p2'].append(p2['coord'])
            datadict['frc2'].append(p2['frc'])
            datadict['fow2'].append(p2['fow'])
            datadict['bear2'].append(p2['bear'])

        except:
            continue

    df2 = pd.DataFrame(datadict)
    return df2


def readfromzip(zipped):
    z = zipfile.ZipFile(zipped, 'r')
    for filename in z.namelist():
        xmlstr = z.read(filename)
    return xmlstr


def datafoldercheck(datafolder):
    """
    Give the basic data folder statistics
    """
    # datafolder = '/home/jisun/workspace/nictamnt/tomtom/HD_Flow_Real_Time_Traffic'
    dates = [str(d) for d in range(510, 518)]
    for d in dates:
        fflistzip = set(glob(osp.join(datafolder, '*.130'+ '%s' %d +'_*.zip')))
        fflistxml = set(glob(osp.join(datafolder, '*.130'+ '%s' %d +'_*')))
        ffsizezip = []
        ffsizexml = []
        for ff in fflistzip:
            ffsizezip.append(os.stat(ff).st_size)
        for ff in (fflistxml - fflistzip):
            ffsizexml.append(os.stat(ff).st_size)
        print 'Day %s ziped: %g' % (d, len(fflistzip))
        if len(ffsizezip) > 0:
            print 'min: %.2f, max: %.2f' % (np.min(ffsizezip), np.max(ffsizezip))
        else:
            print 'No zip'

        print 'Day %s xml: %g' % (d, len(fflistxml))
        if len(ffsizexml) > 0:
            print 'min: %.2f, max %.2f' % (np.min(ffsizexml), np.max(ffsizexml))
        else:
            print 'No xml'
        print


def validate_locations():
    #===============================================================
    c2list = glob('/home/jisun/workspace/pycode/TomTom/data/c2/*')
    ids = []
    openlrs = []
    for ff in c2list:
        print ff
        if os.stat(ff).st_size >= 4257714:
            df = datex2_content2(ff)
            openlrs.extend(df.openlr.values)
            ids.extend(df.id.values)
        else:
            print '%s is empty' % ff
            continue

    dfc2 = pd.DataFrame({'id': ids, 'openlr': openlrs})
    dfc2.save('/home/jisun/workspace/pycode/TomTom/data/c2.data')
    dfc2.to_csv('/home/jisun/workspace/pycode/TomTom/data/c2.csv')
    openlrs = np.unique(openlrs)
    ids = np.unique(ids)
    print 'c2 unique opnelr: ', len(openlrs)
    print 'c2 unique ids :', len(ids)

    # c1list = glob('/home/jisun/workspace/pycode/TomTom/data/c1/*')
    # ids = []
    # openlrs = []
    # for ff in c1list:
    #     print ff
    #     if os.stat(ff).st_size > 0:
    #         df = datex2_content1(ff)
    #         openlrs.extend(df.openlr.values)
    #         ids.extend(df.id.values)
    #     else:
    #         print '%s is empty' % ff
    #         continue
    #
    # dfc1 = pd.DataFrame({'id': ids, 'openlr': openlrs})
    # dfc1.save('/home/jisun/workspace/pycode/TomTom/data/c1.data')
    # dfc1.to_csv('/home/jisun/workspace/pycode/TomTom/data/c1.csv')
    # print 'c1 unique openlr', len(np.unique(openlrs))
    # print 'c1 unique id', len(np.unique(ids))


def unzipdata(folder, savefolder):
    filelist = glob(folder + '/*.zip')
    for i in filelist:
        with zipfile.ZipFile(i) as myzip:
            myzip.extractall(savefolder)
            print osp.basename(i)
            old_name = osp.join(savefolder, 'content.xml')
            new_name = osp.join(savefolder, osp.basename(i)[:-4])
            os.rename(old_name, new_name)
    print 'Done!'


def paircheck(datafolder):
    fflist = glob(datafolder + '*_tensorV1.csv')
    accuracy = '%.4f'
    for ff in fflist:
        df = pd.read_csv(ff)
        lp1lon = []
        lp1lat = []
        lp2lon = []
        lp2lat = []
        sp1lon = []
        sp1lat = []
        sp2lon = []
        sp2lat = []

        for p1, p2 in df[['0p1', '0p2']].values:
            tmp1 = ast.literal_eval(p1)
            tmp2 = ast.literal_eval(p2)
            # tmp1 = ast.literal_eval(tmp1[0]), ast.literal_eval(tmp1[1])
            # tmp2 = ast.literal_eval(tmp2[0]), ast.literal_eval(tmp2[1])
            lp1lon.append(tmp1[0])
            lp1lat.append(tmp1[1])
            lp2lon.append(tmp2[0])
            lp2lat.append(tmp2[1])

            sp1lon.append(accuracy % float(tmp1[0]))
            sp1lat.append(accuracy % float(tmp1[1]))
            sp2lon.append(accuracy % float(tmp2[0]))
            sp2lat.append(accuracy % float(tmp2[1]))

        df1 = pd.DataFrame({'id': range(1, len(lp1lon)+1),
                           'lp1lon': lp1lon,
                           'lp1lat': lp1lat,
                           'lp2lon': lp2lon,
                           'lp2lat': lp2lat,
                           'sp1lon': sp1lon,
                           'sp1lat': sp1lat,
                           'sp2lon': sp2lon,
                           'sp2lat': sp2lat})
        df1.to_csv(ff[:-4] + '_point_pair.csv')

if __name__ == '__main__':
    # c2_xmlfile = u'/home/jisun/workspace/pycode/TomTom/data/content2.xml'
    # c1_xmlfile = u'/home/jisun/workspace/pycode/TomTom/data/content1.xml'
    # df = datex2_content2(xmlfile)
    # # df.to_csv(xmlfile[:-4] + '_locations.csv')
    # df = pd.read_csv(xmlfile[:-4] + 'locations.csv')
    # print df
    # results = []
    # resultslen = []
    # df1 = output_locNodes(df)
    # df1.save(c2_xmlfile[:-4] + '_locations_pol.data')
    # df1.to_csv(c2_xmlfile[:-4] + '_locations_pol.csv')

    #datex2_content1(c1_xmlfile)


    # zipc2 = '/home/jisun/workspace/nictamnt/tomtom/HD_Flow_Real_Time_Traffic'
    # zipc2 = '/home/jisun/workspace/pycode/TomTom/data/c2/'
    # savec2 = '/home/jisun/workspace/pycode/TomTom/data/c2/'
    #
    # unzipdata(zipc2, savec2)
    #
    # # zipc1 = '/home/jisun/workspace/nictamnt/tomtom/HD_Traffic'
    # zipc1 = '/home/jisun/workspace/pycode/TomTom/data/c1/'
    # savec1 = '/home/jisun/workspace/pycode/TomTom/data/c1/'
    #
    # unzipdata(zipc1, savec1)
    # flist = ['/home/jisun/workspace/pycode/TomTom/data/c2select/content.xml.130513_081832',
    #          '/home/jisun/workspace/pycode/TomTom/data/c2select/content.xml.130513_121829',
    #          '/home/jisun/workspace/pycode/TomTom/data/c2select/content.xml.130513_180030']
    # # #
    # for ff in flist:
    #     df = datex2_content2(ff)
    #     df.to_csv(ff + '.csv')
    #     df1 = output_locNodes(df)
    #     df1.to_csv(ff + '_locs.csv')
    # print 'Done!'
    # ==============merge the above two tables ======================
    datafolder = '/home/jisun/workspace/pycode/TomTom/data/c2select/'
    # dfid = pd.read_csv(osp.join(datafolder, 'points.csv'))
    # dfidlong = pd.read_csv(osp.join(datafolder, 'points_all.csv'))

    # for ff in flist:
    #
    #     df1 = pd.read_csv(ff + '.csv')
    #     df2 = pd.read_csv(ff + '_locs.csv')
    #     df3 = df1.merge(df2, how='left')
    #     df3.to_csv(ff + '_merged.csv')
    # print 'done!'

    # =============Generate tables with full accuracy =========
    # fflist = glob(datafolder + '*_merged.csv')
    # print fflist
    # for ff in fflist:
    #     print 'processing %s' % ff
    #     edges = {'0p1': [], '0p2': [],
    #              '1travelTime': [],
    #              '2speed': [],
    #              '3distance': []}
    #
    #     df1 = pd.read_csv(ff)[['p1', 'p2',
    #                                  'travelTime',
    #                                  'freeFlowTravelTime',
    #                                  'averageSpeed',
    #                                  'freeFlowSpeed',
    #                                  'ndp1']]
    #     # print df1
    #     for p1, p2, tT, fTT, sp, fsp, ndp in df1.values:
    #         edges['0p1'].append(p1)
    #         edges['0p2'].append(p2)
    #         if not np.isnan(tT):
    #             edges['1travelTime'].append(tT)
    #         else:
    #             edges['1travelTime'].append(fTT)
    #         if not np.isnan(sp):
    #             edges['2speed'].append(sp)
    #         else:
    #             edges['2speed'].append(fsp)
    #         edges['3distance'].append(ndp)
    #
    #     df2 = pd.DataFrame(edges)
    #     print df2.to_csv(ff + '_tensorV1.csv')
    #     print df2.columns

    # fflist = glob(datafolder + '*_tensorV1.csv')
    # dfp3value = pd.read_csv(osp.join(datafolder, 'points3.csv'))[['slon', 'slat']].values
    # dfpfull = pd.read_csv(osp.join(datafolder, 'points_all.csv'))[['lon', 'lat']].values
    # keydict = {}
    # keyid = {}
    # count = 0
    # longid = []
    # slon = []
    # slat = []
    #
    # for x, y in dfpfull:
    #     tmpkey = '%.3f' % x + '%.3f' % y
    #     if tmpkey not in keydict:
    #         keydict[tmpkey] = (x, y)
    #         keyid[tmpkey] = count
    #         longid.append(count)
    #         slon.append(x)
    #         slat.append(y)
    #         count += 1
    # # print keydict.keys()
    #
    # pd.DataFrame({'id': longid, 'slon': slon, 'slat': slat}).to_csv(osp.join(datafolder, 'points3.csv'))
    # index = np.array(['%.3f' % x + '%.3f' % y for x, y in dfp3value])

    # for ff in fflist:
    #     df = pd.read_csv(ff)
    #     id1 = []
    #     id2 = []
    #     idp1 = []
    #     idp2 = []
    #     # print df
    #     for p1, p2 in df[['0p1', '0p2']].values:
    #         tmp1 = ast.literal_eval(p1)
    #         tmp2 = ast.literal_eval(p2)
    #         tmp1 = ast.literal_eval(tmp1[0]), ast.literal_eval(tmp1[1])
    #         tmp2 = ast.literal_eval(tmp2[0]), ast.literal_eval(tmp2[1])
    #
    #         id1.append(keyid['%.3f' % tmp1[0] + '%.3f' % tmp1[1]])
    #         id2.append(keyid['%.3f' % tmp2[0] + '%.3f' % tmp2[1]])
    #         idp1.append(keydict['%.3f' % tmp1[0] + '%.3f' % tmp1[1]])
    #         idp2.append(keydict['%.3f' % tmp2[0] + '%.3f' % tmp2[1]])
    #
    #
    #     df1 = pd.DataFrame({'id1': id1, 'id2': id2,
    #                         'idp1': idp1, 'idp2': idp2}, index=df.index).join(df)
    #     df1.to_csv(ff[:-4] + '_point_pair.csv')
    #
    # paircheck(datafolder)
    datafolder = '/home/jisun/workspace/nictamnt/tomtom/HD_Flow_Real_Time_Traffic'
    datafoldercheck(datafolder)














