__author__ = 'jisun'

import numpy as np
import pandas as pd
import os.path as osp
import pickle


def genkml(points, savefile):
    """
    x and y array to contain all lon, lat
    """

    fd = open(savefile, 'wb')
    beginning = """<?xml version="1.0" encoding="UTF-8"?> \n <kml xmlns="http://www.opengis.net/kml/2.2"> \n <Document> \n"""
    fd.write(beginning)
    count = 1
    for lon, lat in points:
        #longtitude and latitude is different from Lambert projection order
        fd.write("<Placemark>\n <name> Tomtom %g </name>\n" % count)
        fd.write("<description> Tomtom data points %g </description>\n" % count)
        fd.write("<Point><coordinates>%s,%s,0</coordinates>\n</Point>\n" % (lon, lat))
        fd.write("</Placemark>\n")
        count += 1
        #fd.write("new google.maps.LatLng(%f,%f),\n"%(xx,yy)) #This is for google map api javascript code
    fd.write("</Document></kml>")
    fd.close()
    return count


def genpairkml(pointpair, savefile):
    fd = open(savefile, 'wb')
    begining = """<?xml version="1.0" encoding="UTF-8"?> \n
                <kml xmlns="http://www.opengis.net/kml/2.2"> \n
                  <Document> \n

                    <Folder>
                    <open>1</open>
                    <Placemark>\n
                    <name>Absolute Extruded</name> \n
                      <description>Transparent green wall with yellow outlines</description> \n
                    <styleUrl>#PREPSTATUS10</styleUrl>
                    <MultiGeometry> \n
               """
    fd.write(begining)
    count = 0
    for datarow in pointpair:
        s, t = [eval(i) for i in datarow]
        fd.write("""<LineString> \n
                        <tessellate>1</tessellate> \n
                        <coordinates>\n""")

        fd.write('{:s},{:s}, 0\n {:s}, {:s}, 0\n'.format(s[0], s[1], t[0], t[1]))
        fd.write("""</coordinates>\n
                    </LineString>\n""")
        count += 1
    fd.write("""</MultiGeometry></Placemark></Folder></Document>
             </kml>""")
    fd.close()

    return count


if __name__ == '__main__':
    datafolder = '/home/jisun/workspace/pycode/TomTom/data/c2select'
    datafile = 'content.xml.130513_121829_locs.csv'
    savefile1 = 'pairLinestring.kml'
    df = pd.read_csv(osp.join(datafolder, datafile))[['p1', 'p2']]
    # df = pd.read_csv(osp.join(datafolder, datafile))
    to_save1 = osp.join(datafolder, savefile1)
    # points = np.append(df.p1.values, df.p2.values, 0)
    # points = df[['slon', 'slat']].values
    # point2id = pickle.load(open('/home/jisun/workspace/pycode/TomTom/data/tmp/point2id.pcl', 'rb'))
    # points = point2id.values()

    # print genkml(list(set(points)), to_save1)
    # savefile2 = 'content2_location_for_map_p2.kml'
    # to_save2 = osp.join(datafolder, savefile2)
    # points = df.p2.values
    # print genkml(points, to_save2)
    pointpair = df.values
    print pointpair[0][0]
    print genpairkml(pointpair, to_save1)