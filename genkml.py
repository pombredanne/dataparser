__author__ = 'jisun'

import numpy as np
import pandas as pd
import os.path as osp


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
        fd.write("<Point><coordinates>%s,%s,0</coordinates>\n</Point>\n"% (lon, lat))
        fd.write("</Placemark>\n")
        count += 1
        #fd.write("new google.maps.LatLng(%f,%f),\n"%(xx,yy)) #This is for google map api javascript code
    fd.write("</Document></kml>")
    fd.close()
    return count


def genpairkml(points, savefile):
    pass


if __name__ == '__main__':
    datafolder = '/home/jisun/workspace/pycode/TomTom/data/c2select'
    datafile = 'points3.csv'
    savefile1 = 'points3.kml'
    # df = pd.load(osp.join(datafolder, datafile))[['p1', 'p2']]
    df = pd.read_csv(osp.join(datafolder, datafile))
    to_save1 = osp.join(datafolder, savefile1)
    # points = np.append(df.p1.values, df.p2.values, 0)
    points = df[['slon', 'slat']].values
    print points

    print genkml(points, to_save1)
    # savefile2 = 'content2_location_for_map_p2.kml'
    # to_save2 = osp.join(datafolder, savefile2)
    # points = df.p2.values
    # print genkml(points, to_save2)