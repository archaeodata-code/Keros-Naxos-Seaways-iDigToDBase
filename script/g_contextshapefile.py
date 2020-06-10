'''
Version 0.1 JFA
Work: 
    
    -- DictReader to avoid referencing column positions
    -- redundant else statements ~254 forward

This unpackes the Geometry Collection for a context and creates 
a seperate open and close well known text object
'''

import csv
from shapely.geometry import Polygon
from shapely.wkt import loads
import shapefile
from pathlib import Path

def getzed(polygon):
    # this returns the ZED value, which for Keros is the smallest value
    # using the zed value to determine open and close
    polygon = polygon[12:-2]

    if polygon[-1:] == ")":
        polygon = polygon[0:-1]
    else:
        pass
    polygon = polygon.replace(",","")

    pointlist=[]
    for i in polygon.split(" "):
        pointlist.append(float(i))
    points = sorted(pointlist)
    return points[0]

def mergeshapefiles(directory):
    # Merge a bunch of shapefiles with attributes
    files = list(directory.glob('*.shp'))
    fout = str(directory / 'All_Contexts')
    w = shapefile.Writer(fout)
    nbrf = 0
    for f in files:
        nbrf = nbrf + 1
        fin = str(f)
        try:
            r = shapefile.Reader(fin)
            rec = r.record(0)
            w.fields = r.fields[1:]
            w.record(rec[0], rec[1], rec[2], rec[3], rec[4])
            w.shape(r.shape(0))
        except Exception as error:
            print(fin)
            print(rec)
            input(error)

        print('\t\tMerging shape file number: ' + str(nbrf), end = '\r')
    w.close()

def buildshp(points, polytype, row, shapedir):

    # handle odd case where it is a 2d polygon, not 3d
    if points[0:10] == "POLYGON ((":
        points = points[10:-2]
    else:
        points = points[12:-2]

    if points[-1:] == ")":
        points = points[0:-1]
    else:
        pass

    # split the points list into XYZ subparts      
    pointlist = []
    try:
        for xyz in points.split(", "):
            #split the XYZ part in to individual coordinates
            point = []
            for item in xyz.split(" "):
                try:
                    point.append(float(item))
                except Exception as error:
                    print(error)
                    print("these are the points passed in: " + points)
                    print("this is the problematic bit: " + item)
                    input("what?")
        pointlist.append(point)
    except Exception as error:
        input(error)

    context = row[3]

    try:
        polypoints = []
        polypoints.append(pointlist)        
        filename = row[0] + "-" + str(context) + "-" + polytype
        filepath = shapedir / filename
        w = shapefile.Writer(filepath)
        w.poly(polypoints)
        w.field("Trench","C")
        w.field("Type","C")
        w.field("SubType","C")
        w.field("ID","C")
        w.field("UUID","C")

        if context.find("*") > -1:
            context = context.replace('*','ast')
        else:
            pass
        w.record(row[0], row[1], row[2], row[3], row[17])
        w.close()
    except Exception as error:
        print(error)
        input('unhandled error in buildshp')

def openclose(maindir):
    # -------------------------------------------------
    # main program
    # -------------------------------------------------
    rowsprocessed = 0

    # set dir for shapefiles
    # use next line for XY only shapefiles
    #shapedir = "D:\iDigToDBase\gis\XYshapefile"
    # use next line for XYZ shapefiles
    shapedir = maindir / 'gis/XYZshapefile'
    
    # set target directory
    tdir = maindir / 'data/GISLoads'

    # first clean out old files
    print("\n\t\tDeleting old files ...")
    files = list(shapedir.glob('*.*'))
    for f in files:
        try:
            f.unlink()
        except Exception as error:
            print(error)
            input("not able to delete a file")

    # now create new files for open and close
    print("\n\t\tGetting the open and close shape files ...")

    # open the source file as a csv read object
    source = maindir / 'data/Types/All_Context.csv'
    l_contexts = []
    missingpolygon = []
    badgeostring = []
    badguid = []
    with open(source, "r", encoding = "utf8") as fin:
        csvin = csv.reader(fin, dialect = "excel")

        # establish a write file and csv writer object
        target = tdir / 'Geo_All_ContextASU.csv'
        with open(target, "w", encoding = "utf8", newline = '') as fout:

            csvout = csv.writer(fout, dialect='excel')
            header = next(csvin)

            header.insert(9, "Y")
            header.insert(9, "X")
            header.append("opening")
            header.append("closing")
            csvout.writerow(header)
            # for each row in the csv reader
            for row in csvin:
                rowsprocessed = rowsprocessed + 1
                #grab the geo collection field
                s = row[12]
                #split out the x,y coordinations from CoveragePosition
                coverageposition = row[9]
                if coverageposition.find(',') > -1:
                    xylist = coverageposition.split(',')
                    row.insert(9,xylist[1])
                    row.insert(9,xylist[0])
                else:
                    row.insert(9,'')
                    row.insert(9,'')

                # test if we have a geometry collection, otherwise skip the line
                if s[0:19] == "GEOMETRYCOLLECTION(":
                    #clean up the line
                    s = s[19:]                
                    s = s[:-1]
                
                    # split into separate objects, calc area, then put area and polygon in dict
                    areadict = {}
                    for wktobj in s.split("), "):
                        if wktobj[0:4] == "POLY":
                            # make sure WKT ends with right number of parenthesis 
                            if wktobj[-3:] == ')))':
                                wktobj = wktobj[:-1]
                            elif wktobj[-2:] == '))':
                                pass
                            else: 
                                wktobj = wktobj + ')'
                            points = loads(wktobj)
                            polygon = Polygon(points)
                            polyarea = polygon.area             
                            if areadict:
                                try:
                                    if areadict[polyarea]:
                                        pass
                                    else:
                                        pass
                                except KeyError:
                                    areadict[polyarea] = wktobj
                            else:
                                areadict[polyarea] = wktobj

                        else:
                            pass

                    # rather than relying on polygon naming we are 
                    # choosing to programatically select open and close polygons
                    # based on area and zed values as this provided best results
                    # for clipping photogrammetry

                    arealist = []
                    for k,v in areadict.items():
                        arealist.append(k)
                    arealist.sort(reverse=True)


                    #populate two variables with biggest polygons
                    if arealist:
                        poly1 = areadict[arealist[0]]
                        deptha = getzed(poly1)
                        try:  
                            poly2 = areadict[arealist[1]]
                            depthb = getzed(poly2)
                            if deptha <= depthb:
                                row.append(areadict[arealist[0]])
                                row.append(areadict[arealist[1]])
                            else:
                                row.append(areadict[arealist[1]])
                                row.append(areadict[arealist[0]])
                        except IndexError: 
                            row.append(areadict[arealist[0]])
                            row.append('')
                            badrow = row[0] + ' ' + row[1] + ' ' + row[2] + ' ' + row[3]
                            missingpolygon.append(badrow)
                            
                    else:
                        badrow = row[0] + ' ' + row[1] + ' ' + row[2] + ' ' + row[3]
                        missingpolygon.append(badrow)
                        # still want to append blank columns to keep file in consistent format
                        row.append('')
                        row.append('')

                else:
                    badrow = row[0] + ' ' + row[1] + ' ' + row[2] + ' ' + row[3]
                    badgeostring.append(badrow)
                    # still want to append blank columns to keep file in consistent format
                    row.append('')
                    row.append('')

                # write the line to the cvsout
                # avoid duplicating items
                uuid = row[17]
                if uuid not in l_contexts:
                    csvout.writerow(row)
                    l_contexts.append(uuid)
                else:
                    badrow = row[0] + ' ' + row[1] + ' ' + row[2] + ' ' + row[3]
                    badguid.append(badrow)

    print("\n\t\tGrabbed open and close polygons for this many rows: " + str(rowsprocessed))
    
    if missingpolygon:
        errorfile = True
    elif badgeostring:
        errorfile = True
    elif badguid:
        errorfile = True
    else:
        errorfile = False

    if errorfile:
        print('\n\t\t=======================================================================')
        print('\t\tWARNING: there were errors finding open and close polygons.')
        print('\t\tFind them in >CleanseErrorReports@FilesWithMissingOpenClosePolygons.txt')
        print('\t\t=======================================================================')
        ferr = maindir / 'data/CleanseErrorReports/@FilesWithMissingOpenClosePolygons.txt'
        with open(ferr, 'w', encoding = 'utf-8') as fout:
            if missingpolygon:
                missingpolygon.sort()
                fout.write("\n=============================================================\n")
                fout.write("These contexts had only one polygon ...\n\n")
                for item in missingpolygon:
                    fout.write(item + '\n')
            else:
                pass
            
            if badgeostring:
                badgeostring.sort()
                fout.write("\n=============================================================\n")
                fout.write("These contexts had a bad or unusual polygon string ...\n\n")
                for item in badgeostring:
                    fout.write(item + '\n')
            else:
                pass

            if badguid:
                badguid.sort()
                fout.write("\n=============================================================\n")
                fout.write("These contexts have a guid already used (dups?) ...\n\n")
                for item in badguid:
                    fout.write(item + '\n')
            else:
                pass
    else:
        pass


    # now drop the ASUs from this so we have a file without them
    print("\n\t\tNow building a version of file without ASUs (contexts only)")
    source = tdir / 'Geo_All_ContextASU.csv'

    # first build a list of unique context UUIDs
    l_context = []
    with open(source, "r", encoding = "utf8") as fin:
        csvin = csv.DictReader(fin, dialect = "excel")
        for row in csvin:
            try:
                _ = l_context.index(row['Context_UUID'])
            except Exception:
                l_context.append(row['Context_UUID'])

        print("\t\tThere are " + str(len(l_context)) + " unique contexts in file.\n")
        startnbr = len(l_context)


    #now build the target file with just those contexts
    with open(source, "r", encoding = "utf8") as fin:
        csvin = csv.DictReader(fin, dialect = "excel")
        header = csvin.fieldnames

        # establish a write file and csv writer object
        target = tdir / 'Geo_All_ContextNoASU.csv'
        with open(target, "w", encoding = "utf8", newline = '') as fout:

            csvout = csv.writer(fout, dialect='excel')
            csvout.writerow(header)

            for row in csvin:
                if row['Type'] == "Partition" or row['Type'] == "ASU":
                    pass
                else:
                    try:
                        # see if the context is in the context list
                        toss = l_context.index(row['Context_UUID'])
                        outrow = []
                        for k,v in row.items():
                            outrow.append(v)
                        csvout.writerow(outrow)
                        l_context.pop(toss)
                        
                    except Exception as problem:
                        print(problem)
                        input("we are getting a duplicate GUID and we should not be")

        endnbr = len(l_context)
        print("\n\t\tThere were " + str(startnbr-endnbr) + " rows saved.")

        if startnbr - endnbr != 0:
            print('\t\t==================================================================')
            print("\t\tWARNING: There was Contexts that did not get written to file")
            print("\t\tLook in >CleanseErrorReports>@ASUsWithNoContextRecord for them")
            print('\t\t==================================================================\n')
            target = maindir / 'data/CleanseErrorReports/@ASUsWithNoContextRecord.txt'
            with open(target, 'w', encoding = 'utf8') as fout:

                with open(source, "r", encoding = "utf8") as fin:
                    csvin = csv.DictReader(fin, dialect = "excel")
                    for row in csvin:
                        for item in l_context:
                            if item == row['Context_UUID']:
                                # a hit means this row corresponds to UUID that was not written in previous operation, write this data to error file
                                rowout = row['Source'] + " : " + row['Type'] + " : " + row['Identifier'] + " : " + row['Context_UUID']
                                fout.write(rowout)
                                fout.write("\n")
                                l_context.pop(l_context.index(row['Context_UUID']))
                            else:
                                pass


        else:
            pass

                    

    ask = input("\tDo you want to continue building shapefiles for each polygon (y,n)?")
    if ask == "y":

        # create a shape file per open and close polygon
        print("\n\t\t-------------------------------------------------------------------------")
        print("\t\tNow creating open and close shapefile for each context ...")
        print("\t\t--------------------------------------------------------------------------")

        # open up the source file
        source = maindir / 'data/GISLoads/Geo_All_ContextASU.csv'
        
        with open(source, "r", encoding = "utf8") as fin:
            csvin = csv.DictReader(fin, dialect = "excel")
            rows = list(csvin)
            nbrcontexts = len(rows)
        
        with open(source, "r", encoding = "utf8") as fin:
            csvin = csv.DictReader(fin, dialect = "excel")
            ncon = 0
            # create a shapefile for open and close (if they exist) for each row
            for row in csvin:

                ncon = ncon + 1
                print('\t\tLooking a context ' + str(ncon) + ' of ' + str(nbrcontexts), end = '\r')
                try:
                    points = row['opening']

                    if points is None:
                        print('\t\tNo closing polygon for ' + row['Source'] + ' ' + row['Identifier'])
                    elif points == '' or points.lower() == 'none':
                        print('\t\tNo opening polygon for ' + row['Source'] + ' ' + row['Identifier'])

                    else:
                        outrow = []
                        for k,v in row.items():
                            outrow.append(v)
                        polytype = 'open'
                        buildshp(points, polytype, outrow, shapedir)

                except Exception as error:
                    print('\n\n')
                    print(points)
                    print(row)
                    print(shapedir)
                    print(error)
                    input('opening polygon ..... need to handle this error')
                try:
                    points = row['closing']
                    if points is None:
                        print('\t\tNo closing polygon for ' + row['Source'] + ' ' + row['Identifier'])
                    elif points == '' or points.lower() == 'none':
                        print('\t\tNo closing polygon for ' + row['Source'] + ' ' + row['Identifier'])
                    else:
                        outrow = []
                        for k,v in row.items():
                            outrow.append(v)
                        polytype = 'close'
                        #r='5c'
                        buildshp(points, polytype, outrow, shapedir)
                except Exception as error:
                    print('\n\n')
                    print(points)
                    print(row)
                    print(error)
                    print('closing polyton ..... need to handle this error')

        print('\n\t\tBuilding a combined shapefile now')
        xyzfiledir = maindir / 'gis/XYZshapefile'
        mergeshapefiles(xyzfiledir)

        print("\n\t\tShapefiles created")
        input('Any key to continue ...')

    else:
        pass

if __name__ == "__main__":
    maindir = Path("/./Users/nathanmeyer/Keros-Naxos-Seaways-iDigToDBase")
    openclose(maindir)

