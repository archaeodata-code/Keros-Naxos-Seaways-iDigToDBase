'''
version:  0.1 JFA
WORK:
    -- hard coding of column position versus column names
    
This modules adds summarized special find data to files targeted to the GIS
'''
import csv
from pathlib import Path

def getsfsummary(maindir):
    contextasu = True
    sfsummary(maindir,contextasu)
    contextasu = False
    sfsummary(maindir,contextasu)

def sfsummary(maindir,contextasu):
    # this module will create a version of contexts that has summarized
    # specialfind data in it

    # NOTE: MUST have created the flattened version of SpecialFind which
    # has the relationship keys unpacked from the relationbelongsto field.
    # this is the file SpecialFinds2

    # NOTE:  you have to have run g_contextshapefile.py to get the latest Geo_All_Context.csv

    con = maindir / 'data/GISLoads/Geo_All_ContextNoASU.csv'
    sf = maindir / 'data/LoadCSVs/SpecialFind2.csv'
    if contextasu:
        # summing to ASU separate from Context
        sfsum = maindir / 'data/GISLoads/GeoContextASUSpecialFindSummary.csv'
        con = maindir / 'data/GISLoads/Geo_All_ContextASU.csv'
        print('\n\n\t\tSummarizing SFs up to context and ASU levels separately.')
    else:
        # we want to sum all the way up to context only
        sfsum = maindir / 'data/GISLoads/GeoContextNoASUSpecialFindSummary.csv'
        con = maindir / 'data/GISLoads/Geo_All_ContextNoASU.csv'
        print('\n\n\t\tSummarizing SFs up to context level.')

    # load up a list of special finds
    sflist = []
    with open(sf, 'r', encoding = 'utf8') as fin:
        csvsf = csv.reader(fin, dialect = 'excel')
        for row in csvsf:
            sflist.append(row)

    with open(sfsum, 'w', encoding = 'utf8', newline ='') as fout:
        csvout = csv.writer(fout, dialect = 'excel')

        with open(con, 'r', encoding = 'utf8') as fin:
            csvcon = csv.reader(fin, dialect = 'excel')
            headers = next(csvcon)
            # adding a field to header to hold summary count of finds
            sfsummaries = "metallurgical","obsidian","kouphonisi pebble","stone disc", "worked stone", "spool_whorl"
            headers.extend(sfsummaries)
            csvout.writerow(headers)
            nbrrows = 0
            for row in csvcon:
                nbrrows = nbrrows + 1
                hit = False
                rowout = row
                metalnbr = 0
                obsnbr = 0
                pbnbr = 0
                sdnbr = 0
                wsnbr = 0
                swnbr = 0
                contextid = row[17]
                for item in sflist:
                    stype = ''
                    if contextasu:
                        # match on either the context-UUID or the ASU-UUID
                        if item[29] == contextid or item[30] == contextid:
                            hit = True
                        else:
                            hit = False
                    else:
                        if item[29] == contextid:
                            hit = True
                        else:
                            hit = False
                    if hit:
                        stype = item[3]
                        stype = stype.lower()
                        if stype.find('metal') > -1:
                            metalnbr = metalnbr + 1
                        elif stype.find('obsid') > -1:
                            obsnbr = obsnbr + 1
                        elif stype.find('koufo') > -1 or stype.find('kouph') > -1:
                            pbnbr = pbnbr + 1
                        elif stype.find('disc') > -1:
                            sdnbr = sdnbr + 1
                        elif stype.find('worked stone') > -1:
                            pbnbr = wsnbr + 1
                        elif stype.find('spindle') > -1 or stype.find('spool') > -1:
                            pbnbr = swnbr + 1
                        else:
                            pass
                            
                    else:
                        pass
                rowout.append(metalnbr)
                rowout.append(obsnbr)
                rowout.append(pbnbr)
                rowout.append(sdnbr)
                rowout.append(wsnbr)
                rowout.append(swnbr)
                csvout.writerow(rowout)
        if contextasu:
            print('\n\t\tHave processed SF counts for contexts and ASUs totaling: ' + str(nbrrows), end = '\r' )
        else:
           print('\n\t\tHave processed SF counts for contexts totaling: ' + str(nbrrows), end = '\r' )



if __name__ == "__main__":
    maindir = Path("/./Users/nathanmeyer/Keros-Naxos-Seaways-iDigToDBase")
    getsfsummary(maindir)

