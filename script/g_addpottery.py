'''
version:  0.1 JFA
WORK:
    -- too much hard coding around different files, needs abstraction
    -- hard coding of column position versus column names
    
This modules adds summarized pottery data to files targeted to the GIS
'''

import csv
from pathlib import Path

def getpotteryrows(maindir, contextasu):
    # get a list of pottery rows from Pottery2.

    pots = []
    pottery = maindir / 'data/LoadCSVs/Pottery2.csv'
    with open(pottery, 'r', encoding = 'utf8', newline = '') as potterylist:
        potteryReader = csv.DictReader(potterylist, dialect = 'excel')
        badrows = []
        for row in potteryReader:
            rowout=[]
            # those that have ASU UUID
            if row['ASU_UUID']:
                rowout = row['Context_UUID'],row['ASU_UUID'],row['CoverageTemporal'],row['Source'],row['ContextID'],row['Identifier'],row['PotteryKastri'], row['PotterySherdSizes'],row['PotteryWear'],row['PotteryBurning']
            # not an ASU so get Context UUID
            elif row['Context_UUID']:
                rowout = row['Context_UUID'],'',row['CoverageTemporal'],row['Source'],row['ContextID'],row['Identifier'],row['PotteryKastri'], row['PotterySherdSizes'],row['PotteryWear'],row['PotteryBurning']
            # unless it is missing both            
            else:
                # these will not get summed up
                data = "There was a pottery2 row (" + row['Source'] + " " + row["ContextID"] + " " + row["Identifier"] + " that had neither Context nor ASU UUID \n"
                badrows.append(data)

            if rowout:
                pots.append(rowout)
            else:
                pass

        if badrows and not contextasu:
            f = maindir / 'data/CleanseErrorReports/Pottery2RowWithNoContextASU.txt'
            with open(f, mode='w') as fout:
                fout.write("===================================================================\n")
                fout.write("NOTE: Likely pottery from flotation bags that do have only been\n")
                fout.write("      related to SampleBag and need relation added to context.\n")
                fout.write("-------------------------------------------------------------------\n")
                for item in badrows:
                    fout.write(item)
            print('\n\t\tWarning: This number of pottery rows had no Context or ASU ID: ' + str(len(badrows)))
            print('\t\tThey are printed out in Pottery2RowWithNoContextASU.txt')
            input('Any key to continue ...')
        else:
            pass

    return(pots)

def getpotsumcountrows(maindir,contextasu):
    # this module builds a list of rows with pottery summary counts
    # assumes we have already built PotterySummarizedAtContextASU.csv
    # and PotterySummarizedAtContext.csv (the latter rolls ASU to context)
    l_pots = []
    if contextasu:
        # we want both the context and asu records separately
        pottery = maindir / 'data/LoadCSVs/PotterySummarizedAtContextASU.csv'
    else:
        # we want the data summarized to context
        pottery = maindir / 'data/LoadCSVs/PotterySummarizedAtContext.csv'
    with open(pottery, 'r', encoding = 'utf8', newline = '') as potterylist:
        potteryreader = csv.reader(potterylist, dialect = 'excel')
        header = next(potteryreader)
        for row in potteryreader:
            l_pots.append(row)
    return(l_pots,header)

def rollperiods(periods,row,single):

    # now evaluate the period data in periods list and write to a dictionary of periods
    perioddict = {"Pre-Phase A": 'no',"Phase A": 'no',"Phase A/B": 'no',"Phase B": 'no',"Phase B/C": 'no',"Phase C": 'no',"Phase A-C": 'no',"Post-Phase C": 'no', "Other": 'no'}

    if single:
        # this is the code for periods only holding a list of period readings'

        for item in periods:
            if item == "Pre-Phase A":
                perioddict["Pre-Phase A"] = 'yes'
            elif item == "Phase A":
                perioddict["Phase A"] = 'yes'
            elif item == "Phase A ?":
                perioddict["Phase A"] = '?'
            elif item == "Phase A?":
                perioddict["Phase A"] = '?'
            elif item == "Phase A/B":
                perioddict["Phase A/B"] = 'yes'
            elif item == "Phase A/B ?":
                perioddict["Phase A/B"] = '?'
            elif item == "Phase B":
                perioddict["Phase B"] = 'yes'
            elif item == "Phase B ?":
                perioddict["Phase B"] = '?'
            elif item == "Phase B?":
                perioddict["Phase B"] = '?'
            elif item == "Phase B/C":
                perioddict["Phase B/C"] = 'yes'
            elif item == "Phase C":
                perioddict["Phase C"] = 'yes'
            elif item == "Phase C (?)":
                perioddict["Phase C"] = '?'
            elif item == "Phase C?":
                perioddict["Phase C"] = '?'
            elif item == "A-C":
                perioddict["Phase A-C"] = 'yes'
            elif item == "Phase A-C":
                perioddict["Phase A-C"] = 'yes'
            elif item == "Phase A-C?":
                perioddict["Phase A-C"] = '?'
            elif item == "Post-Phase C":
                perioddict["Post-Phase C"] = 'yes'
            elif item == "Post-Phase C?":
                perioddict["Post-Phase C"] = '?'    
            else:
                perioddict["Other"] = item

    else:
        # this is placeholder code for the row with the periods laid out as columns
        input("Phase roll up has logic error -- must fix")

    # update the context row with period values
    # we are appending here to something like All_Context.csv
    # which does not have all the context data fields
    for k in perioddict.keys():
        row.append(perioddict[k])

    # calc the period based on phases in pottery
    if perioddict["Post-Phase C"] == 'yes':
        row.append("Post C")
    elif perioddict["Phase C"] == "yes":
        row.append("C")
    elif perioddict["Phase B"] == "yes":
        row.append("B")
    elif perioddict["Phase A"] == "yes":
        row.append("A")
    elif perioddict["Pre-Phase A"] == "yes":
        row.append("Pre A")
    elif perioddict["Phase A/B"] == "yes":
        row.append("A/B")
    elif perioddict["Phase B/C"] == "yes":
        row.append("B/C")
    elif perioddict["Phase A-C"] == "yes":
        row.append("A-C")
    else:
        row.append(perioddict["Other"])

    return(row)

def getphases(datafolder,infile,outfile,header,contextasu,maindir):
    # NOTE: this module assumes you are passing in some variant of contexts (example, surfaces)
    # and you want to add in dates  from pottery reading.  the header coming in basically is
    # the context header plus extended columns to hold the phase dates and special find summaries
    # this code will take a single context/asu and create a summary of all phase data from multiple
    # bags of pottery

    # write a header of all the columns we want for holding phase data for the context
    if infile == "Geo_All_ContextASU.csv":
        phasedata = ["Pre-Phase A","Phase A","Phase A/B","Phase B","Phase B/C","Phase C","Phase A-C", "Post-Phase C", "Other","Period","Kastri","PotterySherdSizes", "PotteryWear", "PotteryBurning"]  
    elif infile == "Geo_All_Context.csv":
        phasedata = ["Pre-Phase A","Phase A","Phase A/B","Phase B","Phase B/C","Phase C","Phase A-C", "Post-Phase C", "Other","Period","Kastri","PotterySherdSizes", "PotteryWear", "PotteryBurning"]
    elif infile == "surfaceasu.csv":
        phasedata = ["SurfaceID","ASU_ID","Pre-Phase A","Phase A","Phase A/B","Phase B","Phase B/C","Phase C","Phase A-C", "Post-Phase C", "Other","Period","Kastri","PotterySherdSizes", "PotteryWear", "PotteryBurning"]
    else:
        print("PROBLEM: a file passed in that does not have a good header target ...")
        input("Cannot proceed ... hit Cntrl C to terminate.")

    header.extend(phasedata)

    # get a list of pottery keyed either with context or asuid UUID
    l_pots = getpotteryrows(maindir, contextasu)
   
    nbr = 0
    # create a target for writing out
    outf = datafolder / outfile
    with open (outf, 'w', encoding = 'utf8', newline = '') as fout:
        csvout = csv.writer(fout, dialect = "excel")
        csvout.writerow(header)

        # create reader object from infile
        # we will iterate this and search l_pots (Pottery2.csv) for related pottery bags
        inf = datafolder / infile
        with open(inf, 'r', encoding = 'utf8', newline = '') as fin:
            csvin = csv.reader(fin, dialect = 'excel')
            #skip the header
            _ = next(csvin)
            for row in csvin:
                kastri = ''
                size = ''
                wear = ''
                burn = ''
                contextID = ''
                uuid = ''
                periods = []
                if row[1] == 'Partition' or row[1] == 'ASU':
                    isasu = True
                else:
                    isasu = False

                if contextasu == True:
                    # we want both contexts and ASUs separately so get the IdentifierUUID
                    uuid = row[17]
                elif contextasu == False:
                    # we do not want ASUs separately so get the Context_UUID
                    contextID = row[20]
                else:
                    print("Why is this happening: " + contextasu)
                    input("contextasu variable not assigned")

                # find the rows for captured UUID in potrows (Pottery2.csv) and get the periods
                rowsin = 0
                for potrow in l_pots:
                    rowsin = rowsin + 1
                    if contextasu and isasu:
                        # we are looking for separate phasing for context and asu
                        # and this is an ASU 
                        if str(potrow[1]) == uuid:
                            # the uuid for the ASU matches the asu uuid from the potrow
                            periods.append(potrow[2])
                            # get the Kastri y/n etc information if available
                            kastri = potrow[6] + ' --- ' + kastri
                            size = potrow[7] + ' --- ' + size
                            wear = potrow[8] + ' --- ' + wear
                            burn = potrow[9] + ' --- ' + burn
                            
                        else:
                            pass

                    elif contextasu and not isasu:
                        # we are looking for separate phasing for context and asu
                        # and this is a context 
                        if str(potrow[0]) == uuid:
                            # the uuid for the context matches the uuid from the potrow
                            periods.append(potrow[2])
                            # get the Kastri y/n etc information if available
                            kastri = potrow[6] + ' --- ' + kastri
                            size = potrow[7] + ' --- ' + size
                            wear = potrow[8] + ' --- ' + wear
                            burn = potrow[9] + ' --- ' + burn
                        else:
                            pass

                    elif not contextasu:
                        # we want to roll to the context so only match on contextUUID
                        if str(potrow[0]) == contextID:
                            periods.append(potrow[2])
                            # get the Kastri y/n etc information if available
                            kastri = potrow[6] + ' --- ' + kastri
                            size = potrow[7] + ' --- ' + size
                            wear = potrow[8] + ' --- ' + wear
                            burn = potrow[9] + ' --- ' + burn
                        else:
                            pass
                    else:
                        input("could not evaluate contextasu and isasu")

                if isasu and not contextasu:
                    # in this case where we are only summing context we
                    # ignore ASUs here (having got their pots rolled to context above
                    pass
                else:
                    single = True
                    # now roll all the periods found up into the context/ASU level row
                    row = rollperiods(periods,row,single)
                    
                    # add the Kastri etc
                    row.append(kastri)
                    row.append(size)
                    row.append(wear)
                    row.append(burn)

                    # write out the row
                    csvout.writerow(row)
                    nbr = nbr + 1

    if contextasu:
        print("\n\t\tProcessed phase data for context and ASU rows equaling: " + str(nbr) + '\n')
    else:
        print("\n\t\tProcessed phase data for context rows equaling: " + str(nbr) + '\n')


def getpotterycountsweights(infile,outfile,header, contextasu,maindir):
    # this module will take in a file of contexts and add summarized pottery counts and weights etc
    # we pass in GeoContextASUWithPhase.csv or GeoContextNoASUWithPhase.csv
    
    #get the pottery rows
    l_pots,xheader = getpotsumcountrows(maindir,contextasu)
    header.extend(xheader)

    with open(outfile, 'w', encoding = 'utf8', newline = '') as fout:
        csvout =  csv.writer(fout, dialect = 'excel')

        csvout.writerow(header)

        with open(infile, 'r', encoding = 'utf8', newline = '') as fin:
            csvin = csv.reader(fin, dialect = 'excel')
            _ = next(csvin)
            badrows = []
            allrow = 0
            for row in csvin:
                allrow = allrow + 1
                nbrfound = 0
                contextid = row[17]
                for item in l_pots:
                    if contextasu:
                        # we are loading data to context and asu levels
                        # we have to check two columns, context and asu in l_pots for a match
                        # we assume here that only one row in l_pots will match a contextid ...
                        # this should be a good assumption but perhaps it should be checked here
                        if contextid == item[53] or contextid == item[54]:
                            row.extend(item)
                            nbrfound = 1
                            csvout.writerow(row)
                            break
                        else:
                            pass
                    else:
                        # we just loading to context
                        if contextid == item[53]:
                            row.extend(item)
                            nbrfound = 1
                            csvout.writerow(row)
                            break
                        else:
                            pass
            
                if nbrfound == 0:
                    data = row[0] +  " " + row[3] + " did not find a summarized row of pottery counts"
                    badrows.append(data)
                else:
                    pass
    

    if contextasu:
        print('\t\t================================================================================')
        print("\t\tLooked at this many CONTEXT & ASU records to find pottery counts: " + str(allrow))
        print("\t\tThis many did not find pottery counts: " + str(len(badrows)))
        print("\t\tLook at >data>CleanseErrorReports>@ContextASUNoSummarizedPottery to spot check")
        f = maindir / 'data/CleanseErrorReports/@ContextASUNoSummarizedPottery.txt'
    else:
        print('\t\t================================================================================')
        print("\t\tLooked at this many CONTEXT records to find pottery counts: " + str(allrow))
        print("\t\tThis many did not find pottery counts: " + str(len(badrows)))
        print("\t\tLook at >data>CleanseErrorReports>@ContextNoSummarizedPottery to spot check")
        f = maindir / 'data/CleanseErrorReports/@ContextNoSummarizedPottery.txt'

    with open(f, mode='w') as fout:
        fout.write("NOTE: These contexts likely should NOT have pottery.\n")
        fout.write("Spot check with iDig records to confirm ... \n")
        fout.write("------------------------------------------------------\n")
        for item in badrows:
            fout.write(item + '\n')
    print('\t\t================================================================================\n')



def getpottery(maindir):
    # ==================================================================
    # MAIN PROGRAM STARTS HERE

    # create a file of ALL contexts and ASUs with phases from pottery
    # this will NOT roll ASUs up into Contexts
    print("\t\t------------------------------------------------------")
    print("\t\t--------Adding the pottery phases to contexts/ASUs---")
    print("\t\t------------------------------------------------------")
    datafolder = maindir / 'data/GISLoads'
    infile = "Geo_All_ContextASU.csv"
    outfile = "GeoContextASUWithPhase.csv"
    inf = datafolder / infile
    with open(inf, 'r', encoding = 'utf8', newline ='') as fin:
        csvin = csv.reader(fin, dialect = 'excel')
        header = next(csvin)
    contextasu = True
    getphases(datafolder,infile,outfile,header,contextasu, maindir)

    # repeat but at context level (roll ASUs into context)
    print("\t\t------------------------------------------------------")
    print("\t\t-------Rolling ASU pottery phases up to contexts-----")
    print("\t\t------------------------------------------------------")
    datafolder = maindir / 'data/GISLoads'
    infile = "Geo_All_ContextASU.csv"
    outfile = "GeoContextNoASUWithPhase.csv"
    inf = datafolder / infile
    with open(inf, 'r', encoding = 'utf8', newline ='') as fin:
        csvin = csv.reader(fin, dialect = 'excel')
        header = next(csvin)
    contextasu = False
    getphases(datafolder,infile,outfile,header,contextasu, maindir)

    # create a file of ALL contexts/ASUs with phases and summarized pottery
    # this essentially takes file just created and adds the summarized pottery counts
    print("\t\t------------------------------------------------------")
    print("\t\t----Adding summarized pottery counts at contexts and ASUs-")
    print("\t\t------------------------------------------------------\n")

    infile = maindir / 'data/GISLoads/GeoContextASUWithPhase.csv'
    outfile = maindir / 'data/GISLoads/GeoContextAndASUWithPhaseAndPotteryCounts.csv'
    with open(infile, 'r', encoding = 'utf8', newline ='') as fin:
        csvin = csv.reader(fin, dialect = 'excel')
        header = next(csvin)
    contextasu = True
    getpotterycountsweights(infile,outfile,header,contextasu,maindir)

    print("\t\t------------------------------------------------------")
    print("\t\t----Adding summarized pottery counts at contexts only")
    print("\t\t------------------------------------------------------\n")

    # now pull a version that is just contexts
    infile = maindir / 'data/GISLoads/GeoContextNoASUWithPhase.csv'
    outfile = maindir / 'data/GISLoads/GeoContextNoASUWithPhaseAndPotteryCounts.csv'
    with open(infile, 'r', encoding = 'utf8', newline ='') as fin:
        csvin = csv.reader(fin, dialect = 'excel')
        header = next(csvin)
    contextasu = False
    getpotterycountsweights(infile,outfile,header,contextasu,maindir)

if __name__ == "__main__":
    maindir = Path("/./Users/nathanmeyer/Keros-Naxos-Seaways-iDigToDBase")
    getpottery(maindir)


