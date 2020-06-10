'''
version:  0.1 JFA

WORK:
    -- remove hardcodoing of RelationBelongsTo column
    -- use DictReader instead of csvreader to avoid hard coding column positions

This module provides methods to unpack relationships for
development of foreign keys
'''
import csv
from pathlib import Path
from f_getguids import gettableguids
from f_getguids import getcontextguids


def getrelations(infile,outfile,belongstocolumnnum,maindir):

    errordir = maindir / 'data' / 'CleanseErrorReports'

    # load lists with guids against which to ID the belongsTo guids
    potteryrecords = maindir / 'data/Types/Pottery.csv'
    potterylist = gettableguids(potteryrecords)

    flotationrecords = maindir / 'data/Types/Flotation.csv'
    flotationlist = gettableguids(flotationrecords)
    
    contexts = maindir / 'data/Types/All_Context.csv'
    contextlist, asulist, contextasudict = getcontextguids(contexts,errordir)

    # a list for holding records that do not have belongs to guids
    nobelongstoguids = []

    # load a csv reader with infile records
    with open(infile, "r", encoding="utf8") as fin:
        csvin = csv.reader(fin)
        # get a header row for the csvwriter coming up
        header = next(csvin)
        header.append("Context_UUID")
        header.append("ASU_UUID")
        header.append("FlotationBag_UUID")
        header.append("PotteryBag_UUID")

        # open a target file and load with extracted normalized GUIDS
        with open(outfile, "w", newline ='',encoding="utf8") as fout:
            csvout = csv.writer(fout, dialect = 'excel')
            # write a header
            csvout.writerow(header)
            zeroguid = 0

            # get a list to hold cases where a guid did not find its relationship 
            # the other guids did not find a context or ASU ... 
            missingguid = []
            contextfound = 0
            asufound = 0
            flotationfound = 0
            potteryfound = 0
            for row in csvin:
                rowout = row
                belongstoguids = row[belongstocolumnnum]
                guidlist = []

                if belongstoguids == '':
                    #this means no belongsto relation exists for item ... bad thing!
                    if infile.name == 'SampleBag.csv':
                        badfile = ("Trench: " + row[0] + "\tType: " + row[1] + "\tBag: " + row[3] + "\tType: " + row[4] + "\tDesc: " + row[8])
                    elif infile.name == 'SpecialFind.csv':
                        badfile = ("Trench: " + row[0] + "\tType: " + row[1] + "\tID: " + row[2] + "\tTitle: " + row[3] + "\tDesc: " + row[10])
                    else:
                        # we should not have this case
                        print("Processing file: " + str(infile))
                        print(row)
                        input("You have a BelongsTo field that has no quids")
                        badfile = "Problem with " + str(infile)

                    nobelongstoguids.append(badfile)
                    zeroguid = zeroguid + 1
                    
                else:
                    # add all the guids to guidlist
                    guidlist = belongstoguids.split("\\n")
                    iscontext = 0
                    isasu = 0
                    isflotation = 0
                    ispottery = 0
                    noguid = 0
                    contextguid = ''
                    asuguid = ''
                    flotationguid = ''
                    potteryguid = ''
                    errguid = ''
                    for guid in guidlist:
                        try:
                            _ = contextlist.index(guid)
                            # no exception means we have a hit
                            contextguid = guid
                            iscontext = 1
                            contextfound = contextfound + 1
                            continue
                        except:
                            # try the asulist
                            try:
                                _ = asulist.index(guid)
                                asuguid = guid
                                isasu = 1
                                asufound = asufound + 1
                                continue
                            except:
                                # try the flotationlist
                                try:
                                    _ = flotationlist.index(guid)
                                    flotationguid = guid
                                    isflotation = 1
                                    flotationfound = flotationfound + 1
                                    continue
                                except:
                                    # try the potterylist
                                    try:
                                        _ = potterylist.index(guid)
                                        potteryguid = guid
                                        ispottery = 1
                                        potteryfound = potteryfound + 1
                                        continue
                                    except:
                                        # we have a guid that is not a context, asu, flotation bag or pottery bag
                                        # this is unusual but possible; this sample bag belongs to something other
                                        # than those four things (example, a pottery join)
                                        # we will test that there is a context or asu and then leave it
                                        noguid = 1
                                        errguid = guid

                # this first case is where a Context guid was found
                if iscontext == 1:                
                    rowout.append(contextguid)
                    if isasu == 1:
                        rowout.append(asuguid)
                    else:
                        rowout.append('')
                # no context guid found, but an ASU guid
                elif isasu == 1:
                    # find a context guid for this ASU!!!
                    try:
                        contextguid = contextasudict[asuguid]
                        rowout.append(contextguid)
                        contextfound = contextfound + 1
                        rowout.append(asuguid)
                    except: #no context guid found with that asu guid
                        input("\t\tyou have an ASU with no Context UUID")
                        rowout.append("")
                        rowout.append(asuguid)
                else:
                    rowout.append("")
                    rowout.append("")

                if isflotation == 1:
                    rowout.append(flotationguid)
                else:
                    rowout.append("")
                if ispottery == 1:
                    rowout.append(potteryguid)
                else:
                    rowout.append("")
                if noguid == 1 and (iscontext == 0 and isasu == 0):
                    # these are anamolous cases, where the belongs to guid cannot find what it belongs to
                    # did not get a hit on guidlists for contexts, asu, flotation nor pottery bags
                    # print them off to error file to double check
                    # likely they are flotation off a special find, or micromorph off a sample bag
                    missingguid.append(errguid + ": this guid from the following row cannot find parent (it is likely it belongs to something other than context, asu, pottery or flotation)\n" + ' '.join(rowout))
                else:
                    pass

                csvout.writerow(rowout)


        # now write out the guids that could not find their relation
        if len(missingguid) != 0:
            numguid = 0
            efile = '@' + str(infile.stem) + '-BelongsToWithNoParentRecord.txt'
            errfile = errordir / efile
            with open(errfile, 'w') as errout:
                for item in missingguid:
                    numguid = numguid + 1
                    strnumguid = "((" + str(numguid) + "))"
                    errout.write(strnumguid + item)
                    errout.write('\n\n')
            print("\t=============================================================================================")
            print("\tWARNING: While processing " + str(infile.name) + " in order to get relationships, there were")
            print("\t" + str(numguid) + " records with a RelationBelongsTo guid but when searchings guidlists could")
            print("\tnot find a relationship in contexts, asu, flotation or pottery. Look in ")  
            print("\t" + efile + " for the list.")
            print("\t=============================================================================================\n")
            input("\tAny key to continue ...")
        else:
            pass

    return(zeroguid, nobelongstoguids)

def getSBrelations(typedir, maindir):
    # unpack the SampleBag belongs to field and 
    # get the unique guids for items to which the SB is related

    zeroguid = 0
    nobelongstoguids = []
    infile = typedir / 'SampleBag.csv'
    outfile = maindir / 'data/LoadCSVs/SampleBag2.csv'
    errorfile = maindir / 'data/CleanseErrorReports/@SB_WithNoBelongsToGuids.txt'
    #this is bad! hardcoding of the RelationBelongsTo column
    belongstocolumnnum = 14
    zeroguid,nobelongstoguids = getrelations(infile,outfile,belongstocolumnnum,maindir)
    if zeroguid > 0:
        with open(errorfile, 'w', encoding = 'utf8') as fout:
            print('\n\t========================================================================================')
            print('\tWARNING: ' + str(zeroguid) + ' Sample Bags did not have any guids in RelationBelongsTo')
            for item in nobelongstoguids:
                fout.write(item + "\n")
            print("\tFind a list of them in >data>CleanseErrorReports>@SB_WithNoBelongsToGuids.txt")
            print('\t========================================================================================')
            input("Any key to continue ...")

    else:
        pass
    return(zeroguid)

def getSFrelations(typedir, maindir):
    zeroguid = 0
    nobelongstoguids = []
    errorfile = maindir / 'data/CleanseErrorReports/@SF_WithNoBelongsToGuids.txt'
    infile = typedir / 'SpecialFind.csv'
    outfile = maindir / 'data/LoadCSVs/SpecialFind2.csv'
    #this is bad hardcoding of the RelationBelongsTo column
    belongstocolumnnum = 28 #this is bad hardcoding of the RelationBelongsTo column
    zeroguid,nobelongstoguids = getrelations(infile,outfile,belongstocolumnnum,maindir)
    if zeroguid > 0:
        with open(errorfile, 'w', encoding = 'utf8') as fout:
            print('\t========================================================================================')
            print('\tWARNING: ' + str(zeroguid) + ' Special Finds did not have any guids in RelationBelongsTo')
            for item in nobelongstoguids:
                fout.write(item + "\n")
            print("\tFind a list of them in >data>CleanseErrorReports>@SF_WithNoBelongsToGuids.txt")
            print('\t========================================================================================\n')
            input("Any key to continue ...")
    else:
        pass

    return(zeroguid)

def getpotteryrelations(maindir):

    # NOTE:  this module depends on SampleBag2.csv already having
    # been built (in other words SampleBag has already had its
    # belongsto guids flattened into columns)

    # this module will create a v2 of pottery that has the keys of parent records
    # unpacked from belongsto relationship and flattened into columns

    errordir = maindir / 'data/CleanseErrorReports'

    # designate a v2 pottery file as target
    potout = maindir / 'data/LoadCSVs/Pottery2.csv'
    # variable to capture how many rows writen to the v2 file
    pot2nbr = 0
    
    # build a list to capture pottery bags info that do not find foreign keys
    l_missingsb = []
    
    # establish list to hold list of guids
    # guids will be in order of context, asu, samplebag
    l_guids = []
    
    # load the samplebags
    print('\n\tGetting a list of SampleBags to which Pottery Bags can be related.')
    sb = maindir / 'data/LoadCSVs/SampleBag2.csv'
    with open(sb, 'r', encoding = 'utf8') as fin:
        sbin = csv.reader(fin, dialect = 'excel')
        # toss the header
        _ = next(sbin)
        for row in sbin:
            sbguids = [row[17], row[18], row[12]]
            l_guids.append(sbguids)
            
    # load the contexts
    print('\n\tGetting a list of contexts in case a Pottery Bag cannot be related to a Samplebag.')
    sb = maindir / 'data/Types/All_Context.csv'
    with open(sb, 'r', encoding = 'utf8') as fin:
        sbin = csv.reader(fin, dialect = 'excel')
        # toss the header
        _ = next(sbin)
        for row in sbin:
            sbguids = [row[18], row[19], '']
            l_guids.append(sbguids)
            
    # load the SFs
    print('\n\tGetting a list of Special Finds in case a Pottery Bag cannot be related to a context.')
    sb = maindir / 'data/LoadCSVs/SpecialFind2.csv'
    with open(sb, 'r', encoding = 'utf8') as fin:
        sbin = csv.reader(fin, dialect = 'excel')
        # toss the header
        _ = next(sbin)
        for row in sbin:
            sbguids = [row[29], row[30], '']
            l_guids.append(sbguids)
    
    with open(potout, 'w', encoding = 'utf8', newline='') as fout:
        csvout = csv.writer(fout, dialect = 'excel')
        


        # load up the pottery into a csv reader
        pot = maindir / 'data/Types/Pottery.csv'
        with open(pot, 'r', encoding = 'utf8') as potin:
            csvin = csv.reader(potin, dialect = 'excel')

            #now use the pottery rows to find keys in SampleBag and apply to csvout
            print('\n\tWorking through the pottery bags to build foreign keys')
            header = next(csvin)
            header.append('Context_UUID')
            header.append('ASU_UUID')
            header.append('SampleBag_UUID')
            csvout.writerow(header)
            rowshandled = 0
            missingsb = 0
            for row in csvin:
                
                hit = 0
                rowshandled = rowshandled + 1
                rowlist = row
                
                # get the list of IDs that this pottery record belongs to
                # this is bad hard coding of column
                potbelongsto = row[131].strip()

                # break out the GUIDs in the belongsto field
                potbelongslist = potbelongsto.split('\\n')
                
                # now work through l_guid and find foreign keys
                for guid in potbelongslist:
                    for item in l_guids:
                        if guid.strip() in item:
                            if item[0] == '':
                                #we do not have a good context association
                                pass
                            else:
                                # add context
                                rowlist.append(item[0])
                                # add asu
                                rowlist.append(item[1])
                                # add samplebag
                                rowlist.append(item[2])
                                hit = 1
                                break
                        else:
                            pass
                    if hit == 1:
                        break
                    else:
                        pass
                
                if hit == 1:
                    pass
                else:                        
                    missinginfo = "Trench: " + row[0] + "\tcontext: " + row[1] + "\tPot bag :" + row[2] + "\tDesc: " + row[6]
                    l_missingsb.append(missinginfo)
                    missingsb = missingsb + 1

                csvout.writerow(rowlist)
                pot2nbr = pot2nbr + 1

                print('\n\t\tHave evaluated ' + str(pot2nbr) + ' bag', end = '\r')

        if missingsb > 0:
            # write these out to a file
            errfile = errordir / '@PotteryBagsMissingFKs.txt'
            with open(errfile, 'w', encoding = 'utf8') as fout:
                for item in l_missingsb:
                    fout.write(item)
                    fout.write('\n')
            print("\n\t========================================================================================")
            print("\tWARNING: There were " + str(missingsb) + " pottery records that could not find")
            print("\tgood foreign keys and thus will not sum up to context nor will  be findable")
            print("\tin the database.  These bad records are listed in @PotteryBagsMissingFKs.txt")
            print("\t========================================================================================\n")
            input("\tAny key to continue ...")

        else:
            pass

        print("\tHave looked for foreign keys for pot bags numbering: " + str(rowshandled))
        print("\tOf those, this amount were missing good foreign keys: " + str(missingsb))
        print("\tWrote out rows totaling: " + str(pot2nbr))
        print("\tCreated file:  " + str(potout) + "\n")

    return(rowshandled, missingsb)



if __name__ == "__main__":
    maindir = Path('/./Users/nathanmeyer/Keros-Naxos-Seaways-iDigToDBase')
    typedir = Path('/./Users/nathanmeyer/Keros-Naxos-Seaways-iDigToDBase/data/Types')
    zero = getSBrelations(typedir, maindir)
    zero = getSFrelations(typedir, maindir)
    rows, missing = getpotteryrelations(maindir)
    print(str(rows))
    print(str(missing))

