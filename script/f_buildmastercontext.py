'''
Version 0.1 JFA
Work: 
    
    -- using Pandas is a bit heavy
    -- normalizng context type fields is hard-coded


This module takes Type files previously built and creates a site-wide 
list of all the contexts and ASUs.
'''

import csv
from pathlib import Path
import pandas as p
from f_columnstripper import columns, stripper
from f_concatenateidigcsvfiles import stripheaders
from f_validator import cleanse


def buildallcontext(typedir,schemadir,maindir):
    # this method takes the various context types and builds a master file with them all
    errordir = maindir / 'data' / 'CleanseErrorReports'

    # first get just the columns wanted
    schema = schemadir / 'allcontext.json'
    columndict = columns(schema)
    print('\n\tHere are the columns to be used for the combined all context file ...')
    for k, v in columndict.items():
        print('\t\t' + v)
    input('Any key to continue ....')

    # iDig does not have a consistent set of fields for each context so "fix" this.
    # This is hard coding that should get fixed - probably build a dictionary
    # from a text file so that editing the text file gives you the right columns to add
    file = typedir / "FillDeposit.csv"
    df = p.read_csv(file, encoding = "utf8")
    df["RelationBelongsTo"]=""
    df["RelationBelongsToUUID"]=""
    df.to_csv(file, mode='w', index=False, encoding="utf8")

    file = typedir / "ASU.csv"
    df = p.read_csv(file, encoding = "utf8")
    df["Sub-Type"]=""
    df.to_csv(file, mode='w', index=False, encoding="utf8")
        
    file = typedir / "Structure.csv"
    df = p.read_csv(file, encoding = "utf8")
    df["DateLatest"]=""
    df["RelationBelongsTo"]=""
    df["RelationBelongsToUUID"] = ""
    df.to_csv(file, mode='w', index=False, encoding="utf8")

    file = typedir / "Surface.csv"
    df = p.read_csv(file, encoding = "utf8")
    df["Sub-Type"]=""
    df["DateLatest"]=""
    df["RelationBelongsTo"]=""
    df["RelationBelongsToUUID"] = ""
    df.to_csv(file, mode='w', index=False, encoding="utf8")

    file = typedir / "Cut.csv"
    df = p.read_csv(file, encoding = "utf8")
    df["Sub-Type"]=""
    df["RelationBelongsTo"]=""
    df["RelationBelongsToUUID"] = ""
    df.to_csv(file, mode='w', index=False, encoding="utf8")

    # now strip the various context files
    print("\n\tStripping excess columns from context files ...\n")
    filelist = ["ASU.csv","Cut.csv","FillDeposit.csv","Structure.csv","Surface.csv"]
    for f in filelist:
        file = typedir / f
        badfile = stripper(file,columndict)
        if badfile:
            print("\tProblem stripping " + badfile)
            input("\tAny key to continue ...")
        else:
            pass

    # now aggregate the contexts into the master
    print("\tAggregating contexts to master file ... \n")
    filelist = list(typedir.glob("Stripped*.csv"))
    print('\t\tHere is the list of files to be aggregated:')
    for item in filelist:
        print('\t\t\t' + item.name)
    input('Any key to continue ...')
                  
    # we need a list of guids for later so build it now
    print('\n\t\tBuilding a list of non-ASU guids to supply as Context_UUID Foreign Key for ASUs')
    contextguidlist = []
    totalguids = 0
    for filename in filelist:
        fileguids = 0
        if filename.name == "Stripped_ASU.csv":
            # we do not want the ASU Identifiers in this list of contexts
            pass
        else:
            fileguids = 0
            print("\n\t\tGathering guids from " + str(filename.stem))
            inf = typedir / filename
            with open(inf, encoding='utf8') as fin:
                csvin = csv.DictReader(fin, dialect = 'excel')
                for row in csvin:
                    contextguidlist.append(row['IdentifierUUID'])
                    fileguids = fileguids + 1
                    totalguids = totalguids + 1
            print('\t\t' + str(fileguids) + ' for a total of ' + str(totalguids))

    print('\n\t\tThere are ' + str(len(contextguidlist)) + ' non-ASU, unique Context_UUIDs\n')
    input('Any key to continue ...')

    writefile = typedir / "All_Context.csv"
    # set a variable that will capture how many ASUs had problems with parent context
    numerrs = 0
    with open(writefile, "w", encoding = "utf8", newline = '') as fileout:
        csvout = csv.writer(fileout, dialect = 'excel')

        for filename in filelist:
            inf = typedir /  filename
            aggrnum = 0
            
            # read through to get guids to act as Foreign Keys in dbase
            with open(inf, encoding='utf8') as fin:
                csvin = csv.DictReader(fin, dialect = 'excel')
                header = csvin.fieldnames + ["Context_UUID"] + ["ASU_UUID"]
                csvout.writerow(header)
                try:
                    # to hold name and guid of ASUs with no context
                    badasunobelongsto = {}
                    badasunocontextfound = {}

                    for row in csvin:
                        # make sure row is ASU
                        if row['Type'] == 'Partition' or row['Type'] == 'ASU':
                            # handle if ASU was not related to its context
                            if row['RelationBelongsToUUID'] == '':
                                row['Context_UUID'] = ''
                                row['ASU_UUID'] = row['IdentifierUUID']
                                badasunobelongsto[row['Identifier']] = row['Source']
                                print('\n\t\tWarning ...')
                                print('\t\t\t' + row['Source'] + ' : ' + row['Identifier'])
                                print("\t\t\tAn ASU has no BelongsTo Relationship - will add a blank value for FK\n")
                            else:
                                # an ASU should only ever belong to a single context
                                # but just in case there are more than one guid in 
                                # relationshipbelongsto this just takes one
                                goodguid = False
                                belongstoguids = row['RelationBelongsToUUID']
                                guidlist = belongstoguids.split('\\n')                             
                                for guid in guidlist:
                                    try:
                                        _ = contextguidlist.index(guid)
                                        # if index hit we will use this guid
                                        goodguid = True
                                        break
                                    except: #Exception as error:
                                        pass
                                if goodguid:
                                    row['Context_UUID'] = guid
                                    # Redundantly add row Identifier to ASU_UUID
                                    row['ASU_UUID'] = row['IdentifierUUID']
                                else:
                                    row['Context_UUID'] = ''
                                    row['ASU_UUID'] = row['IdentifierUUID']
                                    k = row['Identifier']
                                    v = row['Source']
                                    badasunocontextfound[k] = v
                                    print('\n\t\tWarning ...')
                                    print('\t\t\t' + k + ' : ' + v)
                                    print("\t\t\tAn ASU has a belongs to guid that is not in list of contexts ...\n")
            
                        else:
                            # this not an asu
                            row['Context_UUID'] = row['IdentifierUUID']
                            row['ASU_UUID'] = '' # append a blank value for asu as there is no value
                        
                        outrow = []
                        for k,v in row.items():
                            outrow.append(v)
                        csvout.writerow(outrow)
                        aggrnum = aggrnum + 1

                except Exception as error:
                    print(error)
                    input('Any key to continue ...')

            print('\n\t\t' + str(filename.stem) + ' aggregated ' + str(aggrnum) + ' rows')
    # if there are errors with guids write these out to a file
    if badasunobelongsto or badasunocontextfound:
        # we have guid issues
        numerrs = len(badasunobelongsto) + len(badasunocontextfound)
        errfile = errordir / '@AllContextMasterFileBuildErrors.txt'
        with open(errfile, "w") as fileout:
            for k,v in badasunobelongsto.items():
                msg = "Trench " + v + " context " + k + " had no RelationBelongsTo value"
                fileout.write(msg + '\n')
            for k,v in badasunocontextfound.items():
                msg = "Trench " + v + " context " + k + " had a RelationBelongsTo with no associated context"
                fileout.write(msg + '\n')
        print("\n\t\t=====================================================================")
        print("\t\tWARNING: There were " + str(numerrs) + " errors writing to allcontext" )
        print("\t\tFind the errors in @AllContextMasterFileBuildErrors.txt")
        print("\t\t=====================================================================")
        input('Any key to continue ...')

    else:
        pass

    # strip out the headers
    masterfile = typedir / "All_Context.csv"
    flist = []
    flist.append(masterfile)
    stripheaders(flist)


    print("\n\tValidating the combined master context file \n")
    
    errorfile = maindir / 'data/CleanseErrorReports/@CombinedContextErrors.txt'
    targetdir = maindir / 'CSVExports'
    badfile = cleanse(writefile, schema, maindir,errorfile,targetdir)
    if not badfile:
        pass
    else:
      print('''
        =====================================================
          WARNING:  AllContext did have validation errors.
          You will find them in:
          data>CleanseErrors>@CombinedContextErrors.txt
        =====================================================
          ''')
    # return              
    return(numerrs)

if __name__ == "__main__":
    maindir = Path("/./Users/nathanmeyer/Keros-Naxos-Seaways-iDigToDBase")
    typedir = maindir / 'data/Types'
    schemadir = maindir / 'schema'
    numerrs = buildallcontext(typedir,schemadir,maindir)
    print(numerrs)
