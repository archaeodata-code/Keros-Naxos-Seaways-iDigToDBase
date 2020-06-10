'''
Version 0.1 JFA
Work: 
    
    -- 


This module creates lists/dicts of guids that are using to validate
relationships or otherwise provide support processing guids
'''
import csv

# a set of routines to retrieve the IndentiferUUID from various files

def gettableguids(file):

    # load a list of pottery record guids
    with open(file, "r", encoding = "utf8") as fin:
        treader = csv.DictReader(fin)
        glist = []
        for row in treader:
            glist.append(row['IdentifierUUID'])

    return(glist)


def getcontextguids(file, errordir):

    # load a list of context guids in two lists and a dict
    with open(file, "r", encoding = "utf8") as fin:
        contextreader = csv.DictReader(fin, dialect = 'excel')
        contextlist = []
        asulist = []
        contextasudict = {}
        badrowcount = 0
        badrowlist = []
        for row in contextreader:
            try:
                if row['Type'] == "Partition" or row['Type'] == "ASU":
                    asulist.append(row['IdentifierUUID'])
                    k = row['IdentifierUUID']
                    v = row['Context_UUID']
                    if not v:
                        badrow = row['Source'] + ' : ' + row['Identifier']
                        badrowcount = badrowcount + 1
                        badrowlist.append(badrow)
                    else:
                        pass
                    contextasudict[k] = v
                else:
                    contextlist.append(row['IdentifierUUID'])
            except Exception as error:
                print(error)
                input('in exception for contextasudict')
                
    
        if badrowcount == 0:
            pass
        else:
            print('\n\t================================================================================================')
            print('\tWARNING:  While using All_Context.csv to build a dictionary of ASUs guids and their BelongsTo')
            print('\tGUID (for example the guid for  FillDeposit, etc., to which the ASU belongs), a total of ')
            print('\t' + str(badrowcount) + ' ASUs were added to the dictionary but with a blank Context_UUID guid')
            print('\tLook in @ASU-NoContextUUID.txt for the list of ASUs that do not have a related context')
            print('\t================================================================================================\n')
            errfile = errordir / '@ASU-NoContextUUID.txt'
            with open(errfile, 'w') as fout:
                  for item in badrowlist:
                    fout.write(item)
                    fout.write('\n')
            
                  
    
    return(contextlist, asulist, contextasudict)
