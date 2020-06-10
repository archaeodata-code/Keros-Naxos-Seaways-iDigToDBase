'''
Version 1.0 JFA
Work: 
    -- handle return of badrows from sumpottery?
    -- flattenphotos has no return and no comment to user


This module takes Type files previously built and unpacks the iDig
relationship fields to create the equivalent of database foreign keys.
It also creates a site-wide list of all the contexts and ASUs
'''

from pathlib import Path
from f_buildmastercontext import buildallcontext
from f_unpackrelations import getSBrelations
from f_unpackrelations import getSFrelations
from f_unpackrelations import getpotteryrelations
from f_sumpottery import sumpottery
from f_flattenphotos import flattenphotos


def flatten(typedir, schemadir, maindir):

    # method for building a single, sitewide list of contexts and
    # developing foreign keys so that GIS and database will work well
    
    # clear out the target directory for flattened and summarized files
    print("\tClearing out old LoadCSV files ...\n")
    loaddir = maindir / 'data/LoadCSVs'
    l_oldfiles = list(loaddir.glob('*.*'))
    for f in l_oldfiles:
        try:
            f.unlink
        except Exception as error:
            print(error)
            input("Cannot delete old files")

    # use the site-wide files to build the master context file "allcontext"
    # this file has a (mostly) common subset of fields from all context types
    print('''
    -----------------------------------------------------------------------
    Will now build a master file of all contexts ...
    -----------------------------------------------------------------------
    
        ''')
    
    numerrs = buildallcontext(typedir,schemadir,maindir)
    if numerrs > 0:
        print("\tHave finished building the master context file with GUID errors.")
    else:
        print("\tBuilding the master context file had GUID problems. \n")
    input("Press <enter> to continue or <cntrl-c> to quit.\n")

    # now unpack the relationbelongsto in the samplebag and specialfind 
    # files to better relate to contexts
    print('''
    -----------------------------------------------------------------------
    Unpacking belongsto relationships for samples and special finds.
    -----------------------------------------------------------------------
    
        ''')
        
    badguids = getSBrelations(typedir,maindir)
    if badguids > 0:
        pass
    else:
        print('\tSampleBags all had good RelationBelongsTo fields.')
              
    badguids = getSFrelations(typedir,maindir)
    if badguids > 0:
        pass
    else:

        print('\n\tSpecialFinds all had good RelationBelongsTo fields.')

    #now unpack the relationbelongsto in pottery
    print('''
          
    -----------------------------------------------------------------------
    Unpacking belongsto relationships for for pottery records.
    -----------------------------------------------------------------------
    
        ''')

    rowshandled, missingsb = getpotteryrelations(maindir)
    if missingsb > 0:
        pass
    else:
        print('Handled rows totaling : ' + str(rowshandled))
        print('\tFinding relations for pottery bags had no errors!')

    #now summarize pottery bags to the context or ASU
    print('''
          
    -----------------------------------------------------------------------
    Rolling up pottery bags to the context or ASU
    -----------------------------------------------------------------------
    
        ''')

    badrows = sumpottery(maindir)
    if badrows:
        pass
    else:
        print('\tAll potbag rows summarized well.')
        
    
    #now flatten photo relationships
    #this involves taking any row in Photo.csv that has
    #more than one RelationshipIncludes UUID and 
    #make a row for each relationship
    flattenphotos(typedir)
    

if __name__ == "__main__":
    maindir = Path("/./Users/nathanmeyer/Keros-Naxos-Seaways-iDigToDBase")
    typedir = maindir / 'data/Types'
    schemadir = maindir / 'schema'
    flatten(typedir, schemadir, maindir)
