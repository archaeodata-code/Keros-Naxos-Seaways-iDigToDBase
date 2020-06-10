'''
version:  1.0 JFA

WORK:
    -- the return values from assemble, flatten and geo

The main program
Note:  code legibility was valued over efficency and elegance.
Whether that was achieved is a matter of opinion ;-)  Tabs were used
instead of spaces for indentation.
'''

from pathlib import Path
from b_assemble import assemble
from b_flatten import flatten
from b_geo import geo
from h_builddb import dbbuild

# method for gathering directory selection from user
def getdirectory(dirtype):
    
    b_dir = False
    while not b_dir:
        dirpath = Path(input("What is the path to your " + dirtype + " files?"))
        # check to make sure directory given is valid
        if dirpath.exists():
            b_dir = True
            print("Found directory at:  " + str(dirpath) + "\n")
        else:
            print("That is not a good directory path.  Type the full path name.")

    return(dirpath)

def getdirectories():
    # get source directory for exported CSVs from iDig
    # you can delete this hard coded path and uncomment the code below or edit this path
    sourcedir = Path('/./Users/nathanmeyer/Keros-Naxos-Seaways-iDigToDBase/data/iDigExports/Exported_Last')
    # dirtype = "Parent Directory for CSVs exported from iDig"
    # sourcedir = getdirectory(dirtype)
    
    # get the top level iDigToDBase folder path
    # you can delete this hard coded path and uncomment the code below or edit this path
    maindir = Path('/./Users/nathanmeyer/Keros-Naxos-Seaways-iDigToDBase')
    # dirtype = "iDigToDBase"
    # maindir = getdirectory(dirtype)
    
    # a good directory has been returned, now set the other paths needed later
    
    # this code assumes the user of these programs has set up the folder structure as recommended
    # set the JSON schema directory
    schemadir = maindir / 'schema'
    # get target directory for corrected csv files
    targetdir = maindir / 'data/CSVExports'
    # get target directory for intermediate iDig type file
    typedir = maindir / 'data/Types'
    # get target directory for final db load csv files (ones manipulated for foreign keys etc)
    # get target directory for python scripts
    scriptdir = maindir / 'script'

    # below code can be uncommented to support alternate directory structures
    #
    # get JSON schema directory
    # dirtype = "JSON schema"
    # schemadir = getdirectory(dirtype)
    #
    # get target directory for corrected csv files
    # dirtype = "corrected and validated trench CSVs"
    # targetdir = getdirectory(dirtype)
    #
    # get target directory for site wide type files
    # dirtype = "site-wide CSV type"
    # typedir = getdirectory(dirtype)
    #
    # get target directory for final db load csv files
    # dirtype = "site-wide load-ready CSV with keys etc"
    # loaddir = getdirectory(dirtype)
    #
    # get target directory python scripts
    # dirtype = "python script"
    # scriptdir = getdirectory(dirtype)

    return(sourcedir, schemadir, targetdir, scriptdir, maindir,typedir)


#main program starts here
print('''
===========================================================
Welcome to iDigToDBase
-----------------------------------------------------------
First data will be assembled from iDig Exports.  Then it will be merged into
cross-site type files.  Next it will be processed to develop foreign keys for
the database and GIS.  Summary data for pottery will be built.  CSV files
specifically for the GIS will be created as well as a GeoPackage.   Finally
a database is built.
    ''')
      
input('\tPress any key to continue or cntrl-C to exit.')


# get directories
print('\n\tDetermining directory structure')
sourcedir, schemadir, targetdir, scriptdir, maindir, typedir = getdirectories()



assemble(sourcedir, schemadir, targetdir, scriptdir, maindir, typedir)
input('\n\n\tTypes built. Press any key to continue or cntrl-C to exit.\n')


flatten(typedir, schemadir, maindir)
input('\n\n\tFiles flattened. Press any key to continue or cntrl-C to exit.\n')

geo(maindir)
input('\n\n\tGIS files built. Press any key to continue or cntrl-C to exit.\n')

goodbuild = dbbuild(maindir)
if goodbuild:
    print('\n\n\tDatabase built with no errors')
else:
    print('\n\n\tDatabase built but with errors')
    
print('=====================================================')
print('iDigToDBase finished')
        