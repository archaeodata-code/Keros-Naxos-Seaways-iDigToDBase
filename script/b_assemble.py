'''
version:  1.0 JFA
WORK:
    
This module takes exported CSVs, strips them of unwanted columns, validates
them at trench level and finally concatenates all the types across trenches
to site-wide type files
'''

from pathlib import Path
from f_columnstripper import strip
from f_validator import filecleanse, cleanse
from f_concatenateidigcsvfiles import concatenatetypefiles



def assemble(sourcedir, schemadir, targetdir, scriptdir, maindir,typedir):

    # method for getting iDig exported CSVs, stripping them of unneeded columns,
    # validating them, aggregating them from trench to site wide etc

    # first strip the csv files to the columns wanted

    print('''
    
        -----------------------------------------------------------------
        Will prep CSV files for validation by stripping unwanted columns
        -----------------------------------------------------------------
        ''')

    l_striperr = strip(sourcedir, schemadir, targetdir, scriptdir, maindir)

    # process the return from stripping step
    print("\tHave stripped exported CSV files of unwanted columns.")
    if not l_striperr:
        print ("\tNo problems with any files that had a JSON schema. \n")

    else:
        print("\tWARNING: These files had problems getting stripped:\n")
        for item in l_striperr:
            print("\t\t" + item)

    input("Press <enter> to continue or <cntrl-c> to quit.")

    # validate individual type files; validation happens
    # at individual trench-type file level to enforce quality rules there
    # prior to concatenating into site-wide files
    print('''
          
        ------------------------------------------------------------------
        Will validate individual stripped CSVs against quality rules ...
        ------------------------------------------------------------------
        ''')
        
    l_notvalid = filecleanse(targetdir, schemadir, maindir)
    print("\tHave finished attempting to validate the files.")
    if not l_notvalid:
        print("\tAll files with JSON schema passed validation. \n")

    else:
        print("\t=======================================================")
        print("\tWARNING: The following files had validation errors:")
        for item in l_notvalid:
            print("\t\t" + str(item))
        print("\t=======================================================\n")


    input("Press <enter> to continue or <cntrl-c> to quit.")

    # concatentate individual type files into site-wide files
    print('''
          
        ------------------------------------------------------------------
        Will concatenate individual type files into site-wide types ...
        ------------------------------------------------------------------
        ''')
        
    b_error = concatenatetypefiles(targetdir, typedir, maindir, scriptdir)
    print("\n\tHave finished concatenating the files.\n")
    if not b_error:
        print("\tAll files concatenated well.\n")

    else:
        print('''
              
            WARNING: Not all files had concatenation targets.
            Likely this is by intent; find names in All_Not_Processed.
            If not by intent, fix the list in script/types.txt.
            
            ''')

    print('''
          All the type files now built.
          
          Will validate the combined pottery.csv file.
          
          ''')
    # validate the combined pottery.csv as a convenience
    # this step provides a single place for all pottery errors to be evaluated
    f_error = Path(maindir / 'data/CleanseErrorReports/@AllPotteryErrors.txt')
    f_table = Path(maindir / 'data/Types/Pottery.csv')
    f_schema = Path(schemadir / 'pottery.json')
    cleanse(f_table, f_schema, maindir,f_error,targetdir)

    print('\n\t---------------------------------------')
    print('\tAssembling complete')


if __name__ == "__main__":
    maindir = Path("/./Users/nathanmeyer/Keros-Naxos-Seaways-iDigToDBase")
    sourcedir = Path('/./Users/nathanmeyer/Keros-Naxos-Seaways-iDigToDBase/data/iDigExports/Exported_Last')
    schemadir = maindir / 'schema'
    targetdir = maindir / 'data/CSVExports'
    typedir = maindir / 'data/Types'
    scriptdir = maindir / 'script'
    assemble(sourcedir, schemadir, targetdir, scriptdir, maindir,typedir)



