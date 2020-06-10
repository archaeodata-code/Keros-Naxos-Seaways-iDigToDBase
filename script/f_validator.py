'''
version:  0.1 JFA

WORK:
    -- confirm goodtables bug with unique identifiers is fixed and remove hack
    -- use DictReader instead of csvreader to avoid hard coding column positions

This module provides methods to unpack relationships for
development of foreign keys
'''

import json
import csv
import datetime
from pathlib import Path
from goodtables import validate
from g_errorreport import savepotteryerrors, saveallpotteryerrors, combinepotteryerrors

def filecleanse(targetdir, schemadir, maindir):
    
    
    # set the directory for an error file
    errordir = maindir / 'data' / 'CleanseErrorReports'

    schemasmissing = 0  # to count how many stripped files were missing a validation schema
    badvalidator = []  # this will hold any files that fail to execute the validation routine

    # build a dictionary with file name as key and json schema as value
    filedict = {}
    sfile = maindir / 'script' / 'schemacsv.txt'
    with open(str(sfile)) as f:
        for line in f:
            line = line.strip()
            (key, val) = line.split(":")
            filedict[key] = val

    # make sure we have a good dictionary of schemas
    if len(filedict) == 0:
        input("The dictionary meant to hold schema targets is empty.")
    else:
        pass

    # validate each file against its schema
    l_files = []
    try:
        l_files = list(targetdir.glob('Stripped*.csv'))
        nfiles = len(l_files)
        print("\tWill be validating files totaling: " + str(nfiles) + '\n')
    except:
        input("arg! - not able to load list of files!")

    nokay = 0
    nexamined = 0

        
    # set up the error file
    datetimetxt = str(datetime.datetime.now())
    errorfilename = '@AllContextErrorReport' + datetimetxt + '.txt'
    errorfile = errordir / errorfilename
        
    #l_files.append(Path('/./Users/nathanmeyer/iDigToDBase/data/Types/Pottery.csv'))
    
    for file in l_files:

        nexamined = nexamined + 1
        # get the correct schema
        noschema = 0
        badfile = ""

        strfile=str(file)
        try: 
            schema = filedict[strfile[-8:]]

        except KeyError:
            # we have a couple files that do not match the -8 pattern
            try:
                schema = filedict[strfile[-10:]]

            except KeyError:
                # also have some with this pattern
                try:
                    schema = filedict[strfile[-11:]]

                except KeyError:
                    noschema = 1
                    print("no schema for " + strfile)

        # apply data quality if there is schema, else write out to no schema file
        if noschema == 1:
            schemasmissing = 1 
            writefile = errordir / '@CannotValidateDueToNoSchema.txt'
            try:
                with open(writefile, "a+") as fileout:
                    fileout.write(strfile)
                    fileout.write('\n')
            except FileNotFoundError:
                with open(writefile, "w") as fileout:
                    fileout.write(strfile)
                    fileout.write('\n')
            else:
                input("There is an unknown error writing out to @CannotValidateDueToNoSchema.txt")
        else:
            schema = schemadir / schema
            #perform the actual cleanse
            badfile = cleanse(file, schema, maindir,errorfile,targetdir)
            if badfile:
                badvalidator.append(badfile)
            else:
                nokay = nokay + 1
            
        print('\t' + str(nexamined) + " files examined, " + str(nokay) + " without errors", end='\r')

    print("\n\n\tFiles that have no errors = " + str(nokay) + "\n")
    
    if nfiles != nokay:
        print("\t===================================================")
        print("\tWARNING: " + str(nexamined-nokay) + " files had validation errors.")
        print("\tEach file with errors has a corresponding error report in a JSON file in >data>CSVExports.")
        print("\tThe errors are aggregated in >data>CleanseErrorReports>@ErrorReport.txt")
        print("\t===================================================\n")
    else:
        print("No files had validation errors.")
        
    if schemasmissing:
        print("\tSome files to be validated were missing their JSON schema.")
        print("\t\tLikely this is by intent.")
        print("\t\tLook in the file @FilesWithNoSchema.txt for the names.")
    else:
        pass


    return badvalidator


def cleanse(mytable, myschema, maindir, errorfile, targetdir):
    # Note:  this validation routine handles 10K rows
    # If you need more rows validated, see default value
    # in goodtables config.py
    
    errortuple = False

    errordir = maindir / 'data' / 'CleanseErrorReports'
    # myschema = "/./" + str(myschema)
    
    if myschema.exists():
        pass
    else:
        print(str(myschema))
        input("cannot get to schema file")

    # make sure we have a valid target table:
    if not mytable.exists():
        print("\n")
        print("\t.........................................................")
        print("\tWARNING---->")
        print("\t" + mytable.stem + " does not exist.\n")
        input("\tPress <enter> to continue")
        badfile = mytable

    else:
        try:
            report = validate(mytable,schema=str(myschema), order_fields=True)
            
        except Exception as error:
            print(mytable)
            print('This is the error while trying to validate:  ' + str(error))
            input("Validation not working")
            badfile = mytable
            report = {}
            report['error-count'] = 1
            errortuple = True

        if report['error-count'] == 0:
            badfile = ""

        else:           
        #handle if there were errors on the goodtables validate call
            #get the table rows for metadata
            with open(mytable, encoding = 'utf8') as fin:
                corruptdata = False
                csvin = csv.reader(fin, dialect = "excel")
                # use a try statement incase the error is corrupt data
                try:
                    _ = next(csvin)
                    # we will later use the tablelist to find relevant iDig metadata for error
                    tablelist = []
                    for row in csvin:
                        tablelist.append(row)
                except Exception as error:
                    print(error)
                    input("trouble reading data from mytable")
                    #pdb.set_trace()
                    corruptdata = True

            # grab the error from cleanse report
            try:
                with open(errorfile, 'a+') as ferr:
                    if corruptdata:
                        print("\tCorrupted or untranslatable text in " + str(mytable))
                        ferr.write(str(mytable) + "\n")
                        ferr.write("\tCorrupted or untranslatable text in this table\n\n")
                        print("\n\n")
                    elif errortuple == True:
                        ferr.write(str(mytable) + "\n")
                        ferr.write("\ttuple index out of range\n\n")
                    else:
                        errorjson = report['tables']
                        errorjson = errorjson[0]
                        myheader = errorjson['headers']
                        ferr.write(errorjson['source'])
                        ferr.write("\n")
                        errorjson = errorjson['errors']

                        for item in range(len(errorjson)):
                            errordict = errorjson[item]
                            ferr.write("\t" + "error type: " + errordict["code"] + "\n")
                            try:
                                iDigItem = tablelist[(errordict["row-number"]-2)]
                                ferr.write("\t" + "iDig Object: " + iDigItem[0] + " - " + iDigItem[1] + " - " + iDigItem[2] + " - " + iDigItem[3] + "\n")  
                                #ferr.write("\t" + "column: " + header[(errordict["column-number"]-1)] + "\n")
                                column = errordict["column-number"]
                                column = column - 1
                                #there is bug in goodtables with required or unique on UUID
                                # does not provide correct column so a hack to fix that
                                # at some point when the bug is fixed this has to be removed
                                if myheader[column] == "RelationBelongsTo":
                                    columnname = "IdentifierUUID"
                                elif myheader[column] == "RelationIncludes":
                                    columnname = "IdentifierUUID"
                                elif myheader[column] == "DateTimeZone":
                                    columnname = "IdentifierUUID"   
                                else:                                
                                    columnname = myheader[column]
                                ferr.write("\t" + "field: " + columnname + "\n")
                                # note the test avoids a long ugly line for unique constraint violations
                                msg = str(errordict["message"])
                                if msg[0:4] == "Rows":
                                    msg = "Unique constraint violation on " + myheader[column]
                                else:
                                    pass
                                ferr.write("\t" + "message: " + msg + "\n\n")
                                
                            except:
                                iDigItem = "Could not access error item ..."
                                ferr.write("\t" + "message: " + iDigItem + "\n\n")


            #except FileNotFoundError:
            except Exception as error:
                input(error)
                with open(errorfile, "a+") as ferr:
                    ferr.write("\t" + "message: Missing " + mytable + "\n\n")

            # prep a file name for validation 
            fout = mytable.stem + "_Cleanse.JSON"
            dpath =  maindir / 'data/CleanseErrorReports/ErrorJSON'
            fpath = dpath / fout
            with open(fpath, 'w+') as outfile:
                try:            
                    json.dump(report, outfile)
                except Exception as error:
                    print(error)
                    outfile.write("There is no JSON error report as error validation did not work.")
            badfile = mytable

            # do the specific pottery error processing if the table we are working with is pottery:
            
            if 'data/Types/Pottery.csv' in str(mytable):
                print("\tPerforming validation on combined type file of pottery.csv")
                saveallpotteryerrors(mytable,myheader,errorjson,errordir)

            elif 'ottery' in str(mytable):
                fout = str(mytable)[0:-4] + "_Errors.csv"
                savepotteryerrors(mytable,myheader,errorjson,errordir)
            
            else:
                pass

    # now as redundant check combine the individual pottery file error records
    combinepotteryerrors(errordir,targetdir)

    return badfile


if __name__ == "__main__":
    maindir = Path('/./Users/nathanmeyer/Keros-Naxos-Seaways-iDigToDBase')
    schemadir = Path('/./Users/nathanmeyer/Keros-Naxos-Seaways-iDigToDBase/schema')
    targetdir = Path('/./Users/nathanmeyer/Keros-Naxos-Seaways-iDigToDBase/CSVExports')
    filecleanse(targetdir, schemadir, maindir)
