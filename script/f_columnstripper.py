'''
version:  0.1

WORK:
    -- bad use of JSON reading lines vs using JSON fields
    -- Inefficient rebuilding of columns dicts per CSV file versus per type
    -- Error handling in module stripper
    -- get rid of shutil and use Pathlib

'''




import pandas
from shutil import copy
from pathlib import Path



def columns(jsonfile):
# helper module
# build a list of desired columns from JSON file
    key=0
    dictstring = ""
    try:
        with open(jsonfile) as f:
            # build a list of column names from the json file
            for line in f:
                if "\"name\"" in line:
                    linestring = line.strip()
                    key += 1
                    keystring = str(key)
                    linestring = linestring.replace(",", "")
                    linestring = linestring.replace("\"", "")
                    linestring = linestring.replace("name", keystring)
                    dictstring = dictstring + linestring + "|"
                else:
                    pass
        
            dictstring = dictstring.replace("|\"", "\"")
            dictstring = dictstring[0:-1]

            
            # build a dictionary object from the string of names
            columndict = dict(item.split(':') for item in dictstring.split('|'))

        return(columndict)

    except FileNotFoundError:
    
        columndict = {}
        return(columndict)


# helper module
# module uses Pandas (heavy!) to actually strip unwanted columns
def stripper(oldfile, columndict):

    # build a list of just the values from the dictionary
    columns = []
    for v in columndict.values():
        columns.append(v)
    
    try:
        # use pandas to read the csv
        if str(oldfile)[-3:] == "tab":
            csvin = pandas.read_csv(oldfile,sep='\t', encoding="utf8", low_memory=False)
        else:
            csvin = pandas.read_csv(oldfile, encoding = "utf8")
    
        # create a new pandas data frame with only the desired columns
        try:
            csvout = csvin[columns]
            # save the file off
            newfile = "Stripped_" + oldfile.name[:-3] + "csv"
            newfile = oldfile.parent / newfile
            csvout.to_csv(newfile, encoding="utf8", mode='w', index=False)
            oldfile = ""
        
        except Exception as error:
            print("\n")
            print(error)
            print("\n")
            print(oldfile)
            print("\n")
            print(columndict)
            print("\n")
            print(columns)
            print("\n")
            input("problem!!  hit cntrl-c to cancel program and resolve issue")

    except pandas.errors.ParserError as e:
        # probably have hanging data off the end of the columns
        print("\tWARNING: Stripping did not work for: " + oldfile + "\n")
        print("\t" + e)
        input("\tpress enter to continue")
    
    except OSError:
        # probably file is missing
        print("\tFATAL ERROR: missing csv file: " + oldfile + "\n")

    # return an empty string if all worked or a file name if not
    return(oldfile)


def strip(sourcedir, schemadir, targetdir, scriptdir, maindir):
    # main module to get rid of unwanteds columns from the raw iDig exports
    
    # set the error directory for results of stripping and create an empty file
    # to hold the filenames of exports not stripped
    errordir = maindir / 'data' / 'CleanseErrorReports'
    
    # set up a file for items that have no JSON for stripping
    # copy the last error file to a last version for comparison
    try:
        efile = errordir / '@FilesWithNoSchema.txt'
        efile.rename(errordir / '@FilesWithNoSchemaPrevious.txt')
        print("\tSaved a copy of previous list of files with no corresponding JSON schema ...\n")
    except:
        # there was no previous file so just ignore

        print("\tWARNING: no previous error file to version ... ignoring ...\n")
    efile = errordir / '@FilesWithNoSchema.txt'
    with efile.open(mode='w') as fout:
        fout.write("======================================================\n")
        fout.write("Files with no JSON schema for stripping\n")
        fout.write("These should be expected based on not having a\n")
        fout.write("JSON schema to strip the files\n")
        fout.write("======================================================\n\n")

    # as well as a list of files successfully stripped
    # copy the last error file to a last version for comparison
    try:
        wfile = errordir / '@FilesStripped.txt'
        wfile.rename(errordir / '@FilesStrippedPrevious.txt')
        print("\tSaved a copy of previous list of stripped files for comparison ...\n")
    except:
        # there was no previous file so just ignore
        print("\tWARNING: no previous list of stripped files to version ... ignoring ...\n")
    wfile = errordir / '@FilesStripped.txt'
    with wfile.open(mode='w') as fout:
        fout.write("======================================================\n")
        fout.write("Files stripped using JSON definition\n")
        fout.write("These should be expected based on having a type\n")
        fout.write("JSON schema with which to strip the file\n")
        fout.write("and the existence of that type in the trench\n")
        fout.write("======================================================\n\n")

    # set our list to capture filenames that had JSON but failed to be stripped
    # this is our return value 
    badstripper = []

    # set our list to capture files well stripped
    # we will write this out to capture names of files stripped
    goodstripper = []

    # clear out the target directory for aggregated files and error files
    print("\tClearing out old files ... \n")
    
    tdir = targetdir
    n = 0
    for file in tdir.glob('*.*'):
        try:
            dfile = Path(file)
            dfile.unlink()
            n = n + 1
        except Exception as error:
            print("\tWas attempting to delete this file: " + file)
            print("\tBut encountered this error:")
            print("\t" + error)
            input("Any key to continue or cntrl-C to exit.")
            pass

    print("\tDeleted files = " + str(n) + "\n")

    # move the exported tab files to the target directory
    print("\tMoving exported CSVs over to working directory ...\n")
    sdir = sourcedir
    n = 0
    for file in sdir.glob('**/*.tab'):
        try:
            dfile = Path(file)
            copy(dfile, targetdir)
            n = n + 1
        except Exception as error:
            print("\tWas attempting to copy this file: " + file)
            print("\tBut encountered this error:")
            print("\t" + error)
            input("Any key to continue or cntrl-C to exit.")
            pass

    print("\tCopied files = " + str(n) + "\n")

    # now iterate through files, stripping them with appropriate schema
    print("\tStripping out unwanted columns ... \n")

    # build a dictionary with file name as key and json schema name as value
    # this will be used to select appropriate schema to use as guide to stripping columns
    # the inputs for this dictionary are in an editable file called schema.txt
    filedict = {}
    noschemaerror = 0
    p = scriptdir / 'schema.txt'
    with open(p) as f:
        for line in f:
            line = line.strip()
            (key, val) = line.split(":")
            filedict[key] = val

    # now iterate through list of files in the target directory and strip unwanted columns
    nexamined = 0
    nstripped = 0
    for file in tdir.glob('*.tab'):
        nexamined = nexamined + 1
        strfile = file.name
        nomatch = 0
        # get the correct schema by matching a portion of the file name in the dictionary
        # this is sloppy but only way to handle the messy file names?
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
                    # giving up and throwing an error
                    # this means that the file name did not match any of the above patterns
                    # for schema names in the dictionary
                    nomatch = 1

        if nomatch == 1:
            # we have CSV file with no matching JSON schema
            # this will often be okay as it is files we do not want to process
            # for example the top level exported file which is hte concatenation of all iDig
            # fields we do not use in this program
            noschemaerror = 1 
            writefile = errordir / '@FilesWithNoSchema.txt'
            with open(writefile, "a") as fout:
                fout.write(strfile)
                fout.write('\n')
 
        else:    
            # we have a JSON schema for this CSV file
            # get the desired columns from schema
            sfile = schemadir / schema
            # build a dictionary of fields in schema
            columndict = columns(str(sfile))

            if columndict:
                # we have a good dictionary of columns wanted
                # strip the unwanted columns
                badfile = stripper(file, columndict)
                # if badfile returned not empty, add to badfiles list
                if badfile:
                    badstripper.append(badfile)
                else:
                    # badfile indicates successful stripping so write to list
                    goodstripper.append(str(file))
                    nstripped = nstripped + 1
                print('\t' + str(nexamined) + " files examined, " + str(nstripped) + " stripped thus far", end='\r')

            else:
                # we had a JSON file but building column dictionary failed
                noschemaerror = 1 
                writefile = errordir / '@FilesWithNoSchema.txt'
                with open(writefile, "a") as fout:
                    fout.write(str(sfile))
                    fout.write('\n')

            
    if noschemaerror == 1:
        print("\n\n\t======================================================================")
        print("\tWARNING: There were some files that had no corresponding JSON schema.")
        print("\tThe JSON schema defines what columns to keep versus strip out.")
        print("\tLikely this is as intended unless you are missing a JSON schema.")
        print("\tLook in file data>CleanseErrorReports>@FilesWithNoSchema.txt for the list of files.")
        print("\t======================================================================\n")
    else:
        pass

    goodstripper.sort()
    writefile = errordir / '@FilesStripped.txt'
    with open(writefile, "a") as fout:
        for item in goodstripper:
            fout.write(item)
            fout.write('\n')
    print("\tYou will find a list of files stripped in the file")
    print("\tdata>CleanseErrorReports>@FilesStripped.txt\n")

    return badstripper




