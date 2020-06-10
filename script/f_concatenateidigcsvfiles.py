'''
version:  0.1 JFA

WORK:
    -- consider instead of types.txt using the Preference.JSON to build dictionary
    or better yet, use split to get the type names and some sort of loop to aggregate by type
    -- the stripheader code is fragile - should probably be pandas based
'''

from pathlib import Path
import csv

def concatenatetypefiles(targetdir,typedir, maindir, scriptdir):
    # this code builds a single type file from all the individual 
    # trench type files.

    errordir = maindir / 'data' / 'CleanseErrorReports'

    badconcatenator = []

    # clear out the Type directory for aggregated files and error files
    # also will catch if any of the files are open
    print("\tClearing out old files .... \n")
    if typedir.exists():
        try:
            oldfiles = typedir.glob('*.*')
            for f in oldfiles:
                f.unlink()

        except Exception as error:
            print(error)
            print("\tCould not delete old files from target directory")
            print("\tPerhaps you have a file open?")
            input("\tPress <CNTRL-C> to quit and make sure files are closed.")

    else:
        input("not finding the type file directory?")

    # variable to count files that have problems
    error = 0

    # build a dictionary of key, value pairs to help identify writefile target
    # further along this dict will be used to grab trench specific files and
    # find the right aggregate target file to write to
    # the key is a subset of the file name that is the same across all trenches
    # and the value is the name of the target file
    filedict = {}
    ftype = scriptdir / 'types.txt'
    with open(str(ftype),'r') as f:
        for line in f:
            line = line.strip()
            (key, val) = line.split(":")
            filedict[key] = val
            # note - if you add different / new "types" to iDig you have to edit types.txt
            # we are missing several types currently:   OSL, paleop ... this list changes as types are added

    # loop through the list of files, use the filedict to identify the right target file
    # and write lines from source file to appropriate output file
    try:
        allfiles = len(list(targetdir.glob('Stripped*.csv')))
        print("\tWill be aggregating files totaling: " + str(allfiles) + '\n')
    except:
        input("fatal! - not able to load list of files!")

    nexamined = 0
    nconcat = 0
    # the following are to track the count of records for each type
    rowcount = {'ASU.csv':0,'Cut.csv':0,'FillDeposit.csv':0,'Flotation.csv':0,'Section.csv':0,'Micromorphology.csv':0,'Photo.csv':0,'Plan.csv':0,'Pottery.csv':0,'SampleBag.csv':0,'SpecialFind.csv':0,'Structure.csv':0,'Surface.csv':0,'Topography.csv':0,'Pottery_Join.csv':0,'Residue.csv':0,'Shell.csv':0,'Bone.csv':0,'Human_Bone.csv':0}

    for filename in targetdir.glob('Stripped*.csv'):
        nexamined = nexamined + 1
        # a hack to get the trench name which is missing from some of the D19 files
        # when we merged three years of excavation into single trenches
        # we ended up losing the original Trench-Year name and so get it back
        trenchname = str(filename).split('.')[0]
        nlist = trenchname.split('/')
        trenchname = nlist[len(nlist)-1]
        trenchname = trenchname[9:]
        # end hack

        # find the writefile in filedict value using a substring of filename as key
        sfilename = str(filename)
        try: 
            writefile = filedict[sfilename[-8:]]

        except KeyError:
            # we have a couple files that do not match the -8 pattern
            try:
                writefile = filedict[sfilename[-10:]]

            except KeyError:
                # also have some with this pattern
                try:
                    writefile = filedict[sfilename[-11:]]

                except KeyError:
                    # deal with any files not explicitly handled by above
                    print(sfilename)
                    filename = sfilename + "_Err"
                    badconcatenator.append(filename)
                    input("fatal, could not find target concatenation file here.")

        #prefix file with target csv directory
        wfile = typedir / writefile
        with open(wfile, "a+", newline='', encoding='utf-8') as fileout:
            # handle the files that were not readied
            # to be concatenated into type specific files
            # add their name to the list that will hold names
            # of files not processed
            if sfilename[-4:] == "_Err":
                badconcatenator.append(sfilename)
            
            # otherwise process normally
            else:
                with open(filename, "r", newline='', encoding='utf-8') as fin:
                    csvin = csv.reader(fin, dialect = "excel")
                    csvwriter = csv.writer(fileout, dialect = "excel")
                    try:
                        for row in csvin:
                            if row[0] == 'Source':
                                pass
                            else:
                                row[0] = trenchname
                                rowcount[writefile] = rowcount[writefile] + 1
                                nconcat = nconcat + 1
                            csvwriter.writerow(row)

                    except Exception as exception:
                        # there was an error trying to write to the aggregated
                        # file so write to an error log
                        input("Error writing to aggregated type file.")
                        writefile = errordir / '@AggregationErrors.txt'
                        with open(writefile, "a") as fileout:
                            fileout.write(filename)
                            fileout.write('\n')
                            fileout.write(str(exception))
                            fileout.write('\n\n')
                        error = error + 1
        
    print('\t' + str(nexamined) + " files examined, " + str(nconcat) + " non-header rows so far", end='\r')
            
    # if there were files that had no target to aggregate towards
    if badconcatenator:
        efile = errordir / '@FilesWithNoAggregationTarget.txt'
        with open(efile, 'w', newline = '', encoidng = 'utf-8') as fout:
            for item in badconcatenator:
                fout.write(sfilename)
                fout.write('\n')
    else:
        pass
                            
                            
    # if you have files with errors, message that
    if error > 0:
        print('\n\t===============================================================')
        print("\tWARNING: " + str(error) + " files had errors while aggregating. They are: ")
        with open(writefile, 'r') as f:
            for line in f:
                print("\t\t" + line)
        print("\tFind their names in >data>CleanseErrorReports>@AggregationsErrors.txt\n")
        print('\t=================================================================\n')
    else:
        pass

    filelist = list(typedir.glob('*.csv'))

    # strip the extra headers
    stripheaders(filelist)

    print('\n\n\tHave now built ' + str(len(filelist)) + ' type files with data as follows.')
    # show how many rows were added to each type file so that we can check this if needed
    for item in rowcount:
        if rowcount[item] == 0:
            pass
        else:
            print('\t\t' + item + ' received ' + str(rowcount[item]) + ' rows\n')

    return badconcatenator

def stripheaders(filelist):	

    # method to strip unwanted header rows created when concatenating files
    print("\n\t\tWorking to strip unwanted headers now ...\n")

    # work through each file removing all unwanted headers
    for filename in filelist:
        with open(filename, "r", encoding='utf8') as fin:
            lines = fin.readlines()
            nlines = len(lines)

        with open(filename, "w", encoding='utf8') as fout:
            headerNo = 0  # setting header count variable
            nremoved = 0
            nline=0
            for line in lines:
                nline = nline + 1
                print('\t\t' + str(nlines) + ' lines in ' + filename.name + ', handled ' + str(nline), end='\r')
                if line[:6] != 'Source':
                    fout.write(line)
                else:
                    
                    if headerNo == 0:
                        # write the header once
                        fout.write(line)
                        headerNo = 1
                    else:
                        nremoved = nremoved + 1
            print('\n\t\tRemoved headers totaling ' + str(nremoved))
        print('\n')
