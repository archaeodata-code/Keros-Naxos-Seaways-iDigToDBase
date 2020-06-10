'''
version:  0.1

WORK:
    -- remove hardcodoing of RelationBelongsTo column
    -- use DictReader instead of csvreader to avoid hard coding column positions
    -- externalize fields to be summed to param file
    -- add non-numeric summaries for connent fields
    -- refactor
    -- 124


note this module relies upon Pottery2.csv already being created ..
in other words, we have already extracted Context_UUID and ASUU-UUID
from the belongsto relationship and created them as separate field values

this module takes the individual pottery bag readings and rolls them
up to a context and ASU level, performing math on various count/weight fields
as well as aggregating all text.  Two files created: one where pottery is rolled
to the ASU and where no ASU to the Context level. A second where all is rolled
to the context level (losing resolution at ASU level)

'''

import csv
import pandas as pd
from pathlib import Path


def sumpottery(maindir):

    # set the error directory
    errordir = maindir / 'data/CleanseErrorReports'

    # set the Pottery2 file as data source summary and vessel building
    inpotfile = maindir / 'data/LoadCSVs/Pottery2.csv'
    
    # get the total number of rows in Pottery2
    with open(inpotfile) as f:
        n_potrecs = sum(1 for line in f) - 1
        print("\n\t\tNOTE: Pottery2  has rows totaling " + str(n_potrecs))
        
    # build some tools to work with contexts and asu
    contextasudict,contextnamedict = buildcontextasudict(maindir)

    # process Pottery2 for and build a unique list of ASU and Contexts that have sherds
    with open(inpotfile, "r", encoding = 'utf8', newline ='') as fin:
        csvin = csv.DictReader(fin, dialect = 'excel')
        allcontextlist = []
        onlycontextlist=[]
        asudict={}
        n_evalrows = 0
        n_goodrow = 0
        n_badrows = 0
        
        # build a list to hold Pottery2 records that cannot be summarized
        # as they lack context or asu keys
        l_notsummarized = []
        
        for row in csvin:
            n_evalrows = n_evalrows + 1
            
            # populalate the allcontext list for rows with context and ASU
            if row['Context_UUID']:
                # only store the Context_UUID once
                try:
                    _ = allcontextlist.index(row['Context_UUID'])
                except Exception:
                    allcontextlist.append(row['Context_UUID'])
            else:
                pass
                
            # if ASU_UUID is not blank, then this is ASU
            # make sure we have a Context_UUID for this ASU
            if row['ASU_UUID']:
                if row['Context_UUID']:
                    try:
                        contextuuid = contextasudict[row['ASU_UUID']]
                    except Exception as error:

                        print('\n\tWarning.....')
                        print('\t\t' + error)
                        print("\t\tA pot bag in Pottery2 belongs to an ASU but did not have ContextID")
                        print("\t\tand thus we will not be able to summarize pottery up to the context.\n")
                        input("\t\tSolve this before summarizing pottery")
                else:
                    contextuuid = row['Context_UUID']
                    
                
                # add the ASUs to dict with value as context uuid
                asudict[row['ASU_UUID']] = contextuuid
                n_goodrow = n_goodrow + 1
                
            # else if ASU_UUID is blank than this may be a context
            # that had no ASUs
            elif row['Context_UUID']:
                # only store the Context_UUID once
                try:
                    _ = onlycontextlist.index(row['Context_UUID'])
                except Exception:
                    onlycontextlist.append(row['Context_UUID'])
                    n_goodrow = n_goodrow + 1

            else:
                # except we do have some pottery bags found in flotation that have bag keying problems
                # eventually we need to print off this list and fix "belongsto" relationships for these
                n_badrows = n_badrows + 1
                badrow = "Trench: " + row['Source'] + "\tContext: " + row["ContextID"] + "\tID: " + row["Identifier"] + "\tBelongsTo: " + row["RelationBelongsTo"]
                l_notsummarized.append(badrow)
                
        print("\n\t\t" + str(n_evalrows) + " pottery2 evaluated")
        print("\t\t" + str(n_goodrow) + " pottery2 rows found a Context or ASU")
        print("\t\t" + str(n_badrows) + " rows did not have ASU or Context UUIDs")
        print('\t\tWill be summarizing pottery data to ' + str(len(allcontextlist)) + ' contexts with ' + str(len(asudict)) + ' ASUs\n')
        input("Any key to continue")


    # summarize pottery2 data to context and asu level
    sumpotscontextasu(maindir,errordir,inpotfile,onlycontextlist,asudict)
    
    # sum data to context level (add asu to context)
    sumpotscontext(maindir,errordir,inpotfile,allcontextlist,contextasudict,contextnamedict)

    return(n_badrows)


def sumpotscontextasu(maindir,errordir,inpotfile,onlycontextlist,asudict):
    
    #-----------SUMMARIZE TO CONTEXT OR ASU LEVEL---------------------------------
    # take the detail potbag records and summarize to Context and ASU
    # note the context number should not include the ASU pottery


    # build some tools to hold processed values
    columntext = maindir / 'script/potteryroll.txt'
    columns, header, columndict, potvals = buildsummarytools(columntext)

    print('\n\t\t-----------------------------------------------------------')
    print('\t\tSummarizing pottery data first to a context and ASU level\n')

    # open up a write file to hold the summarized data
    outfile = maindir / 'data/LoadCSVs/PotterySummarizedAtContextASU.csv'
    with open(outfile, "w", encoding = 'utf', newline = '') as fout:
        csvout = csv.writer(fout, dialect = 'excel')

        # write the header
        csvout.writerow(columns)

        # load up a dataframe with Pottery2 rows to perform summary
        # the dataframe allows for easy subselection of rows matching a GUID
        with open(inpotfile, "r", encoding = 'utf8', newline ='') as fin:
            
            # we are just pulling columns we want with usecols
            dfin = pd.read_csv(fin, usecols = columns, skip_blank_lines = True)
            dfna = dfin.fillna(value=0)
                        
            # get the total number of pottery in this pottery2 dataframe
            n_totpots = dfna['PotteryCountBeforeMending'].sum()

            print("\n\t\t"+ str(n_totpots) + " is the sherd count to be summarized")

            # prep a variable to hold running total of summed pots
            n_sumpots = 0

            # work through the list of context UUIDs with no ASUs and summarize up to a summary pottery row
            #nbrcontexts = len(contextlist)
            nbrcon = 0
            
            # a list for rows that could not be summarize
            l_badrows = []
            
            #######################################################################
            # process the contexts for the contextasu file
            # for each unique context in pottery2 process summary row
            for context in onlycontextlist:

                # this next line sub-selects the dataframe for context GUID
                # but where no ASU exists
                df=dfna.loc[(dfna['Context_UUID'] == context) & (dfna['ASU_UUID'] == 0)]

                # drop the rows that matched this Context from the main dataset
                indexNames = dfna[(dfna['Context_UUID'] == context) & (dfna['ASU_UUID'] == 0)].index
                dfna.drop(indexNames , inplace=True)
                
                # preserve the source and context but if error - dataframe is empty
                try:
                    sourcetxt = df['Source'].iloc[0]
                    contexttxt = df['ContextID'].iloc[0]

                except Exception as error:
                    print("-------WARNING----------")
                    print(error)
                    print('\t Context is: ' + context)
                    print("\tTried to find pottery rows for context but found none")
                    print("\tEmpty dataframe from Pottery2")
                    input('Any key to continue ...')
                    sourcetxt = 'empty data frame'
                    contexttxt = ''        
                
                # prep a new clean dictionary with zero values to pass into this
                convals = potvals.fromkeys(potvals, 0)
                # now iterate through the sub-selection
                b_all = False
                d_recs = df.to_dict(orient = 'records')
                potsummary, l_badrows = summarizepotteryforcontext(b_all,d_recs,convals,columndict,l_badrows)
               
            
                # add keys to the summarized pottery data
                potsummary["Context_UUID"] = context
                potsummary["ASU_UUID"] = ''
                potsummary['Source'] = sourcetxt
                potsummary['ContextID'] = contexttxt
                
                # add this contexts pots to the total number of summed pottery
                n_sumpots = n_sumpots + potsummary['PotteryCountBeforeMending']

                # write out the row
                csvout.writerow(list(potsummary.values()))
                nbrcon = nbrcon + 1
            
            if len(l_badrows) > 0:
                # set a file to collect records that could not be summed correctly
                sumerrfile = errordir / '@PotteryBagsContextASUNotSummarized-Con.txt'
                with open(sumerrfile, "w") as fout:
                    for item in l_badrows:
                        fout.write(item)
                        fout.write('\n\n')
                    print("\n\t==============================================================")
                    print("\t\tWARNING: There were some Contexts which had pottery that") 
                    print("\t\tcould not be summarized")
                    print("\t\tFind them in @PotteryBagsContextASUNotSummarized-Con.txt")
                    print("\t==============================================================")
            else:
                pass
            
            print('\n\t\tSo far for just contexts we have summed sherds totaling ' + str(n_sumpots))
            input('Any key to continue ...')
            
            #######################################################################
            # process the asu for the contextasu file
            # work through the list of ASU UUIDs and summarize up to a summary pottery row

            nbrasu = 0

            # prep a list to hold rows that could not be summed
            l_badrows = []
            
            for k,v in asudict.items():

                # this next line sub-selects the dataframe for context GUID
                df=dfna.loc[dfna['ASU_UUID'] == k]

                # drop the rows that matched this ASU
                indexNames = dfna[dfna['ASU_UUID'] == k].index
                dfna.drop(indexNames , inplace=True)
                
                # preserve the source and identifier but skip if error - dataframe is empty
                try:
                    sourcetxt = df['Source'].iloc[0]
                    contexttxt = df['Context_UUID'].iloc[0]

                except Exception as error:
                    print("\n-------WARNING----------")
                    print('\t' + str(error))
                    print('\t\tASU_UUID is: ' + str(k))
                    print('\t\tContext_UUID is: ' + str(v))
                    print("\tTried to find pottery rows for ASU but found none")
                    print("\tEmpty dataframe from Pottery2")
                    input('Any key to continue ...')
                    sourcetxt = 'empty data frame'
                    contexttxt = '' 
 
                # prep a new clean dictionary with zero values to pass into this
                asuvals = potvals.fromkeys(potvals, 0)
                # now iterate through the sub-selection
                b_all = False
                d_recs = df.to_dict(orient = 'records')
                potsummary, l_badrows = summarizepotteryforcontext(b_all,d_recs,asuvals,columndict,l_badrows)

                # add keys to the summarized pottery data
                potsummary['Source'] = sourcetxt
                potsummary['ContextID'] = contexttxt
                potsummary["Context_UUID"] = v
                potsummary["ASU_UUID"] = k
                
                # add this contexts pots to the total number of summed pottery
                n_sumpots = n_sumpots + potsummary['PotteryCountBeforeMending']
                
                # write out the row
                csvout.writerow(list(potsummary.values()))
                nbrasu = nbrasu + 1
            
            if len(l_badrows) > 0:
                # set a file to collect records that could not be summed correctly
                sumerrfile = errordir / '@PotteryBagsContextASUNotSummarized-ASU.txt'
                with open(sumerrfile, "w") as fout:
                    for item in l_badrows:
                        fout.write(item)
                        fout.write('\n\n')
                    print("\t==============================================================")
                    print("\tWARNING: There were some ASUs which had pottery that could not be summarized")
                    print("\tFind them in @PotteryBagsContextASUNotSummarized-ASU.txt")
                    print("\t==============================================================")
            else:
                pass
            
            
            
    print('\n\t\tWe have summed ' + str(n_sumpots) + " sherds across unique contexts and ASUs")
    print('\t\tWe started with a sherd count of ' + str(n_totpots))
    input("Any key to continue ...")
    

    if len(dfna.index) > 0:
        print('\n\t\t===================================================================================')
        print('\t\tWARNING:  There were some un-summarized pot bag rows ')
        print('\t\t' + str(len(dfna.index)) + ' total rows were in this non-matching group')
        print('\t\tFind them listed in >data>CleanseErrorReports>@PotteryBagsAtContextASUNotSummarized2.csv')
        print('\t\t=====================================================================================\n')
        
        efile = maindir / 'data/CleanseErrorReports/@PotteryBagsAtContextASUNotSummarized2.csv'
        dfna.to_csv(efile, index=None, sep=',', mode='w')
    else:
        pass

    print('\n\tCreated file: ' + str(outfile.stem))
    print('\t\tPottery2 had a total sherd count of ' + str(n_totpots))
    print('\t\tWe summed up pottery rows with sherds totaling ' + str(n_sumpots))
    input("Any key to continue ...")


def sumpotscontext(maindir,errordir,inpotfile,allcontextlist,contextasudict,contextnamedict):
    #-----------SUMMARIZE TO CONTEXT---------------------------------
    #note: this makes use of the file created by 
    # sumpotscontextasu:  PotterySummarizedAtContextASU.csv
    
    #note: the context total should here include the pottery from the ASUs
    
    # summarize the pottery from ASUs up into their context.
    # the previous file created preserved the distinct ASUs
    # (alongside those contexts that did not have ASUs) for possible future use
    # but we also want the data at the context not the ASU level


    # build some tools to hold processed values
    columntext = maindir / 'script/potteryroll.txt'
    columns, header, columndict, potvals = buildsummarytools(columntext)

    print('\n\t\t-----------------------------------------------------------------------')
    print('\t\tSummarizing all pottery data now to context level (ASUs rolled up)\n')
     
    print('\n\t\tWe will summarize to contexts totaling: ' + str(len(allcontextlist)))
    input('Any key to continue ...')
    

    # open up a csvwriter to write the summarized data
    outfile = maindir / 'data/LoadCSVs/PotterySummarizedAtContext.csv'
    with open(outfile, "w", encoding = 'utf', newline = '') as fout:
        csvout = csv.writer(fout, dialect = 'excel')
        #write the header
        csvout.writerow(columns)

        # load up a dataframe with Pottery2 rows to perform summary
        # the dataframe allows for easy subselection of rows matching a GUID
        with open(inpotfile, "r", encoding = 'utf8', newline ='') as fin:
            
            # we are just pulling columns we want with usecols
            dfin = pd.read_csv(fin, usecols = columns, skip_blank_lines = True)
            dfna = dfin.fillna(value=0)
                        
            # get the total number of pottery in this pottery2 dataframe
            n_totpots = dfna['PotteryCountBeforeMending'].sum()

            print("\n\t\t"+ str(n_totpots) + " is the sherd count to be summarized")

  
        # prep a variable to hold summary of summed pots
        n_sumpots = 0

        # work through the list of context UUIDs and summarize up 
        # to a summary pottery row; keep track of how many rows summarized
        nbrcon = 0

        # a list for rows that could not be summarize
        l_badrows = []
    
        #######################################################################
        # process the contexts for the context file
        # for each unique context in pottery2 process summary row
        for context in allcontextlist:

            # this next line sub-selects the dataframe for context GUID
            # but where no ASU exists
            df=dfna.loc[(dfna['Context_UUID'] == context)]

            # drop the rows that matched this Context from the main dataset
            indexNames = dfna[(dfna['Context_UUID'] == context)].index
            dfna.drop(indexNames , inplace=True)
                
            # preserve the source and context but if error - dataframe is empty
            try:
                sourcetxt = df['Source'].iloc[0]
                contexttxt = df['ContextID'].iloc[0]

            except Exception as error:
                print("\n-------WARNING----------")
                print(error)
                print('\t Context is: ' + context)
                print("\tTried to find pottery rows for context but found none")
                print("\tEmpty dataframe from Pottery2")
                input('Any key to continue ...')
                sourcetxt = 'empty data frame'
                contexttxt = '' 
                
            # prep a new clean dictionary with zero values to pass into this
            convals = potvals.fromkeys(potvals, 0)
            # now iterate through the sub-selection
            b_all = False
            d_recs = df.to_dict(orient = 'records')
            potsummary, l_badrows = summarizepotteryforcontext(b_all,d_recs,convals,columndict,l_badrows)

            # add keys to the summarized pottery data
            potsummary["Context_UUID"] = context
            potsummary["ASU_UUID"] = ''
            potsummary['Source'] = sourcetxt
            potsummary['ContextID'] = contexttxt

            # add this contexts pots to the total number of summed pottery
            n_sumpots = n_sumpots + potsummary['PotteryCountBeforeMending']

            # write out the row
            csvout.writerow(list(potsummary.values()))
            nbrcon = nbrcon + 1

        if len(l_badrows) > 0:
            sumerrfile = errordir / '@PotteryBagsContextNotSummarized-AllCon.txt'
            with open(sumerrfile, "w") as fout:
                for item in l_badrows:
                    fout.write(item)
                    fout.write('\n\n')
                print("\t==============================================================")
                print("\tWARNING: There were some Contexts which had pottery that could not be summarized")
                print("\tFind them in @PotteryBagsContextNotSummarized-AllCon.txt")
                print("\t==============================================================")
        else:
            pass
    
        if len(dfna.index) > 0:
            print('\n\t\t===================================================================================')
            print('\t\tWARNING:  There were some un-summarized pot bag rows ')
            print('\t\t' + str(len(dfna.index)) + ' total rows were in this non-matching group')
            print('\t\tFind them listed in >data>CleanseErrorReports>@PotteryBagsAtContextNotSummarized2.csv')
            print('\t\t=====================================================================================\n')
            
            efile = maindir / 'data/CleanseErrorReports/@PotteryBagsAtContextNotSummarized2.csv'
            dfna.to_csv(efile, index=None, sep=',', mode='w')
        else:
            pass    
    
        print("\n\t\tCreated file: " + str(outfile.stem))
        print("\t\tSummarized sherds totaling : " + str(n_sumpots))
        input("Any key to continue ...")

    return(l_badrows)


def summarizepotteryforcontext(b_all,d_recs,convals,columndict,l_badrows):
    # this module is used to summarize pottery rows per context or asu 
    
    for row in d_recs:
        
        # for each key add up the values    
        for k in columndict.keys():
            
            if b_all:
                k == 100
            else:
                pass
            
            rowval = row[k]

            # do not add up context and ASU guids and Source and Identifier obviously
            if k in ["Source","ContextID","Context_UUID","ASU_UUID"]:
                pass
            
            # Concatenate the strings for the descriptive fields
            elif k in ["Identifier","Description","PotterySpecialistInitials","PotteryJoins",
                       "PotteryInventoriedMaterial",	"PotteryImports","PotteryWheel","PotteryKastri",	
                       "PotteryImpressions","PotteryMarks","PotterySherdSizes","PotteryWear","PotteryBurning",	
                       "FabricCountComment","ShapeCountComment",	"ShapeFabricCorrelation","DecorationCountComment",	
                       "SurfaceTreatmentComment","FeatureCountComment","CoverageTemporal",
                       "ChronologyComment","IdentifierUUID","RelationBelongsTo",	"RelationBelongsToUUID",	
                       "RelationIncludes","RelationIncludesUUID","SampleBag_UUID",
                       "PotteryFabricBakingPansOrHearths","PotteryFabricCooking","PotteryFabricStoragePithoi",	
                       "PotteryFabricBraziers","PotteryFabricSauceBoats","PotteryFabricBowls","PotteryFabricCups",
                       "PotteryFabricDepas","PotteryFabricTankards","PotteryFabricConicalNeckedJars","PotteryFabricBasins",	
                       "PotteryFabricJugs","PotteryFabricBeakedJug","PotteryFabricPyxides","PotteryFabricLeggedVessels",	
                       "PotteryFabricOpenShape","PotteryFabricClosedShape","PotteryFabricLids","PotteryFabricMoulds",	
                       "PotteryFabricUnknown"]:      
                
                if convals[k] == 0:
                    convals[k] == ''
                else:
                    pass
                
                convals[k] = str(convals[k]) + '\\n' + str(rowval)

                
            # add up counts for numeric fields
            else:
                try:
                    if rowval == '':
                        pass
                    else:
                        nval = int(float(rowval))
                        oval = convals[k]
                        oval = int(oval)
                        convals[k] = oval + nval

                except Exception as error:

                    print("WARNING1-----------------------------------")
                    print(error)
                    srce = str(row['Source'])
                    print("Trench ", srce)
                    contxt = str(row['ContextID'])
                    print("Context ", contxt)
                    pb = str(row["Identifier"])
                    print("PotteryBag # ", pb)
                    print("Field ", k)
                    print("Has a bad value: ", rowval)
                    input("Any key to continue")
                    print('\n')
                    badrow = srce + " " + contxt + " " + pb + " " + str(k) + " " + str(rowval)
                    l_badrows.append(badrow)

    return(convals, l_badrows)

          
def buildsummarytools(columntext):

    # build a header row from text file that holds desired column names
    # for the csv that will have the summarized pottery data
    # at same time build a list of column names and a dict with column names
    # as key and 0-n as value and another dict with columns as key and 0 for value
    print('\n\t\tGathering names of pottery bag fields that need summarization.')
    columndict = {}
    columns = []
    header = ''

    with open(columntext) as filein:
        for line in filein:
            # build up the list of column names we are interested in
            colname = line.strip()
            columns.append(colname)
            # also build a header
            if header == '':
                header = colname
            else:
                header = header + ',' + colname

    # now build a dict that holds the column number as value for key of column name
    print('\n\t\tPreparing a dictionary to hold summarized pottery counts.')
    nbr = -1
    for item in columns:
        nbr = nbr + 1
        columndict[item] = nbr


    # now build a dict that will be used to store the summarized data from the 
    # selection of pottery bags that are in that context or ASU
    potvals = {}
    for item in columns:
        potvals[item] = 0  
        
    return(columns, header, columndict, potvals)


def buildcontextasudict(maindir):
        
    # set the file for list of valid context and asu records
    allcontext = maindir / 'data/Types/All_Context.csv'


    # get dictionaries of context / asu from all_context so we can find 
    # missing context numbers on pottery bags as needed.
    # Note that if this All_Context.csv has rows that do not have a
    # context number we still have a problem downstream when we do a look up
    # also build a dictionary with Identifieruuid and Identifier from pottery bag
    
    with open(allcontext, "r", encoding = 'utf8', newline ='') as fin:

        contextasudict = {}
        contextnamedict = {}
        n_totcontexts = 0
        n_contexts = 0
        csvin = csv.DictReader(fin, dialect = 'excel')
        for row in csvin:
            n_totcontexts = n_totcontexts + 1
            try:
                # if a row is an ASU by type, add to contextasudict
                if row['Type'] == "Partition" or row['Type'] == "ASU":
                    k = row['IdentifierUUID']
                    v = row['Context_UUID']
                    contextasudict[k] = v
                    
                else:
                    n_contexts = n_contexts + 1
                    k = row['IdentifierUUID']
                    v = row['Identifier']
                    contextnamedict[k] = v
            except Exception as error:
                 print(error)
                 input('iproblem adding to dictionary') 
                 
    print("\n\t\tNOTE: All_Context file has rows totaling " + str(n_totcontexts))   
    print("\t\tThere are non-ASU contexts totaling " + str(n_contexts))
    print("\t\tThere are ASUs totaling " + str(len(contextasudict)))
    input("Any key to continue")


    return(contextasudict,contextnamedict)



if __name__ == "__main__":
    maindir = Path('/./Users/nathanmeyer/Keros-Naxos-Seaways-iDigToDBase')
    badrows = sumpottery(maindir)
    print(str(badrows))