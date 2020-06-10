'''
version:  0.1

WORK:
    -- 

This takes the pottery join data and creates many to many relationships
from the iDig relationship field
Very much draft
'''

import csv
import copy
from pathlib import Path

def flattenjoins(typedir):
    
    # this code takes the pottery_join.csv and
    # creates a M:M join row to the context(s)
    # for the join
    
    neval = 0
    nwritten = 0
    nsingle = 0
    fjoin = Path(typedir / 'Pottery_Join.csv')
    fjoins = Path(typedir / 'Pottery_Join_MM_Context_Temp.csv')

    l_headers = ['Source', 'Type', 'Subtype', 'Title', 'DateEarliest', 'DateModified', 'DateTimeZone', 'IdentifierUUID', 'Joins','PotBag-UUID', 'Context-Identifier','Context-UUID', 'ASU-UUID']

    with open(fjoins, 'w', encoding = 'utf8', newline ='') as fout:
        #just open and close this to create the write file
        csvout = csv.writer(fout, dialect = 'excel')
        csvout.writerow(l_headers)
        
    
    with open(fjoin, "r", encoding = 'utf8', newline ='') as fin:

        csvin = csv.DictReader(fin, dialect = 'excel')
    
        for row in csvin:

            newrow = copy.deepcopy(row)
            # drop the last column
            del newrow['RelationIncludesUUID']
            
            
            if len(row['RelationIncludesUUID']) > 36:
 
                # there is more than one UUID, create a row for each
                l_uuids = row['RelationIncludesUUID'].split('\\n')
                print(l_uuids)
                print('\n')

                for item in l_uuids:

                    newrow['PotBag-UUID'] = item
                    
                    with open(fjoins, 'a+', encoding = 'utf8', newline ='') as fout:
        
                        csvout = csv.writer(fout, dialect = 'excel')
                        csvout.writerow(newrow.values())

                        nwritten = nwritten + 1
                    

            else:
                with open(fjoins, 'a+', encoding = 'utf8', newline ='') as fout:
    
                    csvout = csv.writer(fout, dialect = 'excel')
                    csvout.writerow(row.values())
                    nsingle = nsingle + 1
                    nwritten = nwritten + 1
            
            neval = neval + 1
            
    print('\t\tLooked at join rows totaling ' + str(neval))
    print('\t\tRows with just a single UUID totaling ' + str(nsingle))    
    input('\t\tWrote MM join rows out totaling ' + str(nwritten) + '\n')

    # now add the context data to the new file
    neval = 0
    nerror = 0
    nwritten = 0
    fpotbags = Path(loaddir / 'Pottery2.csv')
    fjoins = Path(typedir / 'Pottery_Join_MM_Context_Temp.csv')
    fdone = Path(typedir / 'Pottery_Join_MM_Context.csv')

    # first build dict of all the pottery bags from pottery2 using potbag uuid
    with open(fpotbags, "r", encoding = 'utf8', newline ='') as fin:

        csvin = csv.DictReader(fin, dialect = 'excel')
        d_potbags = {}
        for row in csvin:
            l_fields = [row['ContextID'],row['Context-UUID'],row['ASU-UUID']]
            d_potbags[row['IdentifierUUID']]=l_fields

    # now build dict of all the pottery bags from pottery2 using samplebag uuid
    with open(fpotbags, "r", encoding = 'utf8', newline ='') as fin:

        csvin = csv.DictReader(fin, dialect = 'excel')
        d_potbags2 = {}
        for row in csvin:
            l_fields = [row['ContextID'],row['Context-UUID'],row['ASU-UUID']]
            d_potbags[row['SampleBag-UUID']]=l_fields   

    # now read all the temp join rows and append context data
    with open(fdone, 'w', encoding = 'utf8', newline ='') as fout:
        csvout = csv.writer(fout, dialect = 'excel')
        input(l_headers)
        csvout.writerow(l_headers)

    with open(fjoins, "r", encoding = 'utf8', newline ='') as fin:
        csvin = csv.DictReader(fin, dialect = 'excel')

        for row in csvin:
            l_newfields = []
            berror = False
            print(row)
            neval = neval + 1
            newrow = copy.deepcopy(row)
            print(newrow)
            try:
                l_newfields = d_potbags[row['PotBag-UUID']]
            except:
                #error should not happen but maybe they attached join to sample bag not pottery bag
                try:
                    l_newfields = d_potbags2[row['PotBag-UUID']]
                except:
                    print('no bag')
                    nerror = nerror + 1
                    berror = True
            if berror:
                pass
            else:
                newrow['Context-Identifier']=l_newfields[0]
                newrow['Context-UUID']=l_newfields[1]
                newrow['ASU-UUID']=l_newfields[2]
                print(newrow)
            with open(fdone, 'a+', encoding = 'utf8', newline ='') as fout:
                csvout = csv.writer(fout, dialect = 'excel')
                csvout.writerow(newrow.values())
                nwritten = nwritten + 1

    print('\t\tLooked at join rows totaling ' + str(neval))
    print('\t\tThere were missing pot bags totaling ' + str(nerror))
    input('\t\tWrote MM join rows out totaling ' + str(nwritten) + '\n')


if __name__ == "__main__":
    typedir = Path('/./Users/nathanmeyer/Keros-Naxos-Seaways-iDigToDBase/data/Types')
    flattenjoins(typedir)        
                

