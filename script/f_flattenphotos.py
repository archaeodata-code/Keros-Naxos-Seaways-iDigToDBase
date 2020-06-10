'''
version:  0.1 JFA

WORK:
    -- this is lazy code opening and closing a write file repeatedly
    -- should create a list of dicts and write once

this code takes the Photos.csv and
makes sure each row has RelationsIncludes to only
a single GUID ... this is so that in the dbase
the keying will work 

preserved iDig IdentifierUUID in a new field for reference
but overwrites IdentifierUUID with new guid so loads to db
work

'''

import csv
from pathlib import Path
import uuid

def flattenphotos(typedir):

    print('\n\t\tFor each photo will create a row per item it relates to')
    
    neval = 0
    nwritten = 0
    fphoto = Path(typedir / 'Photo.csv')
    fphoto2 = Path(typedir / 'Photo2.csv')
        
    
    with open(fphoto, "r", encoding = 'utf8', newline ='') as fin:

        csvin = csv.DictReader(fin, dialect = 'excel')
        l_headers = csvin.fieldnames
        l_headers.append("iDigIdentifierUUID")
        
        with open(fphoto2, 'w', encoding = 'utf8', newline ='') as fout:
            #just open and close this to create the write file
            csvout = csv.writer(fout, dialect = 'excel')
            csvout.writerow(l_headers)
    
        d_row = {}
        for row in csvin:
            d_row = row
            
            if len(row['RelationBelongsToUUID']) > 36:       
                # there is more than one UUID, create a row for each
                l_uuids = d_row['RelationBelongsToUUID'].split('\\n')
                
                newrow = {}
                for item in l_uuids:

                    newrow = d_row
                    # replace the belongsto with just a single guid
                    newrow['RelationBelongsToUUID'] = item

                    #save idig guid
                    newrow['iDigIdentifierUUID'] = newrow['IdentifierUUID']
                    #generate a new GUID
                    newrow['IdentifierUUID'] = uuid.uuid4()
                    
                    #write out the new row
                    with open(fphoto2, 'a+', encoding = 'utf8', newline ='') as fout:
                        csvout = csv.writer(fout, dialect = 'excel')
                        csvout.writerow(newrow.values())
                        nwritten = nwritten + 1                   
                    
            else:
                with open(fphoto2, 'a+', encoding = 'utf8', newline ='') as fout:
    
                    csvout = csv.writer(fout, dialect = 'excel')
                
                    csvout.writerow(d_row.values())

                    nwritten = nwritten + 1
            
            neval = neval + 1
            
    print('\n\t\tLooked at photo rows totaling ' + str(neval))
    print('\t\tWrote photo rows out totaling ' + str(nwritten))
    input('Any key to continue ...')

        
                    
if __name__ == "__main__":
    typedir = Path('/./Users/nathanmeyer/Keros-Naxos-Seaways-iDigToDBase/data/Types')
    flattenphotos(typedir)        
                

