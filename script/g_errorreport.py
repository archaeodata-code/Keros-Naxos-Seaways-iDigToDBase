'''
version:  0.1 JFA

WORK:
    -- 

Supports error reporting for pottery data
'''

import csv

def savepotteryerrors(mytable,myheader,errorjson,errordir):
    
    d_errors = errorjson
    
    # handle if the table we are working with is pottery:
    foutname = str(mytable.name)[0:-4] + "_Errors.csv"
    fout = errordir / foutname

    with open(fout, 'w+') as outfile:
        csvout = csv.writer(outfile)
        l_header = ("code","row number","column","message","value","field type","field format")
        csvout.writerow(l_header)
        for item in d_errors:
            l_row = []
            l_row.extend((item['code'],item['row-number'],myheader[item['column-number']-1],item['message']))
            d_msg = item['message-data']
            for k in d_msg.keys():
                l_row.append(d_msg[k])
            csvout.writerow(l_row)
            
def saveallpotteryerrors(mytable,myheader,errorjson,errordir):
    
    l_errors = errorjson
    
    # handle if the table we are working with is pottery:
    foutname = str(mytable.name)[0:-4] + "_Combined_Mistakes.csv"
    fout = errordir / foutname
    
    # build a dictionary of the rows in Pottery2 with rownumbers as key and value a dictionary of the rows columns
    d_rows = {}
    with open(mytable, 'r', newline='') as fin:
        csvin = csv.DictReader(fin, dialect = 'excel')
        key = 2
        for row in csvin:
            d_rows[key] = row
            key = key + 1

        
    with open(fout, 'w+') as outfile:
        csvout = csv.writer(outfile)
        l_header = ("trench","shapecount","diagcount","code","potrownum","potbag","column","message","value","field type","field format")
        csvout.writerow(l_header)
        for item in l_errors:
            d_row = d_rows[item['row-number']]
            s_potbag = d_row['Identifier']
            l_row = []
            l_row.append(d_row['Source'])
            l_row.append(d_row['ShapeCountComment'])
            l_row.append(d_row['PotteryCountDiagnosticsBeforeMending'])
            l_row.extend((item['code'],item['row-number'],s_potbag,myheader[item['column-number']-1],item['message']))
            d_msg = item['message-data']
            for k in d_msg.keys():
                l_row.append(d_msg[k])

            csvout.writerow(l_row)            
            
def combinepotteryerrors(errordir,targetdir):
    
    outfile = errordir / "All_Pottery_Mistakes.csv"
    with open(outfile, 'w+') as fout:
        csvout = csv.writer(fout)
        l_header = ("trench","code","row number","column","message","value","field type","field format")
        csvout.writerow(l_header)
    
        errorfiles = list(errordir.glob('*Errors.csv'))

        for file in errorfiles:
            if "ottery" in str(file):
                with open(file) as fin:
                    csvin = csv.reader(fin)
                    _ = next(csvin)
                    for row in csvin:
                        l_row = row
                        l_row.insert(0,str(file.name))
                        csvout.writerow(l_row)
                        
                    
            
    
