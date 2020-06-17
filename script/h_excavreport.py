import pandas as pd
import json
from pathlib import Path

def getcolumns(schemadata):

    # create a list with the dicts for each attribute from the JSON schema
    d_columns = schemadata["fields"]
    l_columns = []


    for item in d_columns:            
        l_columns.append(item['name'])

    return(l_columns,d_columns)


def typereport(datadir,schemadata,l_columns,d_columns):

    datafile = schemadata["csv"]
    csvfile = datadir / datafile
    filename = schemadata["reportname"]
    reportname = datadir / filename
    d_newcolumns = {}

    with open(csvfile, newline='') as fin:

        #using pandas as it just is easy :-)
        df = pd.read_csv(fin, usecols = l_columns)
        # change the column names to user friendly version
        for item in d_columns:
            d_newcolumns[item['name']] = item['title']
        df.rename(d_newcolumns, inplace = 'true', axis='columns')
        input(list(df.columns))
        df.to_csv(reportname,index=False, sep=',', encoding='utf-8')



def report(maindir):
    datadir = maindir / 'data'
    schemadir = maindir / 'schema/excavation_report'
    l_schema = schemadir.glob('er*.json')

    for item in l_schema:

        schemadata = json.load(open(item))

        l_columns,d_columns = getcolumns(schemadata)

        typereport(datadir,schemadata,l_columns,d_columns)

if __name__ == "__main__":
    maindir = Path('/./Users/nathanmeyer/Keros-Naxos-Seaways-iDigToDBase')
    report(maindir)