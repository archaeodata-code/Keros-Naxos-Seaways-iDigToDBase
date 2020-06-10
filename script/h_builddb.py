import pandas as pd
from sqlalchemy import create_engine
import json
from pathlib import Path
import datetime as dt
from sqlalchemy import Table, Column, String, Float, MetaData
   
def instantiatedb(dbname):
    # this module creates the blank sqlite database
    # you would replace this if you target a different database
    # other than SQLite

    s_engine = 'sqlite:///' + str(dbname)        
    dbengine = create_engine(s_engine, echo=False)
    return(dbengine)
    
def builddb(dbengine, datadir, schemadir):
    
    goodbuild = True

    print("\n\t\tGetting list of schemas ...")  
    schemadata = json.load(open(schemadir/ 'dbschema/dbschema.json'))

    # create a list of schemas
    l_schemadicts = schemadata["dbschema"]
    
    print("\t\tWill now build and load a db table for each schema ...")
    for item in l_schemadicts:
        schema = schemadir / item['schema']
        

        newtable, l_columns = buildtable(schema,dbengine)
        baddb = loaddb(dbengine, datadir, schema, newtable, l_columns)
        if baddb:
            input('\n\t\tWARNING: a bad build result; some rows did not load')
            goodbuild = False
        else:
            pass
        
    return(goodbuild)
    
def buildtable(schema, dbengine):
    #  this module builds a blank table on the dbase

    schemadata = json.load(open(schema))
    tablename = schemadata["tablename"]

    # create a list with the dicts for each attribute from the JSON schema
    l_fielddicts = schemadata["fields"]
    l_stringfields = []
    l_numberfields = []
    # will pass next list to load routine
    l_columns = []

    # drop the identifier now
    for item in l_fielddicts:
        if item['name'] == 'IdentifierUUID':
            pass
        elif item['type'] == 'number':
            l_numberfields.append(item['name'])
        else:   
            l_stringfields.append(item['name'])
            
        l_columns.append(item['name'])
    
    # create a table definition
    metadata = MetaData()
    newtable = Table(tablename, metadata,
    Column('IdentifierUUID', String(), primary_key=True),
    *(Column(name, String()) for name in l_stringfields),
    *(Column(name, Float()) for name in l_numberfields))


  
    # build the table on the db
    newtable.create(dbengine)
    print('\n\t\tGood: ' + tablename + ' table built on database')
  
    return(newtable, l_columns)

def loaddb(dbengine, datadir, schema, newtable, l_columns):
    
    schemadata = json.load(open(schema))
    csvfile = schemadata["dbcsv"]
    csvfile = datadir / csvfile
    
    n_err = 0
    n_commit = 0
    print('\n\t\tLoading rows from ' + csvfile.stem)

    with open(csvfile, newline='') as fin:

        #using pandas as it handles types well
        df = pd.read_csv(fin, usecols = l_columns)
        dfile = df.to_dict(orient = 'records')
        for tablerow in dfile:
            try:
                ins = newtable.insert().values(tablerow)
                conn = dbengine.connect()
                conn.execute(ins)
                n_commit = n_commit + 1
                
            except Exception as error:
                print('\n\n=========================================')
                print(str(error))
                n_err = n_err + 1
                print('\t\tNumber of failed rows = : ' + str(n_err))
            

    
    print("\n\t\t\tThe number of rows that did not load was: " + str(n_err)) 
    print("\t\t\tThe number of rows that committed was: " + str(n_commit))
    if n_err > 0:
        baddb = True
    else:
        baddb = False
        
    return(baddb)
    

def dbbuild(maindir):
    # get a timestamp for dbname
    o_dt = dt.datetime.utcnow()
    s_timestamp = str(o_dt.isoformat(sep='+', timespec='minutes'))
    s_dbname = 'keros-' + s_timestamp + '.sqlite'
    print('\n\t\tWill build a database named >>data/' + s_dbname)
    input('Any key to continue ...')

    dbname = maindir / 'data' / s_dbname
    # get target directory for database table schemas
    schemadir = maindir / 'schema'
    # get target directory for data csv files for db build
    datadir = maindir / 'data'
    
    dbengine = instantiatedb(dbname)
    goodbuild = builddb(dbengine, datadir, schemadir)
    
    return(goodbuild)

if __name__ == "__main__":
    maindir = Path('/./Users/nathanmeyer/Keros-Naxos-Seaways-iDigToDBase')
    dbbuild(maindir)