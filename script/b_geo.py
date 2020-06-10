'''
Version 1.0 JFA
Work:
    -- none
    
This is the coordinating script to build various GIS files.  It will call
various modules to define Well Known Text strings for open and close 
polygons for all the contexts (where possible).  Then,  at both the context 
(including ASUs) and the Context + ASU levels (where ASU is still maintained
as a distinct entity) it will add summary pottery data.  It will then do
the same for Special Finds
'''

from pathlib import Path
from g_contextshapefile import openclose
from g_addpottery import getpottery
from g_sfsummary import getsfsummary
from g_geopackage import buildgeopackage

def geo(maindir):

    # build a shape layer for all the contexts
    print('''
              -------------------------------------------------------------
              Building a GIS layer with polygons for the contexts
              -------------------------------------------------------------
              
          ''')
    openclose(maindir)

    # add pottery data to the records built in previous openclose
    print('''
          -------------------------------------------------------------
          Adding pottery phases & counts to geo_AllContextASU & 
          geo_AllContextNoASU
          -------------------------------------------------------------
              
          ''')
    getpottery(maindir)

    # add sf summary data to the records built in previous openclose
    print('''
              -------------------------------------------------------------
              Adding SF summary counts to geo_AllContextASU & 
              geo_AllContextNoASU
              -------------------------------------------------------------
              
          ''')
    getsfsummary(maindir)
    
    # build a geopackage
    print('''
          
    
              -------------------------------------------------------------
              Building a geopackage
              -------------------------------------------------------------
              
          ''')
    goodpack = buildgeopackage(maindir)
    if goodpack:
        pass
    else:
        print('\n\t\tWarning: building geopackage had problems')



if __name__ == "__main__":
    maindir = Path('/./Users/nathanmeyer/Keros-Naxos-Seaways-iDigToDBase')
    geo(maindir)
