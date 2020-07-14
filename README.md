# iDig To Database (and GeoPackage)

### TL;DR
1. skip to *Python Version and Modules*
2. Make sure your directory structure is good
3. Use iDig Preferences.JSON to dummy up some data

{{TOC}}

## Purpose
The iDig to SQLite Data Processing tools take data exported from Bruce Hartzler's iDig (thanks Bruce!) and move them into a set of CSV files that can support a variety of uses such as import to a database or GIS.  There is also a second round of processing more specific to the project of origin (Keros-Naxos Seaways, directed by Colin Renfrew and Michael Boyd) and this creates additional CSVs for the GIS as well as a GeoPackage and a SQLite database.  All of this can be modified for your project.  The SQLite database is meant not only to serve as project tool but also to be shareable with others including data repositories such as [Open Context](https://opencontext.org/). 

## Basic Requirements
It would be good if a completely non-technical archaeologist could use these tools.  Unfortunately that is not the case.  Minimally you will need to have a rough understanding / tools as follows:

1. You will need a rough familiarity with your platform of choice (Windows, MacOS, Linux) and how to navigate around the installation of tools and possibly configuration files as well as find and use the command line.
2. Unless your iDig set up (or your CSV files otherwise obtained) are just like those used to build this set of tools **AND** your database design is just like the one defined here, you will need a text editor tool to edit the JSON files and the Python scripts.
3. You will need some familiarity with the JSON method of structuring text in a file in a way that is human and machine readable.
4. You will need the Python installed on your computer.  (Directions and sources are beyond the scope of this read me but start here at [Python](https://www.python.org/)).
5. Finally, to make use of the resulting GeoPackage sqlite database that are created, you will need some way of engaging with them.  They have been used with [QGIS](https://qgis.org/en/site/) and DB Browner for [SQLite](https://sqlitebrowser.org).

### Configurability
There is several types of configurability:

1. iDig itself is configurable via the Preferences.JSON file (this is a required file for iDig).  Included in this set of tools is the Preferences.Json file for the Keros-Naxos Seaways project. ***You will want to edit these to match your data model and your quality rules.***
2. The first set of output files produced is defined by a set of JSON schemas using the standard developed by [Frictionless Data](https://specs.frictionlessdata.io/table-schema/#language).  These schemas both define what pieces of data that come out of iDig are kept, but also how they are named and what data quality rules are applied. ***You will want to edit these to match your data model and your quality rules.***
2. The Python code itself can be changed, and some configurability is available is associated parameter files and inline in the code.  ***You will have to edit the Python code to change drive paths as well as there are several places where columns names etc. are hard-coded.***
3. The end database engine can be altered in the python code to support other databases than SQLite (supported here).

## Overview Narrative
This section describes in loose terms how the main steps in the Python programs are executed.  The specific types of outputs are described below under the heading "Outputs".

### The Python Code Flow
The Python code flow starts from exported files from iDig. It first gathers these files and strips them of unwanted columns (configurable via the schema JSON files).  Then it validates at the individual trench level (via the same JSON files).  It then builds a set of site wide "Type" CSV files that roughly correspond to database tables.  These are useful in themselves for trouble shooting, quick reports and queries.  The Python code then "flattens" the iDig data, meaning it unpacks some fields that have multiple values and creates single rows and/or database keys.  From there the code gets more specific to the Keros-Naxos Seaways project by build GIS specific CSVs that summarize key pottery and "special finds" data.  It also builds a GeoPackage for the GIS and ends with build a database of all the flattened Type data.

### General Description of Steps
The below summarizes the steps that one has to take from start to finish.
1. The archaeologist exports one or more trenches from iDig.  
2. Using an appropriate tool such as [iMazing](https://imazing.com), the archaeologist moves these exports from the iPad to the data\iDigExports\Exported_Last folder.  
3. The archaeologist then uses a Python IDE or command line tool to first run the a_iDigToDbase.py script.
4. Various error files and reports are generated along the way and will be useful for trouble shooting data quality issues.

## Standards Applied

### Database Design
There is no clear standard for archaeology informatics that appeared directly applicable to this set of tools.  Several "contenders" were evaluated but in general were found to target a broader scope, be generally quite abstract, and, given our purpose, not specifically very useful.  Here are some further notes:  

1. The naming conventions of the Dublin Core were considered and in some cases adopted.  But as Dublin Core was conceived for cataloging media it is only partially useful.  Note that iDig heavily uses and extends Dublin Core.  Actual experience with the data named after Dublin Core was mixed.
2. Likewise [CIDOC CRM](http://www.cidoc-crm.org/collaborations) was looked at and partially adopted.  However, CIDOC CRM is event based (heavily focused on process both natural and human-based) and more complex in scope and intent then this project. 
3. [OCHRE and ArchaeoML](https://ochre.uchicago.edu/) were reviewed but found to cover a far broader scope and thus necessarily abstract and once again overly complex to implement in this context.
4. [Open Context](https://opencontext.org/) was also reviewed but does not have the same range of data as used here.

In the end then, a design for the database was specific to the Keros-Naxos Seaways Project.  Not ideal but practicable for this scope of work.

### Database Definition And Data Processing
Here there was a direct and simple answer on what standard to use for the actual expression of the database design: [Frictionless Data](http://frictionlessdata.io/) has published a well designed set of standards to express database and data entity designs as well as useful [Python tooling(https://github.com/frictionlessdata/tableschema-py) for processing the data from source to target.

[GeoPackage](https://www.geopackage.org) was selected as the target for a GIS build as the resulting file is extremely portable.

[SQLite](https://www.sqlite.org/) is our target database for three reasons:  1) it is well supported in Python and 2) it is self-contained and easily moved from computer to computer and 3) we plan to build an iPad tool using SQLite in the future.  In SQLite there is no support for  DATETIME, DATE and TIME data types.  Instead they are managed as ISO compliant strings.


## Processing Details

### iDig - The Source Of Data
The iDig application is the ultimate source of data for this set of tools.  iDig exports by Trench and the exports include all the Types and SubTypes as configued in the iDig Preferences.JSON.  For the most part the iDig exports correlate well to the concept of a database entity.  There are however, several details to the iDig implementation that need special handling.

1. iDig does not enforce many data validation rules.  This is inline with the creator's belief that thinking while recording is better than having the iDig application constrain how you record.  In our experience this can have potentially mixed results.  This set of tools enforces some data quality rules via the Preference.JSON file or iDig and in the table schema JSON files and these rules are to a degree configurable and extensible. 
2. Embedded one-to-many relationships.  The iDig application allows the archaeologist to embed within a record one-to-many relationships to "child" entities (as example:  a Context entity can embed many "child" Partitions (or ASUs) as would be represented in the stratigraphic record).  This is done by using the "Includes" relationship to record more than one item (hence creating the 1:M relationship).  This poses a special set of processing issues in order to build a somewhat normalized database structure.  This is accounted for in this set of tools.
3. Depending on individual practices of archaeologists/projects, iDig has the potential to likewise embed a theoretical many-to-many relationship.  An example of this would be if a field sample is take and the archaeologist records more than one "Belongs To" relationship.  This was not attempted on the Keros-Naxos Seaways Project and thus is not represented in these tools.

### Database Design & Configurability
The lack of discipline-wide data standards within archaeology means that any database design will be controversial.  Here we mean the "logical" or "ideal" representation of the information domain: archaeology.

iDig itself is quite configurable, meaning that any project utilizing these tools could and likely would have a slightly different implementation both at a macro level (entities and how the "relationship" feature of iDig implements the notion of entity relationships) and the micro level (fields and controlled language for fields) This set of tools was built in the context of one specific archaeology project and thus reflects that specific implementation of iDig.

For these tools to be of general applicability then, they needed to be configurable and extensible.  This is handled in the following ways:

1. iDig : Preferences.JSON
2. Database Design:  table schema JSON files
3. GIS Design : geoschema JSON files

In addition to database and table design, the tools are meant to be somewhat independent of the actual database engine.  While written with SQLite as the target, by using a different database engine plug-in a different engine can be targeted.  For more on plug-ins see **Python Version and Required Modules**.  In addition to a different plug-in it is probable the Python code will need to be modified.

### Python Version and Modules

This was developed in Python 3.7.

The specific version of Python was that provided by [Anaconda](https://www.anaconda.com/products/individual).

For simplicity's sake all the required modules beyond the basic installation where install using the Anaconda desktop application. The Anaconda desk-top tool is not without its problems but still a good place to start as it also is a good workbench for other tools you might want (such as IDEs and statistical tools).

In addition to modules installed by default, the following are also required:

1. [Pandas](https://pandas.pydata.org) - used to manipulate table-like data.
2. [GeoPandas](https://geopandas.org) - for building GeoPackages.
3. [Table Schema](https://github.com/frictionlessdata/tableschema-py) - for working with CSV files.
4. [Good Tables](https://github.com/frictionlessdata/goodtables-py) for data validation.
5. [Shapely](https://shapely.readthedocs.io/en/latest/) and [PyShp](https://pypi.org/project/pyshp/#examples) for working geometry at a basic level (likely just need one but that will require some code rework).
6. [SQLAlchemy](https://www.sqlalchemy.org) - for database connectivity and build
7. JSON, SQLite, CSV etc come with your basic Python installation (at least in 3.7)

### Specifically On Database and GeoPackage
Important points:

1. These both rely on JSON schema files for their build.
2. These rebuild the entire database or GeoPackage each time.  The premise here is the "single source of truth" for data during the lifetime the project is iDig.  When the project is done, and archived version of the data will be placed on [Zenodo](https://zenodo.org).

### Directory Structure
As developed the code works with the folder structure that was part of this zip file.  If you change you will have to change the code.

## Meta

1. ### Version Number: 0.1 JFA

1. ### Change Log

1. ### Contributors & Contact
Nathan Meyer [orcid](https://orcid.org/0000-0001-6847-8199)
 


