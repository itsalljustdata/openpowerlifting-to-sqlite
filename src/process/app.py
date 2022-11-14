#!/usr/bin/env python3

from pprint import pprint as print
from   orm            import *
import requests
from   bs4            import BeautifulSoup
from   sqlalchemy     import text, create_engine
from   sqlalchemy.orm import sessionmaker

from   pathlib        import Path
from   datetime       import datetime
import pandas                          as pd
from   dask           import dataframe as dd
from   zipfile        import ZipFile
import tempfile
import json
import numpy                           as np

OPL_BASE_URL   : str  = 'https://openpowerlifting.gitlab.io/opl-csv'
OPL_DATA_PAGE  : str  = f'{OPL_BASE_URL}/bulk-csv.html'
OPL_DATA_ZIP   : str  = f'{OPL_BASE_URL}/files/openpowerlifting-latest.zip'

THIS_FOLDER    : Path = Path(__file__).parent
DATA_PATH      : Path = THIS_FOLDER.parent.parent
dataFilePath   : Path = DATA_PATH.joinpath('latest.zip')
csvDTypePath   : Path = THIS_FOLDER.joinpath('csv_dtype.json')
dataDBfilepath : Path = DATA_PATH.joinpath('openpowerlifting.sqlite')
engineURI      : str  = f"sqlite:///{str(dataDBfilepath)}"

class DB ():
    def __init__ (self, engineURI : str = engineURI):
        self.engine   = create_engine(engineURI, future=True, echo = ECHO)
        self.Session  = sessionmaker()
        self.Session.configure(bind=self.engine)
    def connect (self):
        return self.engine.connect()
    def dispose (self):
        try:
            self.engine.dispose()
        except:
            ...

def getLatestCommit():
    with requests.get(OPL_DATA_PAGE) as req:
        req.raise_for_status()
        soup = BeautifulSoup(req.content,"html.parser")

    links = [a['href'] for a in soup.select('a[href*="/commits/"]')]

    if not links or len(links) > 1:
        raise Exception (f"Unable to determine latest commit from \'{OPL_DATA_PAGE}\'")

    return links[0]

def download_file(url, filename):
    """
    Download the file and save to disk
    """
    filename.unlink(missing_ok=True)

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        size = 0
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk:
                size += len(chunk)
                f.write(chunk)
    return size

def getTheLatestData():
    """
        This works out if we've got the latest commit from the OpenPowerLifting data page
    """

    latestURL = getLatestCommit()
    commitGUID = latestURL.split('/')[-1]
    now = datetime.now()

    try:
        db = DB ()
        CommitBase.metadata.create_all(db.engine)

        with db.Session() as session:

            commits = session.query(dataCommit).filter_by(commitGUID=commitGUID).all()

            if len(commits) == 0:
                """ this is the first time we have seen this guid """
                thisCommit = dataCommit(commitGUID, latestURL, now)
                session.add(thisCommit)
            else:
                thisCommit = commits[0]
                thisCommit.lastSeen = now

            if not thisCommit.fileSize or thisCommit.fileSize == 0 or not dataFilePath.is_file():
                doDownload = True
            else:
                doDownload = not (thisCommit.fileSize > 0 and thisCommit.fileSize == dataFilePath.stat().st_size)

            if doDownload:
                thisCommit.fileSize   = download_file (OPL_DATA_ZIP, dataFilePath)
                thisCommit.downloaded = now

            session.commit()

            thisGUID = thisCommit.commitGUID

    finally:
        if db:
            db.dispose()

    return thisGUID

def convertInt(x):
    try:
        return abs(int(x))
    except ValueError:
        return np.nan
def convertFloat(x):
    try:
        return abs(float(x))
    except ValueError:
        return float(np.nan)

def loadTheLatestData(commitGUID):
    """
        This loads the data from the local zip file
    """

    dtypes = json.loads(csvDTypePath.read_text())

    parse_dates = []
    converters  = {}

    for dt in dtypes.keys():
        oldVal = dtypes[dt]
        newVal = oldVal
        if oldVal == 'str':
            newVal = str
        elif oldVal == 'np.float64':
            newVal = None
            converters[dt] = convertFloat
        elif oldVal == 'np.int32':
            newVal = None
            converters[dt] = convertInt
        elif oldVal == 'date':
            newVal = str
            parse_dates.append(dt)
        else:
            newVal = str

        dtypes[dt] = newVal

    dtypes = {key: val for key, val in dtypes.items() if val}

    # Recreate the data objects
    try:
        db = DB ()
        for b in [Base,LoadBase]:
            b.metadata.drop_all(db.engine)
            b.metadata.create_all(db.engine)
    finally:
        db.dispose()

    print (f"{datetime.now()} : Reading zip")
    with ZipFile(dataFilePath, mode="r") as archive:
        csvFiles = [f for f in archive.namelist() if f[-4:].lower() == '.csv']
        if not csvFiles or not len(csvFiles) == 1:
            raise Exception (f"Invalid number of csv files in {str(dataFilePath)}")
        tmpDir = tempfile.mkdtemp(suffix=None, prefix=None, dir=None)
        tmpFile = archive.extract(csvFiles[0], path=tmpDir)
        df = dd.read_csv (tmpFile
                         ,blocksize   = "512M"
                         ,dtype       = dtypes
                         ,parse_dates = parse_dates
                         ,converters  = converters
                         )

    # Convert back to a pandas df
    print (f"{datetime.now()} : Pre-Convert  to pd.DataFrame ")
    df = df.compute()
    print (f"{datetime.now()} : Post-Convert to pd.DataFrame ")

    colsKg = {k : k[0:-2] for k in list(df) if k[-2:].lower() == 'kg'}
    if colsKg:
        df = df.rename (columns = colsKg)

    unspecifieds     = ['MeetCountry','Event','MeetState','MeetTown','MeetName','Division','Equipment','Federation']
    df[unspecifieds] = df[unspecifieds].fillna('Unspecified')
    df['Sex']        = df['Sex'].fillna('?')

    def groupByDF(cols):
        if isinstance(cols,str):
            cols = [cols]
        # return df.groupby(cols).size().reset_index(name='cnt')
        return df.groupby(cols).size().reset_index(name = 'resultCnt')

    def createData (ObjectNameDestination
                   ,dataObject
                   ,session
                   ,colMappings : dict = {}
                   ,commit      : bool = False
                   ):

        echo = len(dataObject) > 10000

        if colMappings and len(colMappings) > 0:
            dataObject = dataObject.rename (columns = colMappings)

        if echo:
            print (f"{datetime.now()} : Pre dict    : {ObjectNameDestination} : {len(dataObject)}")
        asDict = dataObject.to_dict(orient="records")
        if echo:
            print (f"{datetime.now()} : Post dict   : {ObjectNameDestination} : {len(dataObject)}")

        if echo:
            print (f"{datetime.now()} : Pre Insert  : {ObjectNameDestination} : {len(dataObject)}")
        session.bulk_insert_mappings(ObjectNameDestination, asDict)
        if echo:
            print (f"{datetime.now()} : Post Insert : {ObjectNameDestination} : {len(dataObject)}")
        if commit:
            session.commit()
        return dataObject

    def createGroupedByData(ObjectNameDestination, cols ,session, colMappings : dict = {}, commit : bool = False):
        return createData (ObjectNameDestination = ObjectNameDestination
                          ,dataObject            = groupByDF (cols)
                          ,session               = session
                          ,commit                = commit
                          ,colMappings           = colMappings
                          )


    try:
        db = DB ()
        with db.Session() as session:
            createGroupedByData(Event,'Event',session, {"Event" : "name"})
            createGroupedByData(MeetCountry,'MeetCountry',session, {"MeetCountry" : "name"})
            createGroupedByData(FederationLoad,['Federation','ParentFederation'],session, {"Federation" : "name","ParentFederation" : "parent_name"})
            createGroupedByData(Division,'Division',session, {"Division" : "name"})
            createGroupedByData(Equipment,'Equipment',session, {"Equipment" : "name"})
            createGroupedByData(SexWeight,['Sex','WeightClass'],session, {"Sex" : "sex", "WeightClass" : "weight_class"}, True)

        dml = []
        dml.append (f'UPDATE {FederationLoad.__tablename__} SET parent_id = (SELECT p.id FROM  {ParentFederationLoad.__tablename__} AS p WHERE p.name = parent_name) WHERE parent_name IS NOT NULL')
        for _not in ['','NOT ']: # need to put the parents in first because of the FK
            dml.append (f"INSERT INTO {Federation.__tablename__}  (id, name, parent_id, resultCnt) SELECT l.id, l.name, l.parent_id, l.resultCnt FROM  {FederationLoad.__tablename__} l WHERE l.parent_id IS {_not}NULL")

        with (db.engine.connect() as conn
             ,conn.begin()
             ):
            for u in dml:
                # print (u)
                conn.execute (text(u))
    finally:
        if db:
            db.dispose()

    try:
        db = DB ()
        with (db.engine.connect() as conn
             ,conn.begin()
             ):

            def readAndPop(obj, joinColsLeft : list):
                try:
                    this = pd.read_sql_table(table_name = obj.__tablename__
                                            ,con        = conn
                                            )
                except AttributeError as ae:
                    print (str(ae))
                    sql = f'SELECT * FROM {obj.__tablename__}'
                    print (sql)
                    this = pd.read_sql_query (sql = sql
                                             ,con = conn
                                             )
                try:
                    this.pop ('resultCnt')
                except KeyError:
                    ...
                thisDrop = [c for c in this.columns.values.tolist() if not c == "id"]
                if joinColsLeft:
                    thisDrop.extend(joinColsLeft)
                return (this, thisDrop)


            # Update the federation ids
            joinColsLeft = ['Federation', 'ParentFederation']
            feds,deleteCols = readAndPop (FederationLoad,joinColsLeft)
            df = df.merge(feds, left_on = joinColsLeft, right_on = ['name', 'parent_name'])
            df = df.rename (columns = {'id': 'federation_id'})
            df = df.drop(deleteCols, axis = 1)

            # Divisions
            joinColsLeft = ['Division']
            divisions,deleteCols = readAndPop (Division,joinColsLeft)
            df = df.merge(divisions, left_on = joinColsLeft, right_on = ['name'])
            df = df.rename (columns = {'id': 'division_id'})
            df = df.drop(deleteCols, axis = 1)

            # Equipment
            joinColsLeft = ['Equipment']
            equipment,deleteCols = readAndPop (Equipment,joinColsLeft)
            df = df.merge(equipment, left_on = joinColsLeft, right_on = ['name'])
            df = df.rename (columns = {'id': 'equipment_id'})
            df = df.drop(deleteCols, axis = 1)

            # Events
            joinColsLeft = ['Event']
            events,deleteCols = readAndPop (Event,joinColsLeft)
            df = df.merge(events, left_on = joinColsLeft, right_on = ['name'])
            df = df.rename (columns = {'id': 'event_id'})
            df = df.drop(deleteCols, axis = 1)

            # Sex weights
            joinColsLeft = ['Sex','WeightClass']
            weightClass,deleteCols = readAndPop (SexWeight,joinColsLeft)
            df = df.merge(weightClass, left_on = joinColsLeft, right_on = ['sex','weight_class'])
            df = df.rename (columns = {'id': 'weightclass_id'})
            df = df.drop(deleteCols, axis = 1)

            meetLocations = groupByDF(['MeetCountry','MeetState','MeetTown'])
            joinColsLeft = ['MeetCountry']
            countries,deleteCols = readAndPop (MeetCountry,joinColsLeft)
            meetLocations = meetLocations.merge(countries, left_on = joinColsLeft, right_on = ['name'],suffixes = [None,"country_"])
            meetLocations = meetLocations.drop(deleteCols, axis = 1)
            renamer =  {k : k[4:].lower() for k in list(meetLocations) if k[:4] == 'Meet'}
            renamer["id"] = "country_id"
            meetLocations = meetLocations.rename (columns = renamer)
            with db.Session() as session:
                createData (ObjectNameDestination = MeetLocation
                           ,dataObject            = meetLocations
                           ,session               = session
                           ,colMappings           = None
                           ,commit                = True
                           )

            joinColsLeft = ['MeetCountry','MeetState','MeetTown']
            meetLocations,deleteCols = readAndPop (MeetLocationsView,joinColsLeft)
            df = df.merge(meetLocations, left_on = joinColsLeft, right_on = ['country','state','town'])
            df = df.rename (columns = {'id': 'location_id'})
            df = df.drop(deleteCols, axis = 1)

            with db.Session() as session:
                createData (ObjectNameDestination = Result
                           ,dataObject            = df
                           ,session               = session
                           ,colMappings           = {"Name": "name"
                                                    ,"Age": "age"
                                                    ,"AgeClass": "age_class"
                                                    ,"BirthYearClass": "birth_year_class"
                                                    ,"Bodyweight": "body_weight "
                                                    ,"Squat1": "squat_1"
                                                    ,"Squat2": "squat_2"
                                                    ,"Squat3": "squat_3"
                                                    ,"Squat4": "squat_4"
                                                    ,"Best3Squat": "squat_best_3"
                                                    ,"Bench1": "bench_1"
                                                    ,"Bench2": "bench_2"
                                                    ,"Bench3": "bench_3"
                                                    ,"Bench4": "bench_4"
                                                    ,"Best3Bench": "bench_best_3"
                                                    ,"Deadlift1": "deadlift_1"
                                                    ,"Deadlift2": "deadlift_2"
                                                    ,"Deadlift3": "deadlift_3"
                                                    ,"Deadlift4": "deadlift_4"
                                                    ,"Best3Deadlift": "deadlift_best_3"
                                                    ,"Total": "total"
                                                    ,"Place": "place"
                                                    ,"Dots": "dots"
                                                    ,"Wilks": "wilks"
                                                    ,"Glossbrenner": "glossbrenner"
                                                    ,"Goodlift": "goodlift"
                                                    ,"Tested": "tested"
                                                    ,"Country": "country"
                                                    ,"State": "state"
                                                    ,"Date": "date"
                                                    ,"MeetName": "meet_name"
                                                    }
                           ,commit                = True
                           )

        LoadBase.metadata.drop_all(db.engine)

    finally:
        if db:
            db.dispose()


def main():
    """
        Main entry point of program
    """
    thisGUID = getTheLatestData()
    loadTheLatestData(thisGUID)

if __name__ == "__main__":
    main()