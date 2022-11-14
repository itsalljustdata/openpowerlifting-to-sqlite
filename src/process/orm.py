
#!/usr/bin/env python3

from sqlalchemy_utils import create_view
from sqlalchemy import Column, Float, Integer, String, DateTime, Date, ForeignKey, Identity, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import UniqueConstraint

ECHO : bool = False

CommitBase        = declarative_base()
class dataCommit(CommitBase):
    __tablename__ = "dataCommit"
    id = Column(Integer, Identity(start=1, cycle=True), primary_key=True,nullable=False)
    commitGUID = Column(String(40), unique=True,nullable=False)
    fullURL = Column(String(130),nullable=False)
    firstSeen = Column(DateTime, nullable=False)
    lastSeen = Column(DateTime, nullable=False)
    downloaded = Column(DateTime, nullable=True)
    fileSize = Column(Integer, nullable=True)

    def __init__ (self, commitGUID, fullURL, now):
        self.commitGUID = commitGUID
        self.fullURL = fullURL
        self.firstSeen = now
        self.lastSeen = now
        self.downloaded = None
        self.fileSize = None

Base        = declarative_base()
class Event(Base):
    __tablename__ = "event"
    id = Column(Integer, Identity(start=1, cycle=True), primary_key=True,nullable=False)
    name = Column(String(5), unique=True,nullable=False)
    resultCnt = Column(Integer, nullable=False)

    # def __init__ (self, name, resultCnt):
    #     self.name = name
    #     self.resultCnt = resultCnt

class Division(Base):
    __tablename__ = "Division"
    id = Column(Integer, Identity(start=1, cycle=True), primary_key=True,nullable=False)
    name = Column(String(5), unique=True,nullable=False)
    resultCnt = Column(Integer, nullable=False)

    # def __init__ (self, name, resultCnt):
    #     self.name = name
    #     self.resultCnt = resultCnt

class Equipment(Base):
    __tablename__ = "Equipment"
    id = Column(Integer, Identity(start=1, cycle=True), primary_key=True,nullable=False)
    name = Column(String(5), unique=True,nullable=False)
    resultCnt = Column(Integer, nullable=False)

    # def __init__ (self, name, resultCnt):
    #     self.name = name
    #     self.resultCnt = resultCnt

class SexWeight(Base):
    __tablename__ = "WeightClass"
    id = Column(Integer, Identity(start=1, cycle=True), primary_key=True,nullable=False)
    sex = Column(String(3), nullable=False)
    weight_class = Column(String(30), nullable=False)
    resultCnt = Column(Integer, nullable=False)
    __table_args__ = (UniqueConstraint(sex,weight_class, name=f'{__tablename__}_uk'),{})
    # def __init__ (self, sex, weight_class, resultCnt):
    #     self.sex = sex
    #     self.weight_class = weight_class
    #     self.resultCnt = resultCnt

LoadBase        = declarative_base()
class FederationLoad(LoadBase):
    __tablename__ = "_federationLoad"
    id = Column(Integer, Identity(start=1, cycle=True), primary_key=True,nullable=False)
    name = Column(String(30), nullable=False)
    parent_name = Column(String(30), nullable=True)
    parent_id = Column(Integer, nullable = True)
    resultCnt = Column(Integer, nullable=False)
    __table_args__ = (UniqueConstraint(name,parent_name, name='_federation_load_uk'),{})

    # def __init__ (self, name, parent_name, resultCnt):
    #     self.name = name
    #     self.parent_name = parent_name
    #     if self.parent_name and self.parent_name == self.name:
    #         self.parent_name = None
    #     self.resultCnt = resultCnt


class Federation(Base):
    __tablename__ = "federation"
    id = Column(Integer, primary_key=True,nullable=False)
    name = Column(String(30), nullable=False)
    parent_id = Column(Integer, ForeignKey(column = "federation.id", name = "federation_parent_fk"), nullable = True)
    resultCnt = Column(Integer, nullable=False)
    __table_args__ = (UniqueConstraint(name,parent_id, name='_federation_uk'),{})



class MeetCountry(Base):
    __tablename__ = "meet_country"
    id = Column(Integer, Identity(start=1, cycle=True), primary_key=True,nullable=False)
    name = Column(String(132), unique=True,nullable=False)
    resultCnt = Column(Integer, nullable=False)

    # def __init__ (self, name, resultCnt):
    #     self.name = name
    #     self.resultCnt = resultCnt

class MeetLocation(Base):
    __tablename__ = "meet_location"
    id = Column(Integer, Identity(start=1, cycle=True), primary_key=True,nullable=False)
    country_id = Column(Integer,  ForeignKey(column =f"{MeetCountry.__tablename__}.id", name = f"{MeetCountry.__tablename__}_fk"), nullable = False)
    state = Column(String(132),nullable=False)
    town = Column(String(132), nullable=False)
    resultCnt = Column(Integer, nullable=False)
    __table_args__ = (UniqueConstraint(country_id,state,town, name='meet_location_uk'),{})
    # def __init__ (self, country_id, state, town, resultCnt):
    #     self.country_id = country_id
    #     self.state = state
    #     self.town = town
    #     self.resultCnt = resultCnt


class Result(Base):
    __tablename__ = "result"
    id = Column(Integer, Identity(start=1, cycle=True), primary_key=True,nullable=False)
    name = Column(String(60), nullable=False)
    age = Column(Float)
    age_class = Column(String(12))
    birth_year_class = Column(String(12))
    body_weight  = Column(Float)
    squat_1 = Column(Float)
    squat_2 = Column(Float)
    squat_3 = Column(Float)
    squat_4 = Column(Float)
    squat_best_3 = Column(Float)
    bench_1 = Column(Float)
    bench_2 = Column(Float)
    bench_3 = Column(Float)
    bench_4 = Column(Float)
    bench_best_3 = Column(Float)
    deadlift_1 = Column(Float)
    deadlift_2 = Column(Float)
    deadlift_3 = Column(Float)
    deadlift_4 = Column(Float)
    deadlift_best_3 = Column(Float)
    total = Column(Float)
    place = Column(Float)
    dots = Column(Float)
    wilks = Column(Float)
    glossbrenner = Column(Float)
    goodlift = Column(Float)
    tested = Column(String(5))
    country = Column(String(30))
    state = Column(String(30))
    date = Column(Date)
    meet_name = Column(String(500))
    federation_id = Column(Integer,ForeignKey(column = f"{Federation.__tablename__}.id", name =f"{Federation.__tablename__}_fk"), nullable=False)
    division_id = Column(Integer,ForeignKey(column = f"{Division.__tablename__}.id", name = f"{Division.__tablename__}_fk"), nullable=False)
    equipment_id = Column(Integer,ForeignKey(column = f"{Equipment.__tablename__}.id", name = f"{Equipment.__tablename__}_fk"), nullable=False)
    event_id = Column(Integer,ForeignKey(column = f"{Event.__tablename__}.id", name = f"{Event.__tablename__}_fk"), nullable=False)
    weightclass_id = Column(Integer,ForeignKey(column = f"{SexWeight.__tablename__}.id", name = f"{SexWeight.__tablename__}_fk"), nullable=False)
    location_id = Column(Integer,ForeignKey(column = f"{MeetLocation.__tablename__}.id", name = f"{MeetLocation.__tablename__}_fk"), nullable=False)

parent_federation_view = select([
    Federation.id,
    Federation.name
]).select_from(Federation).where (Federation.parent_id.is_(None))
view = create_view(name = 'v_parent_federation', selectable = parent_federation_view, metadata = Base.metadata, cascade_on_drop = False)
class ParentFederation(Base):
    __table__ = view
    __tablename__ = __table__.name

parent_federation_load_view = select([
    FederationLoad.id,
    FederationLoad.name
]).select_from(FederationLoad).where (FederationLoad.parent_name.is_(None))
viewLoad = create_view(name = '_parent_federationLoad', selectable = parent_federation_load_view, metadata = Base.metadata, cascade_on_drop = False)
class ParentFederationLoad(LoadBase):
    __table__ = viewLoad
    __tablename__ = __table__.name


federation_view = select([
    Federation.id,
    Federation.name,
    ParentFederation.name.label("parent_name"),
]).select_from(Federation).outerjoin(ParentFederation, Federation.parent_id == ParentFederation.id)
fedView = create_view(name = 'v_federation', selectable = federation_view, metadata = Base.metadata, cascade_on_drop = False)
class FederationView(Base):
    __table__ = fedView
    __tablename__ = __table__.name

meet_location_view = select([
    MeetLocation.id,
    MeetCountry.id.label("country_id"),
    MeetCountry.name.label("country"),
    MeetLocation.state,
    MeetLocation.town
]).select_from(MeetLocation).join(MeetCountry)
viewMeetLocn = create_view(name = 'v_meet_locations', selectable = meet_location_view, metadata = Base.metadata, cascade_on_drop = False)
# provides an ORM interface to the view
class MeetLocationsView(Base):
    __table__ = viewMeetLocn
    __tablename__ = __table__.name

resultsView = select([
        Result.name.label("Name"),
        SexWeight.sex.label("Sex"),
        Event.name.label("Event"),
        Equipment.name.label("Equipment"),
        Result.age.label("Age"),
        Result.age_class.label("AgeClass"),
        Result.birth_year_class.label("BirthYearClass"),
        Division.name.label("Division"),
        Result.body_weight .label("Bodyweight"),
        SexWeight.weight_class.label("WeightClass"),
        Result.squat_1.label("Squat1"),
        Result.squat_2.label("Squat2"),
        Result.squat_3.label("Squat3"),
        Result.squat_4.label("Squat4"),
        Result.squat_best_3.label("Best3Squat"),
        Result.bench_1.label("Bench1"),
        Result.bench_2.label("Bench2"),
        Result.bench_3.label("Bench3"),
        Result.bench_4.label("Bench4"),
        Result.bench_best_3.label("Best3Bench"),
        Result.deadlift_1.label("Deadlift1"),
        Result.deadlift_2.label("Deadlift2"),
        Result.deadlift_3.label("Deadlift3"),
        Result.deadlift_4.label("Deadlift4"),
        Result.deadlift_best_3.label("Best3Deadlift"),
        Result.total.label("Total"),
        Result.place.label("Place"),
        Result.dots.label("Dots"),
        Result.wilks.label("Wilks"),
        Result.glossbrenner.label("Glossbrenner"),
        Result.goodlift.label("Goodlift"),
        Result.tested.label("Tested"),
        Result.country.label("Country"),
        Result.state.label("State"),
        FederationView.name.label("Federation"),
        FederationView.parent_name.label("ParentFederation"),
        Result.date.label("Date"),
        MeetLocationsView.country.label("MeetCountry"),
        MeetLocationsView.state.label("MeetState"),
        MeetLocationsView.town.label("MeetTown"),
        Result.meet_name.label("MeetName")
]).select_from(Result)  \
    .join(Equipment)  \
    .join(SexWeight)  \
    .join(Division)  \
    .join(Event)  \
    .join(FederationView, FederationView.id == Result.federation_id) \
    .join(MeetLocationsView, Result.location_id == MeetLocationsView.id)  \

viewResults = create_view(name = 'v_results', selectable = resultsView, metadata = Base.metadata, cascade_on_drop = False)
# provides an ORM interface to the view
class ResultView(Base):
    __table__ = viewResults
    __tablename__ = __table__.name
