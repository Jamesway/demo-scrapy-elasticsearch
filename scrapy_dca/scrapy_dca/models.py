from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from scrapy.utils.project import get_project_settings

DeclarativeBase = declarative_base()

def db_connect():
    """
    Performs database connection using database settings from settings.py.
    Returns sqlalchemy engine instance
    """
    return create_engine(get_project_settings().get("CONNECTION_STRING"))

def create_table(engine):
    DeclarativeBase.metadata.create_all(engine)

class PracticeDB(DeclarativeBase):
    __tablename__ = 'practice_table'

    # id = Column(Integer, primary_key=True)
    # doi = Column('doi', String(32), index=true, unique=true)
    # journal = Column('journal', String(32))
    # title = Column('title', Text)
    # authors = Column('authors', Text)
    # published_at = Column('published_at', DateTime, index=true)
    # scraped_at = Column('scraped_at', DateTime)

    id = Column(String(36), primary_key = True)
    license = Column('license', String(32), index = True, unique = True)
    physician_name = Column('name', String(60))
    license_type = Column('license_type', String(32))
    address = Column('address', Text)
    #practice_location = Column('practice_location', Text)
    #services = Column('services', Text)
    base_price = Column('base_price', Integer)
    enabled = Column('enabled', Boolean, index = True, default = True)
    scraped_at = Column('scraped_at', DateTime)



class AppointmentDB(DeclarativeBase):
    __tablename__ = 'appointment_table'

    id = Column(String(36), primary_key = True)
    practice_id = Column('practice_id', String(36), index = True)
    appointment_time = Column('appointment_time', DateTime, index = True)

#
# class SpecialtiesDB(DeclarativeBase):
#     __tablename__ = 'specialties_table'
#
#     id = Column(Integer, PrimaryKey = True)
#     specialty = Column('specialty', String(32), index = True)
#     practice_id = Column('practice_id', String(36), index = True)

# timestamp = Field()  # scrape time
# source = Field()  # eg CA medical board
# name = Field()
# prev_name = Field()
# license = Field()
# license_type = Field()
# issue_date = Field()
# exp_date = Field()
# status1 = Field()
# status2 = Field()
# school = Field()
# graduation = Field()
# address = Field()
#
# # THE FOLLOWING INFORMATION IS SELF-REPORTED BY THE LICENSEE AND HAS NOT BEEN VERIFIED BY THE BOARD
# practice_location = Field()
# services = Field()
# certifications = Field()
# ethnicity = Field()
# language = Field()
# gender = Field()