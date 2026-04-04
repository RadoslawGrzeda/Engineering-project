import os
from sqlalchemy.orm import sessionmaker

import sqlalchemy
from dotenv import load_dotenv
load_dotenv()
import os

SQL_ALCHEMY_DATABASE_URL = os.getenv('POSTGRES_CONNECTION')

engine = sqlalchemy.create_engine(SQL_ALCHEMY_DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
