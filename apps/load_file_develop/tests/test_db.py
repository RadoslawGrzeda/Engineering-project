import sys 
import os 
import pytest
import datetime
from pydantic import ValidationError
import sqlalchemy as sa
from dotenv import load_dotenv
load_dotenv()

def init_db():
    connection_string=os.getenv("postgress_connection")
    engine=sa.create_engine(connection_string)
    return engine


def test_db_connection():
    engine=init_db()
    assert engine.connect()

def test_tables_exist():
    engine=init_db()
    inspector=sa.inspect(engine)
    tables=inspector.get_table_names()
    assert 'sector' in tables
    assert 'department' in tables
    assert 'segment' in tables
    assert 'chief' in tables
    assert 'pos_information' in tables
    assert 'contractor' in tables
    assert 'product' in tables