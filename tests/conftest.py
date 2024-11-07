import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from cmti_tools.tables import Base

@pytest.fixture(scope="session")
def engine():
    return create_engine('sqlite:///:memory:', echo = True)

@pytest.fixture(scope="session")
def tables(engine):
    Base.metadata.create_all(engine)
    yield
    Base.metadata.drop_all(engine)

@pytest.fixture
def session(engine, tables):
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()