from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy_utils import database_exists, create_database


BASE = declarative_base()

SESSION = None


def initialize_global_database(user_name, user_password, host, port, db_name):
    global SESSION

    if SESSION:
        return

    engine = create_engine(
        f"postgresql+psycopg2://{user_name}:{user_password}@{host}:{port}/{db_name}",
        echo=False,
    )

    SESSION = sessionmaker(bind=engine)

    if not database_exists(engine.url):
        create_database(engine.url)
        print("db is created")

    import history.__all_models

    BASE.metadata.create_all(engine)


def get_session() -> Session:
    global SESSION
    return SESSION()
