from sqlalchemy import (
    create_engine
)

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import sessionmaker

# from db_module.db_utils import subclasses

Base = declarative_base()


engine_postgres = create_engine("postgresql+psycopg2://yassine:ysn@127.0.0.1:5432/db_kanyon")

engine_sqlite = create_engine('sqlite:////Users/ysn/Projects/kanyon/data/Basefront.db')



def dbsession():
    # print(subclasses(Base))
    Session = sessionmaker(bind=engine_postgres)
    session = Session()
    return session

def dbsession_sqlite():
    # print(subclasses(Base))
    Session = sessionmaker(bind=engine_sqlite)
    session = Session()
    return session

# if __name__ == "__main__":
#     session = dbsession()

# if __name__ == '__main__':
#     main()
