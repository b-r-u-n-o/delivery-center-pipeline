
from sqlalchemy import create_engine

class Conections():
    
    def __init__(self) -> None:
        pass
        

    def postgreSQL(self, user:str, secret:str, host:str, db:str, port:str = '5432') -> str:

        """
        Cria uma engine para acesso a instância Postgres

            possuí dependência com o psycopg2-binary

        """

        dialect = 'postgresql'
        driver = 'psycopg2'
        username = user
        password = secret
        host = host
        port = port
        database = db

        return create_engine(f"{dialect}+{driver}://{username}:{password}@{host}:{port}/{database}")
