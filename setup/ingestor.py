#!/usr/bin/env python3
import pandas as pd
import os
import dotenv
import logging
from conect.engineSQL import Conections

logger = logging.getLogger(__name__)
logger.setLevel(logging.info)

# TODO: Criar conexão com a base de dados
def createEngineSQL():
    """
    Cria a engine de conexão com o PostgresSQL
    """

    # utilzando o dotenv para proteger as variáveis de ambiente
    path_env = os.path.join(os.path.dirname(__file__), ".env")
    dotenv.load_dotenv(path_env)

    USER = os.getenv(key="POSTGRES_USER")
    SECRET = os.getenv(key="POSTGRES_PASSWORD")
    HOST = os.getenv(key="POSTGRES_HOST")
    DB = os.getenv(key="POSTGRES_DB_NAME")

    con = Conections()
    engine = con.postgreSQL(user=USER, secret=SECRET, host=HOST, db=DB)
    return engine


# TODO: Enviar os dados dos arquivos csv


def filesPath():
    """
    Gera de maneira recursiva os paths para os arquivos especificados

    """

    path = "/home/developer/Code/datalab-work-at-deliverycenter/datasets/"
    nameFiles = [
        "channels",
        "deliveries",
        "drivers",
        "hubs",
        "orders",
        "payments",
        "stores",
    ]
    files = [f"{path}{i}.csv" for i in nameFiles]

    return files, nameFiles


def ingestSQL() -> None:
    """
    Conecta a base de dados e realiza a ingestão dos dados
    presentes nos arquivos csv

    """

    engine = createEngineSQL()
    files, names = filesPath()
    n = 0

    with engine.begin() as connection:

        for f in files:
            df = pd.read_csv(f, delimiter=",", index_col=False, encoding="ISO-8859-1")
            logger.info(f"Ingestão {df.head()} ocorreu com sucesso.")
            df.to_sql(
                name=names[n],
                con=connection,
                if_exists="replace",
                chunksize=200,
                index=False,
            )
            n += 1
            # yield f
            # log


if __name__ == "__main__":
    ingestSQL()
    logger.info(f"Ingestão finalizada com sucesso.")
