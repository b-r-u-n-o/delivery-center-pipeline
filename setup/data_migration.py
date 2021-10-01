#!/usr/bin/env python3
import pandas as pd
import os
import logging
import dotenv
from conect.engineSQL import Conections

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def createEngineDB():
    """
    Cria a engine de conexão com a base PostgreSQL

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


def createEngineDW():
    """
    Cria a engine de conexão com o Data Warehouse
    """

    # utilizando o dotenv para proteger as variáveis de ambiente
    path_env = os.path.join(os.path.dirname(__file__), ".env")
    dotenv.load_dotenv(path_env)

    USER = os.getenv(key="POSTGRES_DW_USER")
    SECRET = os.getenv(key="POSTGRES_DW_PASSWORD")
    HOST = os.getenv(key="POSTGRES_DW_HOST")
    DB = os.getenv(key="POSTGRES_DW_NAME")

    con = Conections()

    engine = con.postgreSQL(user=USER, secret=SECRET, host=HOST, db=DB, port=5433)

    return engine


def extractData(tbla: str):
    """
    Extrai os dados da tabela especificada,
    como um dataframe Pandas
    """

    engine = createEngineDB()
    with engine.begin() as connection:
        df = pd.read_sql_table(table_name=tbla, con=connection, schema="public")

    return df


def loadDataDW(name: str, df: pd.DataFrame) -> None:
    """
    Conecta a base de dados e realiza a ingestão dos
    dados no Data Warehouse
    """
    engine = createEngineDW()

    with engine.begin() as connection:
        df.to_sql(
            name=name, con=connection, if_exists="replace", chunksize=200, index=False
        )


if __name__ == "__main__":

    nameFiles = [
        "channels",
        "deliveries",
        "drivers",
        "hubs",
        "orders",
        "payments",
        "stores",
    ]

    for i in nameFiles:
        tabela = extractData(tbla=i)
        logger.info(f"[INFO]: carregada com sucesso a tabela {i}")

        try:

            tabela.to_csv(
                f"/home/developer/Code/\
                    datalab-work-at-deliverycenter/Lake/raw/LOAD_{i}.csv",
                sep=";",
                index=False,
                index_label=False,
            )
            logger.info(f"Arquivo {i} salvo com sucesso na camada raw")

        except OSError as e:
            print(e)

        else:
            parent_dir = "/home/developer/Code/delivey-center-fluxo/datalake"
            directory = "raw"
            path = os.path.join(parent_dir, directory)
            logger.info(
                f"Diretório {directory} criado com sucesso no path {parent_dir}"
            )

            tabela.to_csv(
                f"/home/developer/Code/delivey-center-fluxo/datalake/raw/LOAD_{i}.csv",
                sep=";",
                index=False,
                index_label=False,
            )
            logger.info(f"Arquivo {i} salvo com sucesso na camada raw")

        loadDataDW(name=i, df=tabela)
        logger.info(
            f"Replicação da tabela executada com sucesso {i} para o Data Warehouse."
        )
        logger.info(f"Tabela carregada {tabela.head()}.")
