# pip install sqlalchemy
# pip install psycopg2
import csv
import datetime
import time
from pathlib import Path

import pandas as pd
import psycopg2
# import sqlalchemy
from sqlalchemy import create_engine

agora = datetime.datetime.now()
agora_string = agora.strftime("%A %d %B %y %I:%M")
agora_datetime = datetime.datetime.strptime(agora_string, "%A %d %B %y %I:%M")
hora = datetime.date.today()
inicio = time.time()
print(f'Time Início: {agora_datetime}')
# time.sleep(50)

try:
    print(f'{"Step 1: Reading file CSV":.^60}')
    # abrindo o arquivo CSV e mostrando 5 linhas.
    # skiprows => informar quais as linhas que serão ignoradas
    # nrows => qual a quantidade de linhas que serão percorridas
    # usecols => qual as colunas que serão utilizadas
    BD1 = pd.read_csv(filepath_or_buffer='C:/Fabio/Python/Carrefour/CTE.csv',
                      sep='|', nrows=5, index_col=None, skiprows=[1, 2, 3], low_memory=False, usecols=[
                          1, 2, 3])

    coluna = {
        "emissao   ": "dt_emissao"}

    BD2 = BD1.rename(columns=coluna)
    # BD.to_excel('Novo_BD.xlsx')
    # BD2.to_csv('Pequeno_BD.csv')
    print('Readying file completed')
    print(f'{"Visualizando com head()":.^60}')
    print(BD2.head())
    print(f'{"Visualização com describe()":.^60}')
    print(BD2.describe())
    print(f'{"Visualização com info()":.^60}')
    print(BD2.info())

except Exception as error:
    print(error.__class__.__name__)
    print(error.args)
    print(error)

finally:
    print('Finished reading process ....')

##################################################

##################################################
print(f'{"Step 2: Insert data entry":.^60}')

try:

    hostname = 'localhost'
    database = 'Carrefour'
    username = 'postgres'
    pwd = '1234'
    port_id = 5432

    connection = psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id
    )

    try:
        cursor = connection.cursor()
        print(f'{"Step 5: Iniciando o looping":.^60}')
        for index, row in BD2.iterrows():
            _sql = (
                "INSERT INTO cte_teste_4(cte, peso, dt_emissao) VALUES (%s, %s, %s)")
            valor = (row["cte"], row["peso"], row["dt_emissao"])
            cursor.execute(_sql, valor)
            connection.commit()
        cursor.close()
        connection.close()
    except Exception as error:
        print(error.__class__.__name__)
        print(error.args)
        print(error)

    finally:
        print('Process creation finished')

    time.sleep(50)
    cursor.close()
    connection.close()
    TABLE_NAME = 'CTE_Teste_4'
    # DB_NAME = 'PostgreSQL'
    # ROOT_DIR = Path(__file__).parent
    # BD_FILE = ROOT_DIR / DB_NAME

    # BD.to_excel('Novo_BD.xlsx')
    print('Readying file completed')
# Inserindo os dados
    BD2.to_sql(
        name='cte_teste_4',
        con=connection,
        if_exists='replace',
        index=False,
        dtype={
            'cte': 'Text',
            'peso': 'Text',
            'dt_emissao':  'Text',
        }
    )
    # pd.ready_sql(valor, connection)
    connection.commit()

    print(f'Data entry completed in :{TABLE_NAME}')

except Exception as error:
    print(error.__class__.__name__)
    print(error.args)
    print(error)

finally:
    print('Process creation finished')


fim = time.time()
print(
    f'Tempo de processamento: {int(fim-inicio)} segundos {int((fim-inicio)/60)} Minutos')
'''
    Carrefour.insert(10089469009655, 0.12, '2021-06-01', 'ES', 'ES', '2038', 'FL VITORIA', 'CO VIANA 01', '29130055', 'CC', 'R', 90.9, 0, '0.0', '0.0', '0.0',
                     0, 0, 0, 0, 0, 36.57, 0, '0.0', '0.0', 0, 36.57, '23173', '20121850003502', '4884082000569', '.com', 'mercado envios servicos de logistica ltda')
'''
