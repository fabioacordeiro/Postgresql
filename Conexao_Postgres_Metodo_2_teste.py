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
                          1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                          17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
                          31, 32])

    coluna = {
        "emissao   ": "dt_emissao",
        "taxa_extra_central": "tx_entrega_central",
        "taxa_extra_destino": "tx_entrega_destino",
        "taxa_extra_orgem": "tx_entrega_origem"}

    BD = BD1.rename(columns=coluna)
    # BD.to_excel('Novo_BD.xlsx')
    # BD.to_csv('Novo_BD.csv')
    print('Readying file completed')
    print(f'{"Visualizando com head()":.^60}')
    print(BD.head())
    print(f'{"Visualização com describe()":.^60}')
    print(BD.describe())
    print(f'{"Visualização com info()":.^60}')
    print(BD.info())

except Exception as error:
    print(error.__class__.__name__)
    print(error.args)
    print(error)

finally:
    print('Finished reading process ....')

##################################################

##################################################
print(f'{"Step 2: Insert data entry":.^60}')
conexao_pg = create_engine('postgresql://postgres:1234@localhost:5432/Carrefour'
                           ).connect()
# engine = sqlalchemy.create_engine(BD)


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

    # cursor = connection.cursor()
    # cursor.execute('select \'Hello World\'')
    # for row in cursor:
    # print(row)

    # cursor.close()
    # connection.close()
    TABLE_NAME = 'CTE_Teste_4'
    # DB_NAME = 'PostgreSQL'
    # ROOT_DIR = Path(__file__).parent
    # BD_FILE = ROOT_DIR / DB_NAME

    # BD.to_excel('Novo_BD.xlsx')
    print('Readying file completed')
# Inserindo os dados
    BD.to_sql(
        name='cte_teste_4',
        con=connection,
        if_exists='replace',
        index=False,
        dtype={
            'cte': 'Text',
            'peso': 'Float',
            'dt_emissao':  'DATE',
            'uf_destino': 'TEXT',
            'uf_origem': 'TEXT',
            'cidade_destino': 'TEXT',
            'unidade_origem': 'TEXT',
            'unidade_destino': 'TEXT',
            'cep': 'TEXT',
            'tarifa': 'TEXT',
            'opcao_entrega': 'TEXT',
            'valor_declarado': 'Float',
            'ad_valorem': 'Float',
            'coleta': 'TEXT',
            'embalagem': 'TEXT',
            'entrega': 'TEXT',
            'icms': 'Float',
            'tx_entrega_central': 'Float',
            'tx_entrega_destino': 'Float',
            'tx_entrega_origem': 'Float',
            'fob': 'Float',
            'frete': 'Float',
            'gris': 'Float',
            'interior': 'TEXT',
            'reversa': 'TEXT',
            'tde': 'Float',
            'faturamento': 'Float',
            'conta_corrente': 'TEXT',
            'cnpj_tomador': 'TEXT',
            'cnpj_remetente': 'TEXT',
            'produto': 'TEXT',
            'razao_social': 'TEXT'

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
