# pip install psycopg2
import csv
import datetime
import time
from pathlib import Path

import pandas as pd
import psycopg2

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
                      sep='|', index_col=None, skiprows=[1, 2, 3], low_memory=False, usecols=[
                          1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                          17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
                          31, 32])

    coluna = {
        "emissao   ": "dt_emissao",
        "taxa_extra_central": "tx_entrega_central",
        "taxa_extra_destino": "tx_entrega_destino",
        "taxa_extra_orgem": "tx_entrega_origem"}

    BD2 = BD1.rename(columns=coluna)
    print(f'{"Step 2: Dataframe created":.^60}')
    # BD.to_excel('Novo_BD.xlsx')
    # BD2.to_csv('Teste_Grande_BD.csv')
    # print('Readying file completed')
    print(f'{"Step 3: Show size Dataframe":.^60}')
    print(f'Linhas do BD2:{BD2[BD2.columns[0]].count()}')
    # print(BD2.head())
    # print(f'{"Visualização com describe()":.^60}')
    # print(BD2.describe())
    # print(f'{"Visualização com info()":.^60}')
    # print(BD2.info())

except Exception as error:
    print(error.__class__.__name__)
    print(error.args)
    print(error)

finally:
    print('Finished reading process ....')

##################################################

print(f'{"Step 4: Create connect from database":.^60}')

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
        print(f'{"Step 5: Starting looping of INSERT":.^60}')
        for index, row in BD2.iterrows():
            _sql = ("""INSERT INTO cte_teste_4(cte, peso, dt_emissao, uf_destino, uf_origem, cidade_destino, unidade_origem, unidade_destino, cep, tarifa, opcao_entrega, valor_declarado, ad_valorem, coleta, embalagem, entrega, icms, tx_entrega_central, tx_entrega_destino, tx_entrega_origem, fob, frete, gris, interior, reversa, tde, faturamento, conta_corrente, cnpj_tomador, cnpj_remetente, produto, razao_social) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""")
            valor = (row['cte'], row['peso'], row['dt_emissao'], row['uf_destino'], row['uf_origem'], row['cidade_destino'],
                     row['unidade_origem'], row['unidade_destino'], row['cep'], row['tarifa'], row['opcao_entrega'],
                     row['valor_declarado'], row['ad_valorem'], row['coleta'], row['embalagem'], row['entrega'],
                     row['icms'], row['tx_entrega_central'], row['tx_entrega_destino'], row['tx_entrega_origem'],
                     row['fob'], row['frete'], row['gris'], row['interior'], row['reversa'], row['tde'],
                     row['faturamento'], row['conta_corrente'], row['cnpj_tomador'], row['cnpj_remetente'],
                     row['produto'], row['razao_social'])
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

    time.sleep(1)
    TABLE_NAME = 'CTE_Teste_4'
    # DB_NAME = 'PostgreSQL'
    # ROOT_DIR = Path(__file__).parent
    # BD_FILE = ROOT_DIR / DB_NAME

    # BD.to_excel('Novo_BD.xlsx')
    print(f'Data entry completed in :{TABLE_NAME}')

except Exception as error:
    print(error.__class__.__name__)
    print(error.args)
    print(error)

finally:
    print('Process finished')

print(f'{"Step 6: Process finished of INSERT":.^60}')
fim = time.time()
print(
    f'Tempo de processamento: {int(fim-inicio)} segundos {int((fim-inicio)/60)} Minutos')
