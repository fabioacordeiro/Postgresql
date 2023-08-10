# pip install sqlalchemy
# pip install psycopg2

import csv

import pandas as pd
import psycopg2


class Config:
    def __init__(self):
        self.config = {"postgres": {"user": "postgres", "password": "1234",
                                    "host": "localhost", "port": "5432",
                                    "database": "Carrefour"}}


class Connection(Config):
    def __init__(self):
        Config.__init__(self)
        try:
            self.conn = psycopg2.connect(**self.config["postgres"])
            self.cur = self.conn.cursor()
        except Exception as e:
            print('Erro na conexao', e)
            exit(1)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self.connection.close()

    @property
    def connection(self):
        return self.conn

    @property
    def cursor(self):
        return self.cur

    def commit(self):
        self.connection.commit()

    def fetchall(self):
        return self.cursor.fetchall()

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())

    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchall()


class Person(Connection):
    def __init__(self):
        Connection.__init__(self)

    def insert(self, *args):
        try:
            _sql = """INSERT INTO public.cte_teste_2 (cte, peso, dt_emissao, uf_destino, uf_origem, cidade_destino, unidade_origem, unidade_destino, cep, tarifa, opcao_entrega, valor_declarado, ad_valorem, coleta, embalagem, entrega, icms, tx_entrega_central, tx_entrega_destino, tx_entrega_origem, fob, frete, gris, interior, reversa, tde, faturamento, conta_corrente, cnpj_tomador, cnpj_remetente, produto, razao_social) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""  # Onde o name é o nome do campo a ser inserido na tabela
            self.execute(_sql, args)
            self.commit()
            print('Dados inseridos com sucesso !!!')
        except Exception as e:
            print('Erro ao inserir dados ...', e)

    def insert_csv(self, filename):
        try:
            data = csv.DictReader(open(filename, encoding='utf-8'))
            for row in data:
                print(row)
                self.insert(row['cte'], row['peso'], row['dt_emissao'], row['uf_destino'], row['uf_origem'], row['cidade_destino'], row['unidade_origem'], row['unidade_destino'], row['cep'], row['tarifa'], row['opcao_entrega'], row['valor_declarado'], row['ad_valorem'], row['coleta'], row['embalagem'], row['entrega'],
                            row['icms'], row['tx_entrega_central'], row['tx_entrega_destino'], row['tx_entrega_origem'], row['fob'], row['frete'], row['gris'], row['interior'], row['reversa'], row['tde'], row['faturamento'], row['conta_corrente'], row['cnpj_tomador'], row['cnpj_remetente'], row['produto'], row['razao_social'])
        except Exception as e:
            print('Erro ao inserir arquivo csv ...', e)


try:
    print(f'{"Step 1: Reading file CSV":.^60}')
    # abrindo o arquivo CSV e mostrando 5 linhas.
    # skiprows => informar quais as linhas que serão ignoradas
    # nrows => qual a quantidade de linhas que serão percorridas
    # usecols => qual as colunas que serão utilizadas
    BD1 = pd.read_csv(filepath_or_buffer='C:/Fabio/Python/Carrefour/CTE.csv',
                      sep='|', nrows=5, index_col=None, skiprows=[1], low_memory=False, usecols=[
                          1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                          17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
                          31, 32])

    coluna = {
        "emissao   ": "dt_emissao",
        "taxa_extra_central": "tx_entrega_central",
        "taxa_extra_destino": "tx_entrega_destino",
        "taxa_extra_orgem": "tx_entrega_origem"}

    BD = BD1.rename(columns=coluna)
    BD.to_excel('Novo_BD.xlsx')
    # BD.to_csv('Novo_BD.csv')
    print('Readying file completed')

except Exception as error:
    print(error.__class__.__name__)
    print(error.args)
    print(error)

finally:
    print('Finished reading process ....')

if __name__ == "__main__":
    Carrefour = Person()

    Carrefour.insert(10089469009655, 0.12, '2021-06-01', 'ES', 'ES', '2038', 'FL VITORIA', 'CO VIANA 01', '29130055', 'CC', 'R', 90.9, 0, '0.0', '0.0', '0.0',
                     0, 0, 0, 0, 0, 36.57, 0, '0.0', '0.0', 0, 36.57, '23173', '20121850003502', '4884082000569', '.com', 'mercado envios servicos de logistica ltda')

    # Carrefour.insert_csv('Novo_BD.csv')
