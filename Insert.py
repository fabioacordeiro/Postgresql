import csv
import time
import chardet
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


class Config:
    def __init__(self):
        self.config = {
            "postgres": {
                "user": "postgres",
                "password": "Lucas",
                "host": "localhost",
                "port": "5432",
                "database": "carrefour"
            }
        }


class Connection(Config):
    def __init__(self):
        super().__init__()
        try:
            self.conn = psycopg2.connect(**self.config["postgres"])
            self.cur = self.conn.cursor()
        except Exception as e:
            print('Erro na conexão:', e)
            exit(1)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self.conn.close()

    def commit(self):
        self.conn.commit()

    def execute(self, sql, params=None):
        self.cur.execute(sql, params or ())

    def fetchall(self):
        return self.cur.fetchall()


class Person(Connection):
    def insert(self, *args):
        try:
            sql = """
            INSERT INTO carrefour.transp (cod_tr, nome_transp, nome_fantasia, responsavel, tel_fixo, e_mail_envio, e_mail, ativa, analista, ativar, uf, parceiro, endereco, municipio, cnpj, tipo_carga, vuc_plataforma, vuc, leve_seco, leve_refrigerado, toco_seco, toco_seco_plt, toco_refrigerado, truck_seco, truck_refrigerado, carreta_seca, carreta_seca_plt, carreta_refrigerado, ac, al, am, ap, ba, ce, df, es, goias, ma, mg, ms, mt, pa, pb, pe, pi, pr, rj, rn, ro, rr, rs, sc, se, sp, tocantis, data_cadastro, user_cadastro) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            self.execute(sql, args)
            self.commit()
            print('Dados inseridos com sucesso!')
        except Exception as e:
            print('Erro ao inserir dados:', e)

    def insert_csv(self, filename):
        try:
            with open(filename, 'r', encoding='UTF-8', errors='ignore') as file:
                data = csv.DictReader(file)
                for row in data:
                    self.insert(
                        row['cod_tr'], row['nome_transp'], row['nome_fantasia'], row['responsavel'], 
                        row['tel_fixo'], row['e_mail_envio'], row['e_mail'], row['ativa'], 
                        row['analista'], row['ativar'], row['uf'], row['parceiro'], 
                        row['endereco'], row['municipio'], row['cnpj'], row['tipo_carga'], 
                        row['vuc_plataforma'], row['vuc'], row['leve_seco'], row['leve_refrigerado'], 
                        row['toco_seco'], row['toco_seco_plt'], row['toco_refrigerado'], row['truck_seco'], 
                        row['truck_refrigerado'], row['carreta_seca'], row['carreta_seca_plt'], 
                        row['carreta_refrigerado'], row['ac'], row['al'], row['am'], row['ap'], 
                        row['ba'], row['ce'], row['df'], row['es'], row['goias'], row['ma'], 
                        row['mg'], row['ms'], row['mt'], row['pa'], row['pb'], row['pe'], row['pi'], 
                        row['pr'], row['rj'], row['rn'], row['ro'], row['rr'], row['rs'], row['sc'], 
                        row['se'], row['sp'], row['tocantis'], row['data_cadastro'], row['user_cadastro']
                    )
        except Exception as e:
            print('Erro ao inserir arquivo CSV:', e)


def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    print("Encoding detectado:", result['encoding'])
    return result['encoding']


def process_csv(file_path):
    try:
        print(f'{"Step 1: Reading CSV File":.^60}')
        encoding = detect_encoding(file_path)
        data = pd.read_csv(
            file_path,
            sep=';', nrows=1, index_col=None, low_memory=False,
            encoding=encoding,
            usecols=range(1, 59)
        )
        coluna = {
            "go": "goias",
            "to": "tocantis"
        }
        data = data.rename(columns=coluna)
        data.to_csv('Transp_Novo.csv', index=False)
        print('Reading file completed')
    except Exception as error:
        print(f"Erro ao processar CSV: {error}")
    finally:
        print('Finished reading process')


if __name__ == "__main__":
    start_time = time.time()

    process_csv('C://Fabio//Desenvolvimento//Postgresql//Transp.csv')

    carrefour = Person()
    carrefour.insert_csv('Transp_Novo.csv')

    end_time = time.time()
    print(f'Tempo de processamento: {int(end_time - start_time)} segundos')