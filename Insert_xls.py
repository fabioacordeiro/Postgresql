# pip install pandas 
# openpyxl 
# psycopg2 
# sqlalchemy

import pandas as pd
import psycopg2
from psycopg2 import sql

# Configuração da conexão com o PostgreSQL
class PostgreSQLConnection:
    def __init__(self, user, password, host, port, database):
        self.connection = None
        self.cursor = None
        try:
            # Forçando a codificação correta durante a conexão
            self.connection = psycopg2.connect(
                user=user,
                password=password,
                host=host,
                port=port,
                database=database,
                options="-c client_encoding=UTF8"
            )
            self.cursor = self.connection.cursor()
            print("Conexão com o PostgreSQL realizada com sucesso!")
        except Exception as e:
            print("Erro ao conectar ao PostgreSQL:", e)

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Conexão com o PostgreSQL encerrada.")

# Função para inserir os dados no PostgreSQL
def insert_data(cursor, data):
    insert_query = sql.SQL("""
        INSERT INTO carrefour.transp (
            cod_tr, nome_transp, nome_fantasia, responsavel, tel_fixo, 
            e_mail_envio, e_mail, ativa, analista, ativar, uf, parceiro, 
            endereco, municipio, cnpj, tipo_carga, vuc_plataforma, vuc, 
            leve_seco, leve_refrigerado, toco_seco, toco_seco_plt, 
            toco_refrigerado, truck_seco, truck_refrigerado, carreta_seca, 
            carreta_seca_plt, carreta_refrigerado, ac, al, am, ap, ba, ce, df, 
            es, goias, ma, mg, ms, mt, pa, pb, pe, pi, pr, rj, rn, ro, rr, rs, sc, 
            se, sp, tocantis, data_cadastro, user_cadastro
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """)
    
    for _, row in data.iterrows():
        cursor.execute(insert_query, tuple(row))

# Função principal para ler o arquivo Excel e inserir os dados no PostgreSQL
def main():
    # Configurações do banco de dados
    db_config = {
        "user": "postgres",
        "password": "Lucas",
        "host": "localhost",
        "port": "5432",
        "database": "carrefour"
    }

    # Caminho do arquivo Excel
    excel_file = "C://Fabio//Desenvolvimento//Postgresql//Transp.xlsx"

    # Ler o arquivo Excel usando pandas
    try:
        df = pd.read_excel(excel_file, engine='openpyxl')
        print("Arquivo Excel lido com sucesso!")
    except Exception as e:
        print("Erro ao ler o arquivo Excel:", e)
        return

    # Conectar ao PostgreSQL e inserir os dados
    connection = PostgreSQLConnection(**db_config)
    
    try:
        insert_data(connection.cursor, df)
        connection.connection.commit()
        print("Dados inseridos com sucesso no PostgreSQL!")
    except Exception as e:
        print("Erro ao inserir dados no PostgreSQL:", e)
    finally:
        connection.close()

if __name__ == "__main__":
    main()