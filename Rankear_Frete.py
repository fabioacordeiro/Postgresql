import pandas as pd

# Caminho do arquivo Excel
file_path = 'C:\\Fabio\\CARREFOUR\\CONTRATACAO\\RANKING_FRETE.xlsx'

# Nome da planilha específica que você deseja importar
sheet_name = 'tabela_frete'

# Carregar a planilha específica em um DataFrame
df = pd.read_excel(file_path, sheet_name=sheet_name)

df1 = df.sort_values(by=['CHAVE', 'FRETE'], ascending=[True, True])

# Criar novas colunas com valores deslocados
df1['CHAVE_ANTERIOR'] = df1['CHAVE'].shift(1)
df1['FRETE_ANTERIOR'] = df1['FRETE'].shift(1)

cont = 1
df1['RANKING'] = 1

# Iterar sobre o DataFrame e atualizar a coluna 'RANKING'
for index in range(1, len(df1)):
    if df1.loc[index, 'CHAVE'] == df1.loc[index-1, 'CHAVE']:
        if df1.loc[index, 'FRETE'] == df1.loc[index-1, 'FRETE']:
            df1.loc[index, 'RANKING'] = cont
        else:
            cont += 1
            df1.loc[index, 'RANKING'] = cont
    else:
        cont = 1
        df1.loc[index, 'RANKING'] = cont

# Salvar o DataFrame atualizado em um novo arquivo Excel
df1.to_excel('ranking_python.xlsx', index=False)