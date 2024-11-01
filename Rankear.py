import pandas as pd

# Caminho do arquivo Excel
file_path = 'C:\\Fabio\\CARREFOUR\\CONTRATACAO\\RANKING_FRETE.xlsx'

# Nome da planilha específica que você deseja importar
sheet_name = 'tabela_frete'

# Carregar a planilha específica em um DataFrame
df = pd.read_excel(file_path, sheet_name=sheet_name)

df1 = df.sort_values(by=['CHAVE','FRETE'], ascending=[True, True])

print (df1.dtypes)

cont = 0

# Iterar sobre o DataFrame e atualizar a coluna 'Status'
for index, row in df1.iterrows():
    if index == 0:
        cont = 1
        row['RANKING']=1
        row['CHAVE_ANTERIOR'] = row['CHAVE'].shift(1)
        row['FRETE_ANTERIOR'] = row['FRETE'].shift(1)
    elif index != 0:
        row['CHAVE_ANTERIOR'] = df1.loc[index-1, 'CHAVE']
        row['FRETE_ANTERIOR'] = df1.loc[index-1, 'FRETE']
        if df1.loc[index, 'CHAVE'] == df1.loc[index-1, 'CHAVE'] and df1.loc[index, 'FRETE'] == df1.loc[index-1, 'FRETE']:
            row['RANKING'] = cont
        elif row['CHAVE'] != row['CHAVE_ANTERIOR']:
            cont=1
            row['RANKING']= cont
        elif df1.loc[index, 'CHAVE'] == df1.loc[index-1, 'CHAVE'] and df1.loc[index, 'FRETE'] > df1.loc[index-1, 'FRETE']:
            row['RANKING']= cont+=1
  
  
df1.to_excel('ranking_python.xlsx')      
     

#print (df1.dtypes.value_counts())
#for v, valor in df1.iterrows():
 #   print(v)
    

# Exibir as primeiras linhas do DataFrame
#print(df1.head())