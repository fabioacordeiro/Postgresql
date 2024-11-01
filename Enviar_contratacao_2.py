#pip install pandas
# pywhatkit
# openpyxl


import pandas as pd
import pywhatkit as kit
import time
from datetime import datetime
# Lendo a planilha
file_path = 'CONTRATACAO.xlsx'
df = pd.read_excel(file_path)

# Verificando os nomes das colunas
#print(df.columns)
#time.sleep(1)
print('')
#print('----------------------------------------------------------------------------------------------')
#print('  CCCCCCCC       AAA      RRRRRRRR  RRRRRRRR  EEEEEEEE FFFFFFFF OOOOOOOO UU    UU RRRRRRRR    . ')
#print('  CC            AA AA     RR  RRR   RR  RRR   EE       FF       OO    OO UU    UU RR  RRR   ')
#print('  CC           AA   AA    RRRRR     RRRRR     EEEEEEE  FFFFFF   OO    OO UU    UU RRRRR   ')
#print('  CC          AAAAAAAAA   RR   RR   RR   RR   EE       FF       OO    OO UU    UU RR   RR  ')
#print('  CC         AA       AA  RR    RR  RR    RR  EE       FF       OO    OO UU    UU RR    RR  ')
#print('  CCCCCCCC  AA         AA RR     RR RR     RR EEEEEEEE FF       OOOOOOOO UUUUUUUU RR     RR   ')
#print('')
#print('')
#print('  CCCCCCCC       AAA      RRRRRRRR  RRRRRRRR  EEEEEEEE FFFFFFFF OOOOOOOO UU    UU RRRRRRRR    . ')
#print('  CC            AA AA     RR  RRR   RR  RRR   EE       FF       OO    OO UU    UU RR  RRR   ')
#print('  CC           AA   AA    RRRRR     RRRRR     EEEEEEE  FFFFFF   OO    OO UU    UU RRRRR   ')
#print('  CC          AAAAAAAAA   RR   RR   RR   RR   EE       FF       OO    OO UU    UU RR   RR  ')
#print('  CC         AA       AA  RR    RR  RR    RR  EE       FF       OO    OO UU    UU RR    RR  ')
#print('  CCCCCCCC  AA         AA RR     RR RR     RR EEEEEEEE FF       OOOOOOOO UUUUUUUU RR     RR   ')
#print('')
#print('')


print(' -------------------- BEM VINDO AO PROGRAMA DE ENVIO DE CONTRATAÇÃO --------------------------')
print('')
print('')
df1 = pd.DataFrame()

#print(df.columns)
#df1['chave_primaria'] = df['origem']+ df['loja']+ df['cidade_destino']+df['uf_destino']+df['tipo_veiculo']

df['chave_primaria'] = df['origem']+ df['loja']+ df['cidade_destino']+df['uf_destino']+df['tipo_veiculo']
#print("------------  Novo  -----------")

#Para recriar os indices e incluir o novo campo como indice
df.reset_index(inplace=True)
df.set_index('chave_primaria', drop=False, inplace=True)
#df.set_index(['chave_primaria', 'origem'], inplace=True)

# Ordenando o DataFrame pelas colunas 'coluna1' e 'coluna2'
# Por padrão, ordena de forma crescente; use ascending=False para ordem decrescente
#df2 = df.sort_values(by=['chaveprimaira'])
df_sorted1 = df.sort_values(by=['FRETE'], ascending=[True])

#print(df_sorted1)

dados = pd.DataFrame(df_sorted1, columns=['chave_primaria','origem','cidade_destino', 
        'uf_destino', 'tipo_veiculo', 'data_carregamento', 'hora_carregamento', 'peso', 'RANKING', 
        'FRETE', 'Tel_Fixo'])
#print(dados.columns)

#'origem', 'loja', 'cidade_destino', 'uf_destino', 'tipo_veiculo',

# Iterando sobre as linhas da planilha
for index, row in df.iterrows():
    tel_fixo = str(row['Tel_Fixo'])
        # Garantindo que o número está no formato correto para o WhatsApp
    if not tel_fixo.startswith('+'):
        tel_fixo = f"+{tel_fixo}"
    
    message = f'''Boa tarde !!!
    Tem veículo disponível para atender a rota?
    Detalhes: {row.to_dict()}'''
    loja_carrefour = str(row['loja'])
    Transp = str(row['NOME_TRANSPORTADORA'])
    veiculo = str(row['tipo_veiculo'])
    valor = str(row['FRETE'])
    faixa = str(row['RANKING'])
    origem = str(row['origem'])
    cidade_destino = str(row['cidade_destino'])
    uf_destino = str(row['uf_destino'])
    data_car1 = str(row['data_carregamento'])
    data_car = str(f'{data_car1[8:9]}/{data_car1[5:6]}/{data_car1[0:3]}')
    #data_car = datetime.strptime(data_car1, '%d/%m/%Y')
    hora_car1 = str(row['hora_carregamento'])
    hora_car = hora_car1[10:15]
    peso = int(row['peso'])
    #peso = round(peso1,2)
    #data_e_hora_em_texto = ‘01/03/2018 12:30’
    #data_e_hora = datetime.strptime(data_e_hora_em_texto, ‘%d/%m/%Y %H:%M’)
    
    
    msg = f''' Boa tarde !!!
    Tem veículo disponível para atender a rota abaixo ?
    Veículo:{veiculo}
    LOJA:{loja_carrefour}
    Origem:{origem}
    Destino:{cidade_destino}-{uf_destino}
    Data de carregamento:{data_car} {hora_car}
    peso:{peso}
    
    No aguardo !    
    '''
    
        
    print(f'------------------- Enviar solicitação de Contratação de veículo ------------------------------------')
    print(f'Loja:{loja_carrefour}')
    print(f'Transp:{Transp} - Tipo veículo:{veiculo}')
    print(f'Frete:{valor},00')
    print(f'faixa:{faixa} --- Peso (ton): {peso}')
    print(f'------------------------------------------------------------------------------------------------------')
    #print(message)
    print('')
    print('Enviar para este transportador ?')
    print('1=Sim ou 2=Não ?')
    enviar = input(str('')).strip()   
    if enviar in '1':
        # Enviando a mensagem via WhatsApp Web
        try:
        # Enviar mensagem via WhatsApp Web
            kit.sendwhatmsg_instantly(tel_fixo, msg, 15, True, 5)
            print(f"Mensagem enviada para {tel_fixo}")        
        # Aguardar alguns segundos antes de enviar a próxima mensagem
            time.sleep(5)
        except Exception as e:
            print(f"Erro ao enviar mensagem para {tel_fixo}: {e}")
    else:
        print(f"Não enviado para {tel_fixo}")