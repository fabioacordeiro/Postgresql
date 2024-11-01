
#pip install pandas
# pywhatkit
# openpyxl


import pandas as pd
import pywhatkit as kit
import time

# Lendo a planilha
file_path = 'CONTRATACAO.xlsx'
df = pd.read_excel(file_path)

# Verificando os nomes das colunas
print(df.columns)
time.sleep(1)
# Iterando sobre as linhas da planilha
for index, row in df.iterrows():
    tel_fixo = str(row['Tel_Fixo'])
    
    # Garantindo que o número está no formato correto para o WhatsApp
    if not tel_fixo.startswith('+'):
        tel_fixo = f"+{tel_fixo}"
    
    message = f'''Bom dia !!!
    Tem veículo disponível para atender a rota?
    Detalhes: {row.to_dict()}'''
    
    # Enviando a mensagem via WhatsApp Web
    try:
        # Enviar mensagem via WhatsApp Web
        kit.sendwhatmsg_instantly(tel_fixo, message, 15, True, 5)
        print(f"Mensagem enviada para {tel_fixo}")
        
        # Aguardar alguns segundos antes de enviar a próxima mensagem
        time.sleep(5)
        
    except Exception as e:
        print(f"Erro ao enviar mensagem para {tel_fixo}: {e}")