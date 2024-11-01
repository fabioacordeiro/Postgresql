# Fabio Alves Cordeiro 17/08/2024 
# pip install pandas 
# pip install selenium
# pip install openpyxl 

import tkinter as tk
from tkinter import filedialog, messagebox
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time

def enviar_mensagens():
    try:
        # Abre a planilha Excel
        file_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
        df = pd.read_excel(file_path)

        # Inicializa o WebDriver do Selenium
        driver = webdriver.Chrome()

        for _, row in df.iterrows():
            # Obtem os dados de telefone e nome
            telefone = row['Tel_Fixo']
            mensagem = f"Saudações, tem veículo disponível para atender a rota? {row.to_dict()}"

            # Abre o WhatsApp Web
            driver.get(f"https://web.whatsapp.com/send?phone={telefone}&text={mensagem}")
            time.sleep(10)  # Aguarda o carregamento da página

            # Envia a mensagem
            input_box = driver.find_element_by_xpath("//div[@contenteditable='true']")
            input_box.send_keys(Keys.ENTER)
            time.sleep(2)  # Aguarda antes de enviar a próxima mensagem

        messagebox.showinfo("Sucesso", "Mensagens enviadas com sucesso!")
    except Exception as e:
        messagebox.showerror("Erro", f"Ocorreu um erro: {str(e)}")

def sair():
    root.destroy()

# Cria a janela principal
root = tk.Tk()
root.title("Enviar Mensagens de Contratação")

# Botão para enviar mensagens
btn_enviar = tk.Button(root, text="Enviar Mensagens", command=enviar_mensagens)
btn_enviar.pack(pady=10)

# Botão para sair
btn_sair = tk.Button(root, text="Sair", command=sair)
btn_sair.pack(pady=10)

# Inicia a interface gráfica
root.mainloop()