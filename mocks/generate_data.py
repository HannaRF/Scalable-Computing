import sqlite3
import random
import string
import os
import warnings
import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait

warnings.filterwarnings("ignore")

def generate_cpf():
    cpf = ''
    for i in range(14):
        if i == 3 or i == 7:
            cpf += '.'
        elif i == 11:
            cpf += '-'
        else:
            cpf += str(random.randint(0, 9))
    return cpf

def connect():
    return sqlite3.connect('mocks/artist_database.db').cursor()

def generate_ecad_data():
    lista_final = []

    # The chromedriver happens to break sometimes, so we have to keep trying until it works
    while len(lista_final) == 0:
        try:
            # remove warnings
            warnings.filterwarnings("ignore")

            # Define the username and password
            username = "etl@adagg.io"
            password = "Rosalv0!"
            link = f'https://www.ecadnet.org.br/client/app/#/Detalhes/Titular/{np.random.randint(1,100000)}/'

            # initialize the Chrome driver
            options = webdriver.ChromeOptions()
            #options.add_argument('--headless')
            options.add_experimental_option('excludeSwitches', ['enable-logging'])
            driver = webdriver.Chrome(options=options)

            driver.get(link)

            # Login
            driver.implicitly_wait(2)
            driver.find_element(By.XPATH, "//input[@type='email']").send_keys(username)
            driver.find_element(By.XPATH, "//input[@type='password']").send_keys(password)
            driver.execute_script("arguments[0].click();", driver.find_element(By.XPATH, "//input[@type='submit']"))

            # has to wait for the page to load
            driver.execute_script("arguments[0].click();", driver.find_element(By.XPATH, "//input[@type='submit']"))
            WebDriverWait(driver, 10).until(lambda driver: driver.find_element(By.XPATH, "//div[@class='boxContainerInto larguraBox']"))

            # find the elements on the page
            nome_titular = driver.find_element(By.XPATH, "//div[@class='tituloObraNome ng-binding']")
            pseudonimo = driver.find_element(By.XPATH, "//div[@class='pseudonimoTitular ng-binding']")

            # Colect the data
            nome_titular = nome_titular.text
            pseudonimo = pseudonimo.text

            # find the div that contains the table
            div_master = driver.find_element(By.XPATH, "//div[@class='boxContainerInto larguraBox']")
            lista_associacoes = []

            # Loop through the table and append the data to the list
            for linha in div_master.find_elements(By.XPATH, ".//div[@ng-repeat='titularidade in item.Filiacoes']"):
                lista_associacoes.append(linha.text.split('\n'))

            associacao = lista_associacoes[0][0]
            
            lista_final = [nome_titular, pseudonimo, generate_cpf(), associacao]

            # Close the driver
            driver.quit()
        
        except:
            pass

    return lista_final

def upload_info(lista: list, cursor: sqlite3.Cursor):

    # Table creation
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS artists (
        artist_code INTEGER PRIMARY KEY AUTOINCREMENT,
        full_name TEXT, artistic_name TEXT,
        cpf TEXT,
        association TEXT)
    ''')
    cursor.connection.commit()

    # Insert data
    cursor.execute('''
    INSERT INTO artists (full_name, artistic_name, cpf, association)
    VALUES (?, ?, ?, ?)
    ''', lista)
    cursor.connection.commit()

def get_association_list(cursor: sqlite3.Cursor):

    # Table creation
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS artists (
        artist_code INTEGER PRIMARY KEY AUTOINCREMENT,
        full_name TEXT, artistic_name TEXT,
        cpf TEXT,
        association TEXT)
    ''')

    cursor.connection.commit()

    cursor.execute('''
    SELECT DISTINCT association FROM artists
    ''')

    return [i[0] for i in cursor.fetchall()]

def get_artist_code(cursor: sqlite3.Cursor):

    # Table creation
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS artists (
        artist_code INTEGER PRIMARY KEY AUTOINCREMENT,
        full_name TEXT, artistic_name TEXT,
        cpf TEXT,
        association TEXT)
    ''')

    cursor.connection.commit()

    cursor.execute('''
    SELECT DISTINCT artist_code FROM artists
    ''')

    return [i[0] for i in cursor.fetchall()]

def generate_royalties_data(cursor: sqlite3.Cursor):
    lista_codigos = get_artist_code(cursor)

    fontes = ["YOUTUBE", "SPOTIFY", "TV GLOBO", "RÁDIOS AM/FM", "SHOWS AO VIVO", "SONORIZAÇÃO AMBIENTAL"]

    df = pd.DataFrame(columns=['data','cod_artista','fonte','valor_rendimento'])

    for i in range(1000):
        fonte = np.random.choice(fontes, p=[0.15, 0.3, 0.1, 0.15, 0.25, 0.05])
        cod_artista = np.random.choice(lista_codigos)
        data = f'{random.randint(2010,2021)}-{random.randint(1,12)}-{random.randint(1,28)}'
        valor_rendimento = round(random.uniform(0.01, 100), 2)

        df = pd.concat([df, pd.DataFrame([[data, cod_artista, fonte, valor_rendimento]], columns=['data','cod_artista','fonte','valor_rendimento'])])

    # generate serial code with 10 characters and 5 digits
    code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
    df.to_csv(f'mocks/csvs/royalties_{code}.csv', index=False)

def generate_followers_data(cursor: sqlite3.Cursor):
    codes_list = get_artist_code(cursor)

    final_list = []

    for code in codes_list:
        final_list.append({
            "artist_code": code,
            "instagram": random.randint(0, 100000),
            "twitter": random.randint(0, 100000),
            "facebook": random.randint(0, 100000),
            "youtube": random.randint(0, 100000),
            "spotify": random.randint(0, 100000)
        })

    return final_list

# cursor = connect()
# for i in range(10):
#     lista = generate_ecad_data()
#     upload_info(lista, cursor)
# cursor.connection.close()

# cursor = connect()
# for i in range(10):
#     generate_royalties_data(cursor)
# cursor.connection.close()

cursor = connect()
followers_list = generate_followers_data(cursor)
cursor.connection.close()

print(followers_list)