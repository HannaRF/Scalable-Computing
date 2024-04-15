import sqlite3
import random
import string

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

conn = sqlite3.connect('mocks/artist_database.db')
cursor = conn.cursor()

cursor.execute('''CREATE TABLE artistas
    (cod_artista INTEGER PRIMARY KEY AUTOINCREMENT,
    nome_completo TEXT,
    nome_artistico TEXT,
    cpf TEXT,
    associacao TEXT)''')

artistas = [
('Marina de Oliveira Sena', 'Marina Sena', generate_cpf(), 'UBC'),
('Eduarda Bittencourt Simões', 'DUDA BEAT', generate_cpf(), 'UBC'),
('Geizon Carlos da Cruz Fernandes', 'Xamã', generate_cpf(), 'SOCINPRO'),
('Maria Bethânia Viana Teles Veloso', 'Maria Bethânia', generate_cpf(), 'UBC'),
('Phabullo Rodrigues da Silva', 'Pabllo Vittar', generate_cpf(), 'UBC')
]

cursor.executemany('INSERT INTO artistas (nome_completo, nome_artistico, cpf, associacao) VALUES (?, ?, ?, ?)', artistas)

conn.commit()
conn.close()