from  Queue import Queue
from DataFrame import DataFrame
from Handler import TratadorLimpezaCSV, TratadorFiltroNomeCodigo, TratadorSomaValoresPorFila, TratadorSomaTotalEntreFilas, TratadorUnificarDataFrames


# Exemplo de uso:


# Criar filas de entrada
input_queue = Queue()
input_queue2 = Queue()
input_queues = [input_queue, input_queue2]

data1 = [
    ["01-01-2023", 1, "YOUTUBE", 5000],
    ["01-01-2023", 2, "SPOTIFY", 8000],
    ["01-01-2023", 3, "TV GLOBO", 6000],
    ["01-01-2023", 4, "RÁDIOS AM/FM", 7000],
    ["01-01-2023", 5, "SHOW AO VIVO", 10000],
    ["01-02-2023", 1, "YOUTUBE", 4500],
    ["01-02-2023", 3, "TV GLOBO", 5500],
    ["01-02-2023", 4, "RÁDIOS AM/FM", 6500],
    ["01-02-2023", 5, "SHOW AO VIVO", 10500],
    ["01-03-2023", 2, "SPOTIFY", 9000],
    ["01-03-2023", 3, "TV GLOBO", 7000],
    ["01-03-2023", 4, "RÁDIOS AM/FM", 8000],
    ["01-03-2023", 5, "SHOW AO VIVO", 11000],
    ["01-04-2023", 1, "YOUTUBE", 4800],
    ["01-04-2023", 2, "SPOTIFY", 8500],
    ["01-04-2023", 3, "TV GLOBO", 6500],
    ["01-04-2023", 5, "SHOW AO VIVO", 10000]
]

# Criar DataFrame de exemplo
data2 = [
    ["01-02-2024", 1, "YOUTUBE", 4800.50],
    ["01-02-2024", 3, "TV GLOBO", 5500.75],
    ["01-02-2024", 4, "RÁDIOS AM/FM", 6300.25],
    ["01-02-2024", 5, "SHOW AO VIVO", 10850.80],
    ["01-03-2024", 2, "SPOTIFY", 9200.35],
    ["01-03-2024", 3, "TV GLOBO", 7100.60],
    ["01-03-2024", 4, "RÁDIOS AM/FM", 8200.45],
    ["01-03-2024", 5, "SHOW AO VIVO", 11200.70],
    ["01-04-2024", 1, "YOUTUBE", 4600.90],
    ["01-04-2024", 2, "SPOTIFY", 8650.25],
    ["01-04-2024", 3, "TV GLOBO", 6800.30],
    ["01-04-2024", 5, "SHOW AO VIVO", 10250.95],
    ["01-05-2024", 2, "SPOTIFY", 8400.80],
    ["01-05-2024", 3, "TV GLOBO", 6200.50],
    ["01-05-2024", 4, "RÁDIOS AM/FM", 6900.20],
    ["01-05-2024", 5, "SHOW AO VIVO", 9700.10],
    ["01-06-2024", 1, "YOUTUBE", 4750.25],
    ["01-06-2024", 3, "TV GLOBO", 5800.45],
    ["01-06-2024", 4, "RÁDIOS AM/FM", 6400.70],
    ["01-06-2024", 5, "SHOW AO VIVO", 9850.30],
    ["01-07-2024", 2, "SPOTIFY", 8350.40],
    ["01-07-2024", 3, "TV GLOBO", 6950.25],
    ["01-07-2024", 4, "RÁDIOS AM/FM", 7250.15],
    ["01-07-2024", 5, "SHOW AO VIVO", 10600.20]
]

# Criando listas para cada coluna
datas1 = []
codigos_artistas1 = []
fontes1 = []
valores1 = []
# Separando os dados em listas de cada coluna
for linha in data1:
    datas1.append(linha[0])
    codigos_artistas1.append(linha[1])
    fontes1.append(linha[2])
    valores1.append(linha[3])

# Criando listas para cada coluna
datas2 = []
codigos_artistas2 = []
fontes2 = []
valores2 = []
# Separando os dados em listas de cada coluna
for linha in data2:
    datas2.append(linha[0])
    codigos_artistas2.append(linha[1])
    fontes2.append(linha[2])
    valores2.append(linha[3])


# Criar DataFrame de exemplo
df = DataFrame({
    "Data": datas1[:12],
    "Código Artista": codigos_artistas1[:12],
    "Fonte": fontes1[:12],
    "Valor": valores1[:12]
})
df2 = DataFrame({
    "Data": datas1[12:],
    "Código Artista": codigos_artistas1[12:],
    "Fonte": fontes1[12:],
    "Valor": valores1[12:]
})
df3 = DataFrame({
    "Data": datas2[:17],
    "Código Artista": codigos_artistas2[:17],
    "Fonte": fontes2[:17],
    "Valor": valores2[:17]
})
df4 = DataFrame({
    "Data": datas2[17:],
    "Código Artista": codigos_artistas2[17:],
    "Fonte": fontes2[17:],
    "Valor": valores2[17:]
})

print("DataFrame original:")
print(df3)
# Adicionar DataFrame à fila de entrada
input_queue.enqueue(df)
input_queue.enqueue(df2)
input_queue2.enqueue(df3)
input_queue2.enqueue(df4)

# Instanciar e usar tratador de limpeza CSV
tratador_limpeza_csv = TratadorLimpezaCSV(input_queues)
tratador_limpeza_csv.handle()
df_limpo = tratador_limpeza_csv.outputQueues[0].dequeue()  # Mudei de 'peek' para 'get'
df_limpo2 = tratador_limpeza_csv.outputQueues[1].dequeue()  # Mudei de 'peek' para 'get'
print("DataFrame limpo:")
print(df_limpo2)

# Instanciar e usar tratador de filtro por nome e código
tratador_filtro_nome_codigo = TratadorFiltroNomeCodigo(tratador_limpeza_csv.outputQueues, 3, "Código Artista")
tratador_filtro_nome_codigo.handle()
df_filtrado = tratador_filtro_nome_codigo.outputQueues[0].dequeue() # Mudei de 'peek' para 'get'
# Imprimir DataFrame filtrado
print("DataFrame filtrado:")
print(df_filtrado)

# Adicionar DataFrame à fila de entrada novamente
input_queue.enqueue(df)
input_queue.enqueue(df2)
input_queue2.enqueue(df3)
input_queue2.enqueue(df4)
input_queues = [input_queue, input_queue2]

# # Instanciar e usar tratador de merge
# tratador_merge = TratadorMerge(input_queues, "Código Artista")
# tratador_merge.handle()
# df_mesclado = tratador_merge.outputQueues[0].dequeue()[0]
# # Imprimir DataFrame mesclado
# print("DataFrame mesclado:")
# print(df_mesclado)

# print(df2)

# Instanciar e usar tratador de soma de valores

input_queue = Queue()
input_queue2 = Queue()
input_queue.enqueue(df)
input_queue.enqueue(df2)
input_queue2.enqueue(df3)
input_queue2.enqueue(df4)

input_queues = [input_queue, input_queue2]
tratador_soma_valores = TratadorSomaValoresPorFila(input_queues, "Valor")
tratador_soma_valores.handle()

# Imprimir soma total
print("Soma por fila:")
print(tratador_soma_valores.outputQueues[0].peek())


# Instanciar e usar tratador de soma entre filas
tratador_soma_valores_entre_filas = TratadorSomaTotalEntreFilas(tratador_soma_valores.outputQueues)
soma_total_entre_filas = tratador_soma_valores_entre_filas.handle()
# Imprimir soma total entre filas
print("Soma total entre as filas:")
print(soma_total_entre_filas[0].dequeue())

# # Instanciar e usar tratador de merge de DataFrames
# tratador_merge_dataframes = TratadorMergeDataFrames(input_queues, on="Código Artista")
# df_merged = tratador_merge_dataframes.handle()[0]
# # Imprimir DataFrame mesclado
# print("DataFrame mesclado:")
# print(df_merged)


input_queue = Queue()
input_queue2 = Queue()
input_queue.enqueue(df)
input_queue.enqueue(df2)
input_queue2.enqueue(df3)
input_queue2.enqueue(df4)

input_queues = [input_queue, input_queue2]

# # Instanciar e usar tratador de unificação de DataFrames

tratador_unificar_dataframes = TratadorUnificarDataFrames(input_queues)
tratador_unificar_dataframes.handle()
df_unificado = tratador_unificar_dataframes.outputQueues[0].dequeue()
# Imprimir DataFrame unificado
print("DataFrame unificado:")
print(df_unificado)


#Responder a pergunta de quanto o artista com codigo 3 ganhou
#Instanciar e usar tratador de filtro por nome e código

input_queue = Queue()
input_queue2 = Queue()
input_queue.enqueue(df)
input_queue.enqueue(df2)
input_queue2.enqueue(df3)
input_queue2.enqueue(df4)

input_queues = [input_queue, input_queue2]

tratador_filtro_nome_codigo = TratadorFiltroNomeCodigo(input_queues, 3, "Código Artista")
tratador_filtro_nome_codigo.handle()
#Usar tratador de soma de valores por fila
tratador_soma_valores = TratadorSomaValoresPorFila(tratador_filtro_nome_codigo.outputQueues, "Valor")
tratador_soma_valores.handle()
#Imprimir soma total por fila
print("Soma por fila:")
print(tratador_soma_valores.outputQueues[0].peek())
#Usar tratador de soma total entre filas
tratador_soma_valores_entre_filas = TratadorSomaTotalEntreFilas(tratador_soma_valores.outputQueues)
tratador_soma_valores_entre_filas.handle()
#Imprimir soma total entre filas
print("Soma total entre as filas:")
print(tratador_soma_valores_entre_filas.outputQueues[0].peek())