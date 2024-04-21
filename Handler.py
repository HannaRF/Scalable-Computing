from queue import Queue  # Mudei de 'Queue' para 'queue'
from DataFrame import DataFrame  # Mantive a importação da classe DataFrame

class Handler:
    def __init__(self, inputQueues: list):
        self.inputQueues = inputQueues
        self.outputQueues = [Queue() for _ in inputQueues]  # Inicialização automática de outputQueues

    def handle(self):
        for i, queue in enumerate(self.inputQueues):
            output_queue = Queue()
            while not queue.empty():  # Mudei de 'is_empty' para 'empty'
                dataframe = queue.get()  # Mudei de 'dequeue' para 'get'
                processeddataframe = self.process(dataframe)
                output_queue.put(processeddataframe)  # Mudei de 'enqueue' para 'put'
            self.outputQueues[i] = output_queue

    def process(self, dataframe: DataFrame):
        # Process the dataframe
        return processeddataframe

class TratadorLimpezaCSV(Handler):
    def process(self, dataframe: DataFrame) -> DataFrame:
        cleaneddata = []
        for row in dataframe.values:  # Mudei de 'dataframe._data' para 'dataframe.values'
            cleaned_row = [self.clean_value(value) for value in row]
            cleaneddata.append(cleaned_row)
        return DataFrame(dict(zip(dataframe.columns, zip(*cleaneddata))))

    def clean_value(self, value):
        value = self.convert_to_int(value)
        if isinstance(value, float):
            value = self.substitute_dot_by_comma(str(value))
        if isinstance(value, str):
            value = self.remove_accent(value)  # Remover acentuação
        return value

    def substitute_dot_by_comma(self, text):
        return text.replace('.', ',')

    def remove_accent(self, text):
        accent_table = {
            'á': 'a', 'à': 'a', 'ã': 'a', 'â': 'a',
            'é': 'e', 'ê': 'e', 'í': 'i', 'ó': 'o',
            'õ': 'o', 'ô': 'o', 'ú': 'u', 'ü': 'u',
            'ç': 'c', 'ñ': 'n', 'Á': 'A', 'À': 'A',
            'Ã': 'A', 'Â': 'A', 'É': 'E', 'Ê': 'E',
            'Í': 'I', 'Ó': 'O', 'Õ': 'O', 'Ô': 'O',
            'Ú': 'U', 'Ü': 'U', 'Ç': 'C', 'Ñ': 'N'
        }
        text_without_accent = ''.join(accent_table.get(char, char) for char in text)
        return text_without_accent
    
    def convert_to_int(self, text):
        try:
            return int(float(text))
        except ValueError:
            return text

class TratadorFiltroNomeCodigo(Handler):
    def __init__(self, input_queues: list, termo_pesquisa, nome_coluna):
        super().__init__(input_queues)
        self.termo_pesquisa = termo_pesquisa
        self.nome_coluna = nome_coluna

    def process(self, dataframe: DataFrame) -> DataFrame:
        col_index = list(dataframe.columns).index(self.nome_coluna)  # Mudei de 'dataframe._columns' para 'dataframe.columns'
        filtered_rows = [row for row in dataframe.values if row[col_index] == self.termo_pesquisa]  # Mudei de 'dataframe._data' para 'dataframe.values'
        return DataFrame(dict(zip(dataframe.columns, zip(*filtered_rows))))

class TratadorMerge(Handler):
    def __init__(self, input_queues: list, chave_merge: str):
        super().__init__(input_queues)
        self.chave_merge = chave_merge

    def process(self, dataframe: DataFrame) -> DataFrame:
        merged_data = {}
        
        for i, queue in enumerate(self.inputQueues):
            while not queue.empty():
                df = queue.get()
                for row in df.values:
                    if isinstance(row, dict):
                        chave = row.get(self.chave_merge)
                    else:
                        chave = row[list(dataframe.columns).index(self.chave_merge)]  # Mudei de 'dataframe._columns' para 'dataframe.columns'
                    if chave in merged_data:
                        merged_data[chave].append(row)
                    else:
                        merged_data[chave] = [row]

        merged_dfs = []
        
        for chave, linhas in merged_data.items():
            merged_df = DataFrame(dict(zip(dataframe.columns, zip(*linhas))))
            merged_dfs.append(merged_df)
        
        return merged_dfs

#Tratador que soma os valores de uma coluna específica
class TratadorSoma(Handler):
    def __init__(self, input_queues: list, coluna_soma: str):
        super().__init__(input_queues)
        self.coluna_soma = coluna_soma

    def process(self, dataframe: DataFrame) -> DataFrame:
        col_index = list(dataframe.columns).index(self.coluna_soma)  # Mudei de 'dataframe._columns' para 'dataframe.columns'
        soma = sum([row[col_index] for row in dataframe.values])  # Mudei de 'dataframe._data' para 'dataframe.values'
        return soma


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
input_queue.put(df)
input_queue.put(df2)
input_queue2.put(df3)
input_queue2.put(df4)

# Instanciar e usar tratador de limpeza CSV
tratador_limpeza_csv = TratadorLimpezaCSV(input_queues)
tratador_limpeza_csv.handle()
df_limpo = tratador_limpeza_csv.outputQueues[0].get()  # Mudei de 'peek' para 'get'
df_limpo2 = tratador_limpeza_csv.outputQueues[1].get()  # Mudei de 'peek' para 'get'
print("DataFrame limpo:")
print(df_limpo2)

# Instanciar e usar tratador de filtro por nome e código
tratador_filtro_nome_codigo = TratadorFiltroNomeCodigo(tratador_limpeza_csv.outputQueues, 3, "Código Artista")
tratador_filtro_nome_codigo.handle()
df_filtrado = tratador_filtro_nome_codigo.outputQueues[0].get() # Mudei de 'peek' para 'get'
# Imprimir DataFrame filtrado
print("DataFrame filtrado:")
print(df_filtrado)

# Adicionar DataFrame à fila de entrada novamente
input_queue.put(df)
input_queue.put(df2)
input_queue2.put(df3)
input_queue2.put(df4)
input_queues = [input_queue, input_queue2]

# Instanciar e usar tratador de merge
tratador_merge = TratadorMerge(input_queues, "Código Artista")
tratador_merge.handle()
df_mesclado = tratador_merge.outputQueues[0].get()[0]
# Imprimir DataFrame mesclado
print("DataFrame mesclado:")
print(df_mesclado)

print(df2)

