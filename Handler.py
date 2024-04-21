from Queue import Queue
from DataFrame import DataFrame

class Handler:
    def __init__(self, inputQueues: list):
        self.inputQueues = inputQueues
        self.outputQueues = [Queue() for _ in inputQueues]  # Inicialização automática de outputQueues

    def handle(self):
        for i, queue in enumerate(self.inputQueues):
            output_queue = Queue()
            while not queue.is_empty():
                dataframe = queue.dequeue()
                processeddataframe = self.process(dataframe)
                output_queue.enqueue(processeddataframe)
            self.outputQueues[i] = output_queue

    def process(self, dataframe: DataFrame):
        # Process the dataframe
        return processeddataframe

class TratadorLimpezaCSV(Handler):
    def process(self, dataframe: DataFrame) -> DataFrame:
        cleaneddata = []
        for row in dataframe._data:
            cleaned_row = [self.clean_value(value) for value in row]
            cleaneddata.append(cleaned_row)
        return DataFrame(cleaneddata, columns=dataframe._columns)

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
        # Dicionário de mapeamento de caracteres acentuados para seus equivalentes sem acentos
        accent_table = {
            'á': 'a', 'à': 'a', 'ã': 'a', 'â': 'a',
            'é': 'e', 'ê': 'e', 'í': 'i', 'ó': 'o',
            'õ': 'o', 'ô': 'o', 'ú': 'u', 'ü': 'u',
            'ç': 'c', 'ñ': 'n', 'Á': 'A', 'À': 'A',
            'Ã': 'A', 'Â': 'A', 'É': 'E', 'Ê': 'E',
            'Í': 'I', 'Ó': 'O', 'Õ': 'O', 'Ô': 'O',
            'Ú': 'U', 'Ü': 'U', 'Ç': 'C', 'Ñ': 'N'
        }

        # Aplicando o mapeamento de caracteres
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
        col_index = dataframe._columns.index(self.nome_coluna)
        filtered_rows = [row for row in dataframe._data if row[col_index] == self.termo_pesquisa]
        return DataFrame(filtered_rows, columns=dataframe._columns)


class TratadorMerge(Handler):
    def __init__(self, input_queues: list, chave_merge: str):
        super().__init__(input_queues)
        self.chave_merge = chave_merge

    def process(self, dataframe: DataFrame) -> DataFrame:
        # Inicializa um dicionário vazio para armazenar as linhas mescladas por ID
        merged_data = {}
        
        # Itera sobre cada fila de entrada
        for i, queue in enumerate(self.inputQueues):
            while not queue.is_empty():
                df = queue.dequeue()
                for row in df._data:
                    # Obtém o valor da chave para mesclar
                    if isinstance(row, dict):
                        chave = row.get(self.chave_merge)
                    else:
                        chave = row[dataframe._columns.index(self.chave_merge)]
                    
                    # Verifica se a chave já existe no dicionário
                    if chave in merged_data:
                        # Se a chave já existe, adiciona a linha atual aos dados mesclados
                        merged_data[chave].append(row)
                    else:
                        # Se a chave não existe, cria uma nova entrada no dicionário
                        merged_data[chave] = [row]

        # Inicializa uma lista para armazenar os DataFrames mesclados
        merged_dfs = []
        
        # Converte os dados mesclados em DataFrames e os adiciona à lista
        for chave, linhas in merged_data.items():
            merged_df = DataFrame(linhas, columns=dataframe._columns)
            merged_dfs.append(merged_df)
        
        # Retorna a lista de DataFrames mesclados
        return merged_dfs

# Exemplo de uso:

# Criar filas de entrada
input_queue = Queue()
input_queue2 = Queue()
output_queue = Queue()

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
# Criando uma instância da classe DataFrame com os dados de teste
df = DataFrame(data1[:12], columns = ["Data", "Código Artista", "Fonte", "Valor"])
df2 = DataFrame(data1[12:], columns = ["Data", "Código Artista", "Fonte", "Valor"])
df3 = DataFrame(data2[:17], columns = ["Data", "Código Artista", "Fonte", "Valor"])
df4 = DataFrame(data2[17:], columns = ["Data", "Código Artista", "Fonte", "Valor"])
# Adicionar DataFrame à fila de entrada
input_queue.enqueue(df)
input_queue.enqueue(df2)
input_queue2.enqueue(df3)
input_queue2.enqueue(df4)

input_queues = [input_queue, input_queue2]

# Instanciar e usar tratador de limpeza CSV
tratador_limpeza_csv = TratadorLimpezaCSV(input_queues)
tratador_limpeza_csv.handle()
df_limpo = tratador_limpeza_csv.outputQueues[0]
df_limpo2 = tratador_limpeza_csv.outputQueues[1]
print("DataFrame limpo:")
print(df_limpo2.peek())


# Instanciar e usar tratador de filtro por nome e código
tratador_filtro_nome_codigo = TratadorFiltroNomeCodigo(tratador_limpeza_csv.outputQueues, 3, "Código Artista")
tratador_filtro_nome_codigo.handle()
df_filtrado = tratador_filtro_nome_codigo.outputQueues[0].peek()
# Imprimir DataFrame filtrado
print("DataFrame filtrado:")
print(df_filtrado)

input_queue.enqueue(df)
input_queue.enqueue(df2)
input_queue2.enqueue(df3)
input_queue2.enqueue(df4)
input_queues = [input_queue, input_queue2]
# Instanciar e usar tratador de merge
tratador_merge = TratadorMerge(input_queues, "Código Artista")
tratador_merge.handle()
df_mesclado = tratador_merge.outputQueues[0].peek()[0]
# Imprimir DataFrame mesclado
print("DataFrame mesclado:")
print(df_mesclado)

print(df2)
