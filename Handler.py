from Queue import Queue
from DataFrame import DataFrame

class Handler:
    def __init__(self, inputQueues: list, outputQueues: list):
        self.inputQueues = inputQueues
        self.outputQueues = outputQueues
    
    def handle(self):
        for queue in self.inputQueues:
            output_queue = Queue()
            while not queue.is_empty():
                dataframe = queue.dequeue()
                processeddataframe = self.process(dataframe)
                output_queue.enqueue(processeddataframe)
            self.outputQueues.append(output_queue)
            self.outputQueues.pop(0)

    
    def process(self, dataframe: DataFrame):
        # Process the dataframe
        return processeddataframe
    

class TratadorLimpezaCSV(Handler):
    def __init__(self, input_queues: list, output_queues: list):
        super().__init__(input_queues, output_queues)

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
    def __init__(self, input_queues: list, output_queues: list, termo_pesquisa, nome_coluna):
        super().__init__(input_queues, output_queues)
        self.termo_pesquisa = termo_pesquisa
        self.nome_coluna = nome_coluna

    def process(self, dataframe: DataFrame) -> DataFrame:
        col_index = dataframe._columns.index(self.nome_coluna)
        filtered_rows = [row for row in dataframe._data if row[col_index] == self.termo_pesquisa]
        return DataFrame(filtered_rows, columns=dataframe._columns)







# Exemplo de uso:

# Criar filas de entrada e saída
input_queue = Queue()
output_queue = Queue()

# Criar DataFrame de exemplo
data = [
    ["Fulano", 30.5, "Engénheiro"],
    ["Béltrano", 35, "Desenvolvedor"],
    ["Beltrano", 45, "Estudante"],
    ["Ciclano", 45, "Designer"]
]

# Criando uma instância da classe DataFrame com os dados de teste
df = DataFrame(data, columns=["Nome", "Idade", "Profissão"])

#print(df)
# Adicionar DataFrame à fila de entrada
input_queue.enqueue(df)
input_queues = [input_queue]
output_queues = [output_queue]

# Instanciar e usar tratador de limpeza CSV
tratador_limpeza_csv = TratadorLimpezaCSV(input_queues, output_queues)
tratador_limpeza_csv.handle()
#df_limpo = tratador_limpeza_csv.outputQueues[0].dequeue()
# Imprimir DataFrame limpo
#print("DataFrame limpo:")
#print(df_limpo)


# Instanciar e usar tratador de filtro por nome e código
new_input_queues = [tratador_limpeza_csv.outputQueues[0]]
tratador_filtro_nome_codigo = TratadorFiltroNomeCodigo(new_input_queues, [Queue()], 45, "Idade")
tratador_filtro_nome_codigo.handle()
df_filtrado = tratador_filtro_nome_codigo.outputQueues[0].dequeue()
# Imprimir DataFrame filtrado
print("DataFrame filtrado:")
print(df_filtrado)

# Outros tratadores podem ser usados de forma semelhante, ajustando os parâmetros conforme necessário.
