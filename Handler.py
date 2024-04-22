from Queue import Queue  # Mudei de 'Queue' para 'queue'
from DataFrame import DataFrame  # Mantive a importação da classe DataFrame

class Handler:
    def __init__(self, inputQueues: list):
        self.inputQueues = inputQueues
        self.outputQueues = [Queue() for _ in inputQueues]  # Inicialização automática de outputQueues

    def handle(self):
        for i, queue in enumerate(self.inputQueues):
            output_queue = Queue()
            while not queue.is_empty():  # Mudei de 'is_empty' para 'empty'
                dataframe = queue.dequeue()  # Mudei de 'dequeue' para 'get'
                processeddataframe = self.process(dataframe)
                output_queue.enqueue(processeddataframe)  # Mudei de 'enqueue' para 'put'
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


class TratadorSomaValoresPorFila(Handler):
    def __init__(self, inputQueues, coluna):
        super().__init__(inputQueues)
        self.coluna = coluna

    def handle(self):
        for i, input_queue in enumerate(self.inputQueues):
            total_sum = 0
            while not input_queue.is_empty():
                df = input_queue.dequeue()
                col_values = df.get_column(self.coluna)  
                total_sum += sum(col_values)  
            self.outputQueues[i].enqueue(DataFrame({"Queue_Total_Sum": [total_sum]}))




class TratadorSomaTotalEntreFilas(Handler):
    def __init__(self, inputQueues):
        super().__init__(inputQueues)
        self.outputQueues = [Queue() for _ in inputQueues]

    def handle(self):
        total_sum = 0
        for fila in self.inputQueues:
            fila_soma = 0
            while not fila.is_empty():
                df = fila.dequeue()
                fila_soma += df.values[0][0]  # Somando os valores de Total_sum de cada DataFrame na fila
            total_sum += fila_soma
        
        output_queues = []
        for i, fila in enumerate(self.inputQueues):
            output_queue = self.outputQueues[i]  # Utilizando o atributo de outputQueue da classe
            if i == 0:
                output_queue.enqueue(DataFrame({"Total_Total_sum": [total_sum]}))
            else:
                output_queue.enqueue(None)
            output_queues.append(output_queue)
        
        return output_queues


class TratadorUnificarDataFrames(Handler):
    def handle(self):
        for i, input_queue in enumerate(self.inputQueues):
            unified_dataframe = self._unify_dataframes(input_queue)
            self.outputQueues[i].enqueue(unified_dataframe)


    def _unify_dataframes(self, queue):
        unified_data = {}
        while not queue.is_empty():
            df = queue.dequeue()
            for col in df.columns:
                if col not in unified_data:
                    unified_data[col] = []
                unified_data[col].extend(df.get_column(col))
        return DataFrame(unified_data)



class TratadorUnificarDataFramesPorColunas(Handler):
    def handle(self):
        unified_dataframes = self._unify_dataframes()
        output_queues = [Queue() for _ in range(len(unified_dataframes))]
        for i, unified_df in enumerate(unified_dataframes):
            output_queues[i].enqueue(unified_df)
        return output_queues

    def _unify_dataframes(self):
        unified_dataframes = []
        colunas_comuns = self._get_common_columns()
        if not colunas_comuns:
            return unified_dataframes  # Retorna uma lista vazia se não houver colunas comuns
        for input_queue in self.inputQueues:
            unified_data = {}
            while not input_queue.is_empty():
                df = input_queue.dequeue()
                for col in colunas_comuns:
                    if col not in unified_data:
                        unified_data[col] = []
                    if col in df.columns:
                        unified_data[col].extend(df.get_column(col))
                    else:
                        unified_data[col].extend([None] * len(df))
            unified_dataframes.append(DataFrame(unified_data))
        return unified_dataframes

    def _get_common_columns(self):
        common_cols = set()
        first_queue = self.inputQueues[0]
        while not first_queue.is_empty():
            df = first_queue.peek()
            common_cols.update(df.columns)
            first_queue.dequeue()
        for queue in self.inputQueues:
            while not queue.is_empty():
                df = queue.dequeue()
                common_cols.intersection_update(df.columns)
        return list(common_cols)







