"""
This module provides a set of classes for handling data processing tasks.

Classes:
    Handler: A generic handler for processing data queues.
    TratadorLimpezaCSV: A handler for cleaning CSV data.
    TratadorFiltroNomeCodigo: A handler for filtering data based on a column name and a search term.
    TratadorSomaValoresPorFila: A handler for summing values in a specified column across data queues.
    TratadorSomaTotalEntreFilas: A handler for summing total values across multiple data queues.
    TratadorUnificarDataFrames: A handler for unifying data frames from a single queue.
    TratadorUnificarDataFramesPorColunas: A handler for unifying data frames based on common columns from multiple queues.
"""


from Queue import Queue  # Mudei de 'Queue' para 'queue'
from DataFrame import DataFrame  # Mantive a importação da classe DataFrame

class Handler:
    """
    A generic handler for processing data queues.

    Attributes:
        inputQueues (list): A list of input queues containing data to be processed.
        outputQueues (list): A list of output queues to store processed data.

    Methods:
        __init__: Initializes the Handler object with input queues.
        handle: Processes data from input queues and stores processed data in output queues.
        process: Placeholder method for data processing, to be implemented by subclasses.
    """
    def __init__(self, inputQueues: list):
        """
        Initializes the Handler object with input queues.

        Args:
            inputQueues (list): A list of input queues containing data to be processed.
        """
        self.inputQueues = inputQueues
        self.outputQueues = [Queue() for _ in inputQueues]  # Inicialização automática de outputQueues

    def handle(self):
        """
        Processes data from input queues and stores processed data in output queues.
        """
        for i, queue in enumerate(self.inputQueues):
            output_queue = Queue()
            while not queue.is_empty():  # Mudei de 'is_empty' para 'empty'
                dataframe = queue.dequeue()  # Mudei de 'dequeue' para 'get'
                processeddataframe = self.process(dataframe)
                output_queue.enqueue(processeddataframe)  # Mudei de 'enqueue' para 'put'
            self.outputQueues[i] = output_queue

    def process(self, dataframe: DataFrame):
        """
        Placeholder method for data processing, to be implemented by subclasses.

        Args:
            dataframe (DataFrame): The data frame to be processed.

        Returns:
            DataFrame: The processed data frame.
        """
        # Process the dataframe
        return processeddataframe

class TratadorLimpezaCSV(Handler):
    """
    A handler for cleaning CSV data.

    Methods:
        process: Cleans the CSV data frame.
        clean_value: Cleans individual values in the data frame.
        substitute_dot_by_comma: Substitutes dots with commas in text.
        remove_accent: Removes accents from text.
        convert_to_int: Converts text to integers.
    """
    def process(self, dataframe: DataFrame) -> DataFrame:
        """
        Cleans the CSV data frame.

        Args:
            dataframe (DataFrame): The CSV data frame to be cleaned.

        Returns:
            DataFrame: The cleaned CSV data frame.
        """
        cleaneddata = []
        for row in dataframe.values:  # Mudei de 'dataframe._data' para 'dataframe.values'
            cleaned_row = [self.clean_value(value) for value in row]
            cleaneddata.append(cleaned_row)
        return DataFrame(dict(zip(dataframe.columns, zip(*cleaneddata))))

    def clean_value(self, value):
        """
        Cleans individual values in the data frame.

        Args:
            value: The value to be cleaned.

        Returns:
            The cleaned value.
        """
        value = self.convert_to_int(value)
        if isinstance(value, float):
            value = self.substitute_dot_by_comma(str(value))
        if isinstance(value, str):
            value = self.remove_accent(value)  # Remover acentuação
        return value

    def substitute_dot_by_comma(self, text):
        """
        Substitutes dots with commas in text.

        Args:
            text (str): The text to be processed.

        Returns:
            str: The processed text.
        """
        return text.replace('.', ',')

    def remove_accent(self, text):
        """
        Removes accents from text.

        Args:
            text (str): The text to be processed.

        Returns:
            str: The processed text.
        """
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
        """
        Converts text to integers.

        Args:
            text (str): The text to be converted.

        Returns:
            int or str: The converted value.
        """
        try:
            return int(float(text))
        except ValueError:
            return text

class TratadorFiltroNomeCodigo(Handler):
    """
    A handler for filtering data based on a column name and a search term.

    Methods:
        process: Filters the data frame based on a column name and a search term.
    """
    def __init__(self, input_queues: list, termo_pesquisa, nome_coluna):
        """
        Initializes the TratadorFiltroNomeCodigo object with input queues, search term, and column name.

        Args:
            input_queues (list): A list of input queues containing data to be filtered.
            termo_pesquisa: The search term for filtering.
            nome_coluna: The name of the column to perform filtering.
        """
        super().__init__(input_queues)
        self.termo_pesquisa = termo_pesquisa
        self.nome_coluna = nome_coluna

    def process(self, dataframe: DataFrame) -> DataFrame:
        """
        Filters the data frame based on a column name and a search term.

        Args:
            dataframe (DataFrame): The data frame to be filtered.

        Returns:
            DataFrame: The filtered data frame.
        """
        col_index = list(dataframe.columns).index(self.nome_coluna)  # Mudei de 'dataframe._columns' para 'dataframe.columns'
        filtered_rows = [row for row in dataframe.values if row[col_index] == self.termo_pesquisa]  # Mudei de 'dataframe._data' para 'dataframe.values'
        return DataFrame(dict(zip(dataframe.columns, zip(*filtered_rows))))


class TratadorSomaValoresPorFila(Handler):
    """
    A handler for summing values in a specified column across data queues.

    Methods:
        handle: Sums values in a specified column across data queues.
    """
    def __init__(self, inputQueues, coluna):
        """
        Initializes the TratadorSomaValoresPorFila object with input queues and column name.

        Args:
            inputQueues: A list of input queues containing data to be processed.
            coluna: The name of the column to perform summing.
        """
        super().__init__(inputQueues)
        self.coluna = coluna

    def handle(self):
        """
        Sums values in a specified column across data queues.
        """
        for i, input_queue in enumerate(self.inputQueues):
            total_sum = 0
            while not input_queue.is_empty():
                df = input_queue.dequeue()
                col_values = df.get_column(self.coluna)  
                total_sum += sum(col_values)  
            self.outputQueues[i].enqueue(DataFrame({"Queue_Total_Sum": [total_sum]}))




class TratadorSomaTotalEntreFilas(Handler):
    """
    A handler for summing total values across multiple data queues.

    Methods:
        handle: Sums total values across multiple data queues.
    """
    def __init__(self, inputQueues):
        """
        Initializes the TratadorSomaTotalEntreFilas object with input queues.

        Args:
            inputQueues (list): A list of input queues containing data to be processed.
        """
        super().__init__(inputQueues)
        self.outputQueues = [Queue() for _ in inputQueues]

    def handle(self):
        """
        Sums total values across multiple data queues.
        """
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
    """
    A handler for unifying data frames from a single queue.

    Methods:
        handle: Unifies data frames from a single queue.
        _unify_dataframes: Unifies data frames from a single queue.
    """
    def handle(self):
        """
        Unifies data frames from a single queue.
        """
        for i, input_queue in enumerate(self.inputQueues):
            unified_dataframe = self._unify_dataframes(input_queue)
            self.outputQueues[i].enqueue(unified_dataframe)


    def _unify_dataframes(self, queue):
        """
        Unifies data frames from a single queue.

        Args:
            queue: The queue containing data frames to be unified.

        Returns:
            DataFrame: The unified data frame.
        """
        unified_data = {}
        while not queue.is_empty():
            df = queue.dequeue()
            for col in df.columns:
                if col not in unified_data:
                    unified_data[col] = []
                unified_data[col].extend(df.get_column(col))
        return DataFrame(unified_data)



class TratadorUnificarDataFramesPorColunas(Handler):
    """
    A handler for unifying data frames based on common columns from multiple queues.

    Methods:
        handle: Unifies data frames based on common columns from multiple queues.
        _unify_dataframes: Unifies data frames based on common columns from multiple queues.
        _get_common_columns: Retrieves common columns from multiple data frames.
    """
    def handle(self):
        """
        Unifies data frames based on common columns from multiple queues.
        """
        unified_dataframes = self._unify_dataframes()
        output_queues = [Queue() for _ in range(len(unified_dataframes))]
        for i, unified_df in enumerate(unified_dataframes):
            output_queues[i].enqueue(unified_df)
        return output_queues

    def _unify_dataframes(self):
        """
        Unifies data frames based on common columns from multiple queues.

        Returns:
            list: A list of unified data frames.
        """
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
        """
        Retrieves common columns from multiple data frames.

        Returns:
            list: A list of common columns.
        """
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







