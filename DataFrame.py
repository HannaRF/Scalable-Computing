import numpy as np

class DataFrame:
    def __init__(self, data):
        """Classe start 

        Args:
            data (dict): Dictionary with keys as the columns names and values as columns. Defaults to None.
        """
        self.__data = data

        self.__colunas = np.array(list(data.keys()))

        valores = []
        for coluna in data.values():
            valores_coluna = np.array(coluna, dtype=object)
            valores.append(valores_coluna)
        self.__valores = np.column_stack(valores)

    @property
    def columns(self):
        return self.__colunas
    
    @property
    def values(self):
        return self.__valores
    
    def add_row(self, row):
        self.__valores = np.vstack((self.__valores, np.array(row, dtype=object)))

    def add_column(self, column):
        """Add column or columns to the DataFrame.

        Args:
            column (dict): Dictionary with keys as column names and values as the columns' 
        """

        self.__colunas = np.append(self.__colunas,list(column.keys()))

        cols = []
    
        for col in column.values():
            valores_coluna = np.array(col)
            cols.append(valores_coluna)
        new_cols = np.column_stack(cols)
        self.__valores = np.hstack((self.__valores, new_cols))

    def get_row(self, index):
        return self.__valores[index]

    def get_column(self, name):
        index = np.where(self.columns == name)[0][0]
        return self[:,index].copy().astype(type(self[0,index]))

    def __str__(self):
        colunas_str = '\t'.join(self.__colunas)
        valores_str = ''
        for i, linha in enumerate(self.values):
            valores_linha = '\t'.join(map(str, linha))
            valores_str += str(i+1) + '\t' + valores_linha + '\n'  
        return f"\t{colunas_str}\n{valores_str}"
    
    def __getitem__(self, item):
        """Get item from the database

        Args:
            item (tuple): tuple of max size 2 in which the first element corresponds to the row 
                          and the second to the column. 
        """
        return self.values[item]

# Exemplo de uso:
data = {
    "Name": ["Alice", "Bob", "Charlie"],
    "Age": [30, 35, 25],
    "Occupation": ["Engineer", "Developer", "Designer"]
}

df = DataFrame(data)
print(df)

# Adicionar uma nova linha
df.add_row(["David", 40, "Manager "])
#print(df)

# # Adicionar uma nova coluna
df.add_column({"Location": ["New York", "San Francisco", "Los Angeles", "Chicago"]})
print(df)

# # Acessar linha e coluna
print(df.get_row(1))
print(df.get_column("Name"))