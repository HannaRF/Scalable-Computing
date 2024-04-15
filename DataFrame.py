class DataFrame:
    def __init__(self, data=None, columns=None):
        if data is None:
            data = []
        self._data = data
        self._columns = columns if columns else []

    def add_row(self, row):
        self._data.append(row)

    def add_column(self, name, values):
        if len(values) != len(self._data):
            raise ValueError("Número de valores não corresponde ao número de linhas")
        self._columns.append(name)
        for i, value in enumerate(values):
            if len(self._data[i]) < len(self._columns):
                self._data[i].append(value)
            else:
                raise ValueError("Número de valores não corresponde ao número de linhas")

    def get_row(self, index):
        return self._data[index]

    def get_column(self, name):
        index = self._columns.index(name)
        return [row[index] for row in self._data]

    def __str__(self):
        column_widths = [max(len(str(row[i])) for row in self._data) for i in range(len(self._columns))]
        output = " | ".join(name.ljust(width) for name, width in zip(self._columns, column_widths)) + "\n"
        output += "-" * sum(column_widths) + "\n"
        for row in self._data:
            output += " | ".join(str(cell).ljust(width) for cell, width in zip(row, column_widths)) + "\n"
        return output

# # Exemplo de uso:
# data = [
#     ["Alice", 30, "Engineer"],
#     ["Bob", 35, "Developer"],
#     ["Charlie", 25, "Designer"]
# ]

# df = DataFrame(data, columns=["Name", "Age", "Occupation"])
# print(df)

# # Adicionar uma nova linha
# df.add_row(["David", 40, "Manager"])
# print(df)

# # Adicionar uma nova coluna
# df.add_column("Location", ["New York", "San Francisco", "Los Angeles", "Chicago"])
# print(df)

# # Acessar linha e coluna
# print(df.get_row(1))
# print(df.get_column("Name"))

# for row in df._data:
#     print(row)
