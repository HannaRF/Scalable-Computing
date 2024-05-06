import sqlite3, os
from abc import ABC, abstractmethod
from DataFrame import DataFrame
from mocks.generate_data import generate_followers_data, connect

class DataRepo(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def read(self):
        pass


class DataRepoCSV(DataRepo):
    def __init__(self, path):
        self.path = path
        self.data = []
        self.header = []

    def read(self):
        # read csv file
        with open(self.path, 'r', encoding='utf-8') as file:
            self.data = file.readlines()

    def convert(self):
        # convert data to DataFrame object
        self.header = self.data[0].strip().split(',')
        data_rows = [row.strip().split(',') for row in self.data[1:]]
        converted_data = {col: [] for col in self.header}

        for row in data_rows:
            for col, value in zip(self.header, row):
                converted_data[col].append(value)

        self.data = DataFrame(converted_data)


class DataRepoDB(DataRepo):
    def __init__(self, path):
        self.path = path +"/"+ self.get_db_name()
        self.table_names = self.get_table_names()
        self.data = []
        self.header = []

    def get_db_name(self):
        return [file for file in os.listdir(self.path) if file.endswith(".db")][0]

    def get_table_names(self):
        conn = sqlite3.connect(self.path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        table_names = cursor.fetchall()
        conn.close()
        return [name[0] for name in table_names]

    def read(self, table_name):
        # read DB file
        conn = sqlite3.connect(self.path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        self.data = cursor.fetchall()
        conn.close()
        return self.data

    def convert(self):
        # convert data to DataFrame object
        self.header = [col[0] for col in self.data.description]
        converted_data = {col: [] for col in self.header}

        for row in self.data:
            for col, value in zip(self.header, row):
                converted_data[col].append(value)

        self.data = DataFrame(converted_data)

class DataRepoMemoria(DataRepo):
    def __init__(self):
        self.data = None

    def read(self):
        cursor = connect()
        self.data = generate_followers_data(cursor)
        cursor.connection.close()

        self.header = list(self.data[0].keys())

    def convert(self):
        # convert data to DataFrame object
    
        converted_data = {col: [] for col in self.header}

        for row in self.data:
            for col in self.header:
                converted_data[col].append(row[col])

        self.data = DataFrame(converted_data)


if __name__ == "__main__":

    # # csv
    # for file in os.listdir("mocks/csvs"):

    #     if file.endswith(".csv"):
    #         data_repo = DataRepoCSV(f"mocks/{file}")
    #         data_repo.read()
    #         data_repo.convert()
    #         print(data_repo.data)

    # # db

    data_repo = DataRepoDB(f"mocks/")
    data_repo.read()
    data_repo.convert()
    print(data_repo.data)

    # memoria

    # data_repo = DataRepoMemoria()
    # data_repo.read()
    # data_repo.convert()
    # print(data_repo.data)

