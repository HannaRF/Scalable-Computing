import sqlite3, os
from abc import ABC, abstractmethod
from DataFrame import DataFrame
from mocks.generate_data import generate_followers_data

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
        self.path = path
        self.data = []
        self.header = []

    def read(self):
        # read DB file
        conn = sqlite3.connect(self.path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM artistas")
        self.header = [col[0] for col in cursor.description]
        self.data = cursor.fetchall()
        conn.close()

    def convert(self):
        # convert data to DataFrame object
        converted_data = {col: [] for col in self.header}

        for row in self.data:
            for col, value in zip(self.header, row):
                converted_data[col].append(value)

        self.data = DataFrame(converted_data)


class DataRepoMemoria(DataRepo):
    def __init__(self, dict_followers_data):
        self.data = dict_followers_data

    def read(self):
        # read API
        pass

    def convert(self):
        # convert data to DataFrame object
        pass


if __name__ == "__main__":

    # # csv
    # for file in os.listdir("mocks/csvs"):

    #     if file.endswith(".csv"):
    #         data_repo = DataRepoCSV(f"mocks/{file}")
    #         data_repo.read()
    #         data_repo.convert()
    #         print(data_repo.data)

    # # db

    # data_repo = DataRepoDB(f"mocks/artistas.db")
    # data_repo.read()
    # data_repo.convert()
    # print(data_repo.data)

    # memoria

    cursor = connect()
    dict_followers_data = generate_followers_data(cursor)
    cursor.connection.close()

    data_repo = DataRepoMemoria(dict_followers_data)
    data_repo.read()
    data_repo.convert()
    print(data_repo.data)

