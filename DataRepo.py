import sqlite3
from abc import ABC, abstractmethod
from DataFrame import DataFrame

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


if __name__ == "__main__":
    # Example usage
    data_repo = DataRepoDB("mocks/artist_database.db")
    data_repo.read()
    data_repo.convert()
    print(data_repo.data)

    data_repo = DataRepoCSV("mocks/revenue_data.csv")
    data_repo.read()
    data_repo.convert()
    print(data_repo.data)
