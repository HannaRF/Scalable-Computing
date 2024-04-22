from abc import ABC, abstractmethod
import sqlite3
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
        self.data = None
        self.columns = None

    def read(self):
        # read csv file using elementary python
        with open(self.path, 'r', encoding='utf-8') as file:
            data = file.read()
        self.data = data
    
    def convert(self):
        # convert data to dataframe
        self.data = self.data.split('\n')
        self.columns = self.data[0].split(',')
        converted_data = dict()

        for i in range(len(self.columns)):
            converted_data[self.columns[i]] = [row.split(',')[i] for row in self.data[1:]]
        
        self.data = DataFrame(converted_data)


class DataRepoDB(DataRepo):
    def __init__(self, path):
        self.path = path
        self.data = None
        self.columns = None

    def read(self):
        # read DB file using elementary python and get colum names
        conn = sqlite3.connect(self.path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM artistas")
        data = cursor.fetchall()
        conn.close()
        self.data = data

        # column names
        conn = sqlite3.connect(self.path)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(artistas)")
        columns = cursor.fetchall()
        conn.close()
        self.columns = [col[1] for col in columns]
    
    def convert(self):
        # convert data to dataframe
        data_converted = dict()
        
        for i in range(len(self.columns)): 
            data_converted[self.columns[i]] = [row[i] for row in self.data]

        self.data = DataFrame(data_converted)

        
    
if __name__ == "__main__":

    # data = DataRepoDB("mocks/artist_database.db")
    # data.read()
    # data.convert()
    # print(data.data)


    data = DataRepoCSV("mocks/revenue_data.csv")
    data.read()
    data.convert()
    print(data.data)