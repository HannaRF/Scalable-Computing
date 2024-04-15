from abc import ABC, abstractmethod
import sqlite3

class DataRepo(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def read(self):
        pass


class DataRepoCSV(DataRepo):
    def __init__(self, path):
        self.path = path

    def read(self):
        # read csv file using elementary python
        with open(self.path, 'r', encoding='utf-8') as file:
            data = file.read()
        return data
    

class DataRepoDB(DataRepo):
    def __init__(self, path):
        self.path = path

    def read(self):
        # read DB file using elementary python
        conn = sqlite3.connect(self.path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM artistas")
        data = cursor.fetchall()
        conn.close()
        return data
        
    
if __name__ == "__main__":
    data = DataRepoDB("mocks/artist_database.db")
    print(data.read())