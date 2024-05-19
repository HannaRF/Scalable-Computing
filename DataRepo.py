import sqlite3, os
from abc import ABC, abstractmethod
from DataFrame import DataFrame
from mocks.generate_data import generateData

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
        self.table_names = self.get_table_names()
        self.data = []
        self.header = []

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
        self.header = [col[0] for col in self.data.description]
        conn.close()
        return self.data

    def convert(self):
        # convert data to DataFrame object
        converted_data = {col: [] for col in self.header}

        for row in self.data:
            for col, value in zip(self.header, row):
                converted_data[col].append(value)

        self.data = DataFrame(converted_data)


class DataFrameToDB:
    def __init__(self, path):
        self.path = path
        if not os.path.exists(self.path):
            self._create_database()

    def _create_database(self):
        """
        Create a new SQLite database file.
        """
        conn = sqlite3.connect(self.path)
        conn.close()

    def save(self, df, table_name):
        """
        Save a DataFrame object to a SQLite database table.

        Args:
            df (DataFrame): DataFrame object to save.
            table_name (str): Name of the table where data will be saved.
        """
        # Connect to the SQLite database
        conn = sqlite3.connect(self.path)
        cursor = conn.cursor()

        # Create table with appropriate columns
        columns = df.columns
        columns_def = ", ".join([f"{col} TEXT" for col in columns])
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def})")

        # Insert data into the table
        for row in df.values:
            placeholders = ", ".join(["?" for _ in range(len(columns))])
            cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", tuple(row))

        # Commit and close the connection
        conn.commit()
        conn.close()

class DataRepoMemoria(DataRepo):
    def __init__(self):
        self.data = None

    def read(self, data=None):
        # cursor = connect()
        # self.data = generate_followers_data(cursor)
        # cursor.connection.close()

        #self.header = list(self.data[0].keys())\
        self.data = data
        

    def convert(self):
        # convert data to DataFrame object
        cols = self.data[0].keys()
    
        converted_data = {col: [] for col in cols}

        for row in self.data:
            for col in cols:
                converted_data[col].append(row[col])

        self.data = DataFrame(converted_data)

def extract_csv_data():
    data = []

    for file in os.listdir("mocks/data"):

        if file.endswith(".csv"):
            data_repo = DataRepoCSV(f"mocks/data/{file}")
            data_repo.read()
            data_repo.convert()

            data.append(data_repo.data)
    
    return data


def extract_db_data(data):

    for file in os.listdir("mocks"):

        if file.endswith(".db"):
            data_repo = DataRepoDB(f"mocks/{file}")
            data_repo.read("followers")
            data_repo.convert()
            print(file)
            print(data_repo.data)

            data[file] = data_repo.data 
    
    return data

def extract_memory_data(data):

    data_repo = DataRepoMemoria()
    data_repo.read(data)
    data_repo.convert()
    
    return data_repo.data

if __name__ == "__main__":
    pass
    # generateData().run()

    # # csv
    # data = extract_csv_data()
    
    # # db

    # data = extract_db_data()

    # memoria

    # data_repo = DataRepoMemoria()
    # data_repo.read()
    # data_repo.convert()
    # print(data_repo.data)

