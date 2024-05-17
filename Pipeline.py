from mocks.generate_data import *
from DataRepo import extract_csv_data, extract_memory_data
from Handler import TratadorLimpezaCSV, TratadorFiltroNomeCodigo, TratadorSomaValoresPorFila, TratadorSomaTotalEntreFilas, TratadorUnificarDataFrames
from Queue import Queue
from multiprocessing import Process

def main():


    # Simulate
    simulation = generateData()
    simulation.run()

    # get data
    cade_analytics_data = simulation.data

    # Extract data
    orders, products, storage, users = extract_csv_data()
    requests = extract_memory_data(cade_analytics_data)

    # Start Queue
    queue = Queue()
    for data_frame in [orders, products, storage, users, requests]:
        queue.enqueue(data_frame)
    
    # Start Handler

    process = Process(target=TratadorLimpezaCSV, args=(queue,))



    

if __name__ == "__main__":
    main()
