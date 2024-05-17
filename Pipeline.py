from mocks.generate_data import generateData
from DataRepo import extract_csv_data#, extract_db_data
from Handler import TratadorLimpezaCSV, TratadorFiltroNomeCodigo, TratadorSomaValoresPorFila, TratadorSomaTotalEntreFilas, TratadorUnificarDataFrames
from Queue import Queue

def main():


    # Simulate
    generateData(num_cycles = 1, secs_between_cycles = 1).run()

    # Extract

    data = extract_csv_data()
    for key in data.keys():
        locals()[key] = data[key]
        
    # data2 = extract_db_data()

    # Criar filas de entrada
    input_queue1 = Queue()
    input_queue2 = Queue()
    input_queue1.enqueue(df1)
    input_queue2.enqueue(df2)
    input_queues = [input_queue1, input_queue2]

    # Passar pelos tratadores
    # Limpeza CSV
    tratador_limpeza_csv = TratadorLimpezaCSV(input_queues)
    tratador_limpeza_csv.handle()

    # Filtragem pelo Código Artista = 3
    tratador_filtro_nome_codigo = TratadorFiltroNomeCodigo(tratador_limpeza_csv.outputQueues, 3, "Código Artista")
    tratador_filtro_nome_codigo.handle()

    # Soma de valores por fila
    tratador_soma_valores_por_fila = TratadorSomaValoresPorFila(tratador_filtro_nome_codigo.outputQueues, "Valor")
    tratador_soma_valores_por_fila.handle()

    # Soma total entre filas
    tratador_soma_total_entre_filas = TratadorSomaTotalEntreFilas(tratador_soma_valores_por_fila.outputQueues)
    tratador_soma_total_entre_filas.handle()

    # Unificação de DataFrames
    tratador_unificar_dataframes = TratadorUnificarDataFrames(input_queues)
    tratador_unificar_dataframes.handle()

    # Exibir os resultados
    df_limpo1 = tratador_limpeza_csv.outputQueues[0].dequeue()
    df_limpo2 = tratador_limpeza_csv.outputQueues[1].dequeue()
    df_filtrado1 = tratador_filtro_nome_codigo.outputQueues[0].dequeue()
    df_filtrado2 = tratador_filtro_nome_codigo.outputQueues[1].dequeue()
    df_soma1 = tratador_soma_valores_por_fila.outputQueues[0].dequeue()
    df_soma2 = tratador_soma_valores_por_fila.outputQueues[1].dequeue()
    df_soma_total = tratador_soma_total_entre_filas.outputQueues[0].dequeue()
    df_unificado1 = tratador_unificar_dataframes.outputQueues[0].dequeue()
    df_unificado2 = tratador_unificar_dataframes.outputQueues[1].dequeue()

    # Imprimir os resultados
    print("DataFrame limpo 1:")
    print(df_limpo1)
    print("\nDataFrame limpo 2:")
    print(df_limpo2)
    print("\nDataFrame filtrado 1:")
    print(df_filtrado1)
    print("\nDataFrame filtrado 2:")
    print(df_filtrado2)
    print("\nSoma de valores por fila 1:")
    print(df_soma1)
    print("\nSoma de valores por fila 2:")
    print(df_soma2)
    print("\nSoma total entre filas:")
    print(df_soma_total)
    print("\nDataFrame unificado 1:")
    print(df_unificado1)
    print("\nDataFrame unificado 2:")
    print(df_unificado2)

    # Start trigger (e.g., every 60 seconds)
    # trigger = TimerTrigger(60, pipeline)
    # trigger.start()

    # Initialize dashboard
    # dashboard = Dashboard()

    # Main loop to update dashboard metrics
    while True:
        pass
        # dashboard.update_metrics(pipeline.queue.dequeue())
        # dashboard.display()

if __name__ == "__main__":
    main()
