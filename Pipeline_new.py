import time
from datetime import datetime, timedelta
from multiprocessing import Process, Queue
from mocks.generate_data import generateData
from DataRepo import extract_csv_data, extract_memory_data
from Handler import TratadorLimpezaCSV, TratadorFiltroNomeCodigo, TratadorSomaValoresPorFila, TratadorSomaTotalEntreFilas, TratadorUnificarDataFrames
from datetime import datetime, timedelta

def calculate_products_viewed_per_minute(requests_df, result_queue):
    now = datetime.now()
    one_minute_ago = now - timedelta(minutes=1)
    
    # Acessando os dados do DataFrame
    timestamps = requests_df.get_column('data_notificacao')
    produtos = requests_df.get_column('componente_alvo')
    
    # filtrar pelo último minuto
    products_viewed_last_minute = sum(1 for timestamp, produto in zip(timestamps, produtos) if timestamp > one_minute_ago and "PRODUTO" in produto)
    products_viewed_last_minute = len(set(produtos))

    # Enviando o resultado para a fila de resultados
    result_queue.put(('products_viewed_per_minute', products_viewed_last_minute))


def calculate_products_bought_per_minute(orders_df, result_queue):
    now = datetime.now()
    one_minute_ago = now - timedelta(minutes=1)

    # Acessando os dados do DataFrame
    created_at = orders_df.get_column('created_at')
    quantities = orders_df.get_column('quantity')
    
    # Convertendo timestamps e quantidades para os tipos adequados
    quantities = [int(q) for q in quantities]

    # Contagem dos produtos comprados no último minuto
    products_bought_last_minute = sum(q for ts, q in zip(created_at, quantities) if ts > one_minute_ago)
    
    # Enviando o resultado para a fila de resultados
    result_queue.put(('products_bought_per_minute', products_bought_last_minute))

def calculate_unique_users_per_product_per_minute(requests_df, result_queue):
    now = datetime.now()
    one_minute_ago = now - timedelta(minutes=1)

    # Acessando os dados do DataFrame
    # actions = requests_df.get_column('action')
    timestamps = requests_df.get_column('data_notificacao')
    product_ids = requests_df.get_column('componente_alvo')
    user_ids = requests_df.get_column('id_usuario')

    # Convertendo timestamps para datetime
    # timestamps = [datetime.strptime(ts, '%Y-%m-%d %H:%M:%S') for ts in timestamps]

    user_views_per_product = {}
    for ts, product_id, user_id in zip(timestamps, product_ids, user_ids):
        if ("PRODUTO" in product_id) and (ts > one_minute_ago):
            if product_id not in user_views_per_product:
                user_views_per_product[product_id] = set()
            user_views_per_product[product_id].add(user_id)
    
    # count unique users per product
    unique_user_views_per_product = {product_id: len(users) for product_id, users in user_views_per_product.items()}
    result_queue.put(('unique_users_per_product_per_minute', unique_user_views_per_product))


def main():
    while True:
        # Simulate data
        simulation = generateData()
        simulation.run()

        # Get data
        cade_analytics_data = simulation.data

        # Extract data from CSVs and memory
        orders, products, storage, users = extract_csv_data()
        requests = extract_memory_data(cade_analytics_data)

        # Convert datetime strings to datetime objects for comparison
        for order in orders:
            # 4 : created_at
            order[4] = datetime.strptime(order[4], '%Y-%m-%d')
        for req in requests:
            # 0 : data_notificacao
            req[0] = datetime.strptime(req[0], '%Y-%m-%d %H:%M:%S')

        result_queue = Queue()

        processes = [
            Process(target=calculate_products_viewed_per_minute, args=(requests, result_queue)),
            Process(target=calculate_products_bought_per_minute, args=(orders, result_queue)),
            Process(target=calculate_unique_users_per_product_per_minute, args=(requests, result_queue)),
            # Process(target=calculate_most_bought_products_last_hour, args=(orders, result_queue)),
            # Process(target=calculate_most_viewed_products_last_hour, args=(requests, result_queue)),
            # Process(target=calculate_avg_views_before_purchase, args=(requests, orders, result_queue)),
            # Process(target=calculate_products_sold_without_stock, args=(orders, storage, result_queue))
        ]

        for process in processes:
            process.start()

        for process in processes:
            process.join()

        results = {}
        while not result_queue.empty():
            key, value = result_queue.get()
            results[key] = value

        #clean terminal
        print("\033c")

        # Printing the metrics
        print(f"DashBoard:")
        print(f"Number of products viewed per minute: {results.get('products_viewed_per_minute')}")
        print(f"Number of products bought per minute: {results.get('products_bought_per_minute')}")
        print(f"Unique users viewing each product per minute: {results.get('unique_users_per_product_per_minute')}")
        # print(f"Most bought products in the last hour: {results.get('most_bought_products_last_hour')}")
        # print(f"Most viewed products in the last hour: {results.get('most_viewed_products_last_hour')}")
        # print(f"Average number of views before purchase: {results.get('avg_views_before_purchase')}")
        # print(f"Number of products sold without stock availability: {results.get('products_sold_without_stock')}")

if __name__ == "__main__":
    main()
