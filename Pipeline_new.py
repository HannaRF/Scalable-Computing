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
    products_viewed_last_minute = sum(1 for timestamp, produto in zip(timestamps, produtos) if timestamp > one_minute_ago)
    products_viewed_last_minute = set(produtos)

    # Enviando o resultado para a fila de resultados
    result_queue.put(('products_viewed_per_minute', products_viewed_last_minute))


# def calculate_products_bought_per_minute(orders, result_queue):
#     now = datetime.now()
#     one_minute_ago = now - timedelta(minutes=1)
#     products_bought_last_minute = sum(int(order['quantity']) for order in orders if order['created_at'] > one_minute_ago)
#     result_queue.put(('products_bought_per_minute', products_bought_last_minute))

# def calculate_unique_users_per_product_per_minute(requests, result_queue):
#     now = datetime.now()
#     one_minute_ago = now - timedelta(minutes=1)
#     user_views_per_product = {}
#     for req in requests:
#         if req['action'] == 'view' and req['timestamp'] > one_minute_ago:
#             product_id = req['product_id']
#             user_id = req['user_id']
#             if product_id not in user_views_per_product:
#                 user_views_per_product[product_id] = set()
#             user_views_per_product[product_id].add(user_id)
#     unique_user_views_per_product = {product_id: len(user_ids) for product_id, user_ids in user_views_per_product.items()}
#     result_queue.put(('unique_users_per_product_per_minute', unique_user_views_per_product))

# def calculate_most_bought_products_last_hour(orders, result_queue):
#     now = datetime.now()
#     one_hour_ago = now - timedelta(hours=1)
#     products_bought_last_hour = {}
#     for order in orders:
#         if order['created_at'] > one_hour_ago:
#             product_id = order['product_id']
#             if product_id not in products_bought_last_hour:
#                 products_bought_last_hour[product_id] = 0
#             products_bought_last_hour[product_id] += int(order['quantity'])
#     most_bought_products_last_hour = sorted(products_bought_last_hour.items(), key=lambda x: x[1], reverse=True)
#     result_queue.put(('most_bought_products_last_hour', most_bought_products_last_hour))

# def calculate_most_viewed_products_last_hour(requests, result_queue):
#     now = datetime.now()
#     one_hour_ago = now - timedelta(hours=1)
#     products_viewed_last_hour = {}
#     for req in requests:
#         if req['action'] == 'view' and req['timestamp'] > one_hour_ago:
#             product_id = req['product_id']
#             if product_id not in products_viewed_last_hour:
#                 products_viewed_last_hour[product_id] = 0
#             products_viewed_last_hour[product_id] += 1
#     most_viewed_products_last_hour = sorted(products_viewed_last_hour.items(), key=lambda x: x[1], reverse=True)
#     result_queue.put(('most_viewed_products_last_hour', most_viewed_products_last_hour))

# def calculate_avg_views_before_purchase(requests, orders, result_queue):
#     views_before_purchase = {}
#     for req in requests:
#         if req['action'] == 'view':
#             product_id = req['product_id']
#             if product_id not in views_before_purchase:
#                 views_before_purchase[product_id] = 0
#             views_before_purchase[product_id] += 1
#     purchases = {order['product_id']: int(order['quantity']) for order in orders}
#     avg_views_before_purchase = {product_id: views_before_purchase.get(product_id, 0) / purchases.get(product_id, 1) for product_id in views_before_purchase}
#     result_queue.put(('avg_views_before_purchase', avg_views_before_purchase))

# def calculate_products_sold_without_stock(orders, storage, result_queue):
#     products_sold_without_stock = 0
#     for order in orders:
#         product_id = order['product_id']
#         quantity_ordered = int(order['quantity'])
#         stock = next((item['quantity'] for item in storage if item['product_id'] == product_id), 0)
#         if quantity_ordered > stock:
#             products_sold_without_stock += 1
#     result_queue.put(('products_sold_without_stock', products_sold_without_stock))

def main():
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

    while True:
        result_queue = Queue()

        processes = [
            Process(target=calculate_products_viewed_per_minute, args=(requests, result_queue)),
            # Process(target=calculate_products_bought_per_minute, args=(orders, result_queue)),
            # Process(target=calculate_unique_users_per_product_per_minute, args=(requests, result_queue)),
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

        # Printing the metrics
        print(f"DashBoard:")
        print(f"Number of products viewed per minute: {results.get('products_viewed_per_minute')}")
        # print(f"Number of products bought per minute: {results.get('products_bought_per_minute')}")
        # print(f"Unique users viewing each product per minute: {results.get('unique_users_per_product_per_minute')}")
        # print(f"Most bought products in the last hour: {results.get('most_bought_products_last_hour')}")
        # print(f"Most viewed products in the last hour: {results.get('most_viewed_products_last_hour')}")
        # print(f"Average number of views before purchase: {results.get('avg_views_before_purchase')}")
        # print(f"Number of products sold without stock availability: {results.get('products_sold_without_stock')}")
        print("\n")

        time.sleep(0.5)

if __name__ == "__main__":
    main()
