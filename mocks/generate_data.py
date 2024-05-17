from faker import Faker
import random
import pandas as pd
import numpy as np
import time

class ContaVerdeMock:
    def __init__(self):
        self.fake = Faker("pt_BR")
        self.number_of_users = 0
        self.number_of_products = 0
        self.number_of_storage_items = 0
        self.number_of_orders = 0

    def generate_user(self) -> dict:
        self.number_of_users += 1
        return {
            "user_id": self.number_of_users,
            "name": self.fake.first_name(),
            "last_name": self.fake.last_name(),
            "address": self.fake.address().split("\n")[0] + " -" + self.fake.address().split("/")[1],
            "sign_up_date": self.fake.date_this_decade().strftime("%Y-%m-%d"),
            "birth_date": self.fake.date_of_birth(minimum_age=18, maximum_age=65).strftime("%Y-%m-%d"),
        }
    
    def generate_product(self) -> dict:
        self.number_of_products += 1
        return {
            "product_id": self.number_of_products,
            "name": self.fake.word(),
            "image": self.fake.image_url(),
            "description": self.fake.sentence(),
            "price": self.fake.random_number(2),
        }
    
    def generate_storage(self) -> dict:
        self.number_of_storage_items += 1

        assert self.number_of_products > 0, "No products available to store"
        assert self.number_of_storage_items <= self.number_of_products, "Not enough products to store"

        return {
            "product_id": self.number_of_storage_items,  
            "quantity": random.randint(1, 100)
        }
    
    def generate_order(self) -> dict:
        self.number_of_orders += 1

        assert self.number_of_products > 0, "No products available to store"
        assert self.number_of_storage_items > 0, "No storage items available"
        assert self.number_of_users > 0, "No users available"
        assert self.number_of_storage_items <= self.number_of_products, "Not enough products to store"

        created_at = self.fake.date_this_month().strftime("%Y-%m-%d")
        paid_at = self.fake.date_between_dates(date_start=pd.to_datetime(created_at, format="%Y-%m-%d")).strftime("%Y-%m-%d")
        arrived_at = self.fake.date_between(start_date="today", end_date="+30d").strftime("%Y-%m-%d")

        return {
            "order_id": self.number_of_orders,
            "user_id": random.randint(1, self.number_of_users),
            "product_id": random.randint(1, self.number_of_products),
            "quantity": random.randint(1, 10),
            "created_at": created_at,
            "paid_at": paid_at,
            "arrived_at": arrived_at
        }
    
    def generate_users_file(self, number_of_users: int) -> None:
        users = []
        for _ in range(number_of_users):
            users.append(self.generate_user())
        
        pd.DataFrame(users).to_csv("mocks/data/users.csv", index=False)

    def generate_products_file(self, number_of_products: int) -> None:
        products = []
        for _ in range(number_of_products):
            products.append(self.generate_product())
        
        pd.DataFrame(products).to_csv("mocks/data/products.csv", index=False)

    def generate_storage_file(self) -> None:
        storage = []
        for _ in range(self.number_of_products):
            storage.append(self.generate_storage())
        
        pd.DataFrame(storage).to_csv("mocks/data/storage.csv", index=False)

    def generate_orders_file(self, number_of_orders: int) -> None:
        orders = []
        for _ in range(number_of_orders):
            orders.append(self.generate_order())
        
        pd.DataFrame(orders).to_csv("mocks/data/orders.csv", index=False)


class CadeAnalyticsMock(ContaVerdeMock):

    def __init__(self, num_users: int, num_products: int):
        self.events = ["CLICK", "SCROLL", "ZOOM", "DBCLICK"]
        self.fake = Faker()
        self.number_of_users = num_users
        self.number_of_products = num_products
    
    def get_data(self, num_requests):
        data = []
        
        for _ in range(num_requests):
            event_data = {
                "data_notificacao": self.fake.date_time_between(start_date="-60s", end_date="now").strftime("%Y-%m-%d %H:%M:%S"),
                "id_usuario": random.randint(1, self.number_of_users),
                "estimulo": random.choice(self.events),
                "componente_alvo": self.generate_componente_alvo()
            }
            data.append(event_data)
        
        return data
    
    def generate_componente_alvo(self):
        elementos = ["BOTAO", "IMAGEM", "TEXTO", "INPUT", "SELECT"]
        componentes = ["HOME", f"PRODUTO {random.randint(1, self.number_of_products)}", "CARRINHO", "PERFIL", "PAGAMENTO"]
        return f"{random.choice(elementos)} - {np.random.choice(componentes, p=[0.1, 0.65, 0.1, 0.05, 0.1])}"

class generateData:
    def __init__(self):
        self.data = None
    
    def run(self):
        # Generate new files with ContaVerdeMock
        self.conta_verde = ContaVerdeMock()
        self.conta_verde.generate_users_file(random.randint(1, 100))
        self.conta_verde.generate_products_file(random.randint(1, 100))
        self.conta_verde.generate_storage_file()
        self.conta_verde.generate_orders_file(random.randint(1, 100))

        # Generate new data with CadeAnalyticsMock using the number of users and products from ContaVerdeMock
        self.cade_analytics = CadeAnalyticsMock(self.conta_verde.number_of_users, self.conta_verde.number_of_products)
        num_requests = random.randint(1, 200)
        self.data = self.cade_analytics.get_data(num_requests)

        # Wait for the next cycle
        self.number_of_orders = 0
        self.number_of_products = 0
        self.number_of_storage_items = 0
        self.number_of_users = 0

# if __name__ == "__main__":
#     generateData = generateData()
#     generateData.run()
#     print(generateData.data)