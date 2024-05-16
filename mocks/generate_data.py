from faker import Faker
import random
import requests
import pandas as pd
import time

# This class is used to generate fake data for testing purposes
class generate_data:
    def __init__(self, num_cycles=200, secs_between_cycles=10):
        self.fake = Faker("pt_BR")
        self.number_of_users = 0
        self.number_of_products = 0
        self.number_of_storage_items = 0
        self.number_of_orders = 0
        self.num_cycles = num_cycles
        self.secs_between_cycles = secs_between_cycles

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

    def main(self):
        cycles = 0
        while cycles < self.num_cycles:
            self.generate_users_file(random.randint(1, 20))
            self.generate_products_file(random.randint(1, 100))
            self.generate_storage_file()
            self.generate_orders_file(random.randint(1, 200))

            self.number_of_orders = 0
            self.number_of_products = 0
            self.number_of_storage_items = 0
            self.number_of_users = 0

            cycles += 1
            time.sleep(self.secs_between_cycles)

if __name__ == "__main__":
    generate_data(num_cycles=100, secs_between_cycles=5).main()