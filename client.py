import threading
import time
import grpc
import notifications_pb2
import notifications_pb2_grpc
from mocks.generate_data import CadeAnalyticsMock
import random
import sys


class ClientThread(threading.Thread):
    def __init__(self, thread_id, index):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.index = index

    def run(self):

        # Gerando dados
        num_users = random.randint(1, 200)
        num_products = random.randint(1, 200)
        num_requests = random.randint(1, 200)

        cade_analytics = CadeAnalyticsMock(num_users, num_products)
        data = cade_analytics.get_data(num_requests)

        with grpc.insecure_channel('localhost:50052') as channel:
            stub = notifications_pb2_grpc.NotificationServiceStub(channel)

            id = self.thread_id + (self.index+1)*1000
            
            notification_list = notifications_pb2.NotificationList(
                notifications=[
                    notifications_pb2.Notification(data=[str(value) for value in notification.values()]) for notification in data
                ]
            )

            line_1 = list(data[0].values())

            keys = list(data[0].keys())
            tipos = [type(elemento).__name__ for elemento in line_1]

            notification_header = notifications_pb2.NotificationHeader(data_header = keys, data_type = tipos)
            send_notifications_request = notifications_pb2.SendNotificationsRequest(notification_list = notification_list,
                                                                                    notification_header = notification_header,
                                                                                    id = id)
            
            response = stub.SendNotifications(send_notifications_request)
            #print(f"Notifications sent successfully. Id - {id}")


def run_simulation(max_clients, wait_time, index):
    threads = []
    for i in range(1, max_clients + 1):
        thread = ClientThread(i, index)
        threads.append(thread)
        thread.start()
        time.sleep(wait_time)  # Espera o tempo especificado antes de iniciar a próxima iteração
    for thread in threads:
        thread.join()

if __name__ == '__main__':
    if len(sys.argv) == 3:
        max_clients = int(sys.argv[1])
        wait_time = int(sys.argv[2])  # Especifica em wait_time segundos, a espera entre cada iteração
        index = int(sys.argv[3])
        run_simulation(max_clients, wait_time, index)
    else:
        run_simulation(20, 2, 0)

