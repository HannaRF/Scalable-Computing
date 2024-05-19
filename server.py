from concurrent import futures
import signal
import grpc
import notifications_pb2
import notifications_pb2_grpc
from DataRepo import extract_csv_data, extract_memory_data
from Pipeline_new import main as pipeline
from Queue import *
from DataRepo import *
from multiprocessing import Process

class NotificationServiceServicer(notifications_pb2_grpc.NotificationServiceServicer):
    def __init__(self, queue):
        self.queue = queue

    def SendNotifications(self, request, context):
        data = []
        data_type = request.notification_header.data_type

        for notification in request.notification_list.notifications:

            notification_data = []
            for valor, tipo in zip(notification.data, data_type):
                notification_data.append(eval(f"{tipo}({repr(valor)})"))

            dictionary = dict(zip(request.notification_header.data_header , notification_data))
            data.append(dictionary)

        process_data(data, request.id)        

        return notifications_pb2.Empty()
    
def process_data(data, request_id):
    dataframe = extract_memory_data(data)
    database_id = str(int(request_id / 1000)).zfill(2)
    database = DataFrameToDB(f"mocks/data/database_{database_id}.sqlite")
    database.save(dataframe, f"Tabela{request_id}")


def serve():

    queue = Queue()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    notifications_pb2_grpc.add_NotificationServiceServicer_to_server(NotificationServiceServicer(queue), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    try:
        print("Server started on port 50052")
    except:
        pass

    # Adiciona tratamento de sinal para encerrar o servidor
    signal.signal(signal.SIGINT, lambda sig, frame: stop_server(server))
    server.wait_for_termination()

def stop_server(server):
    print("Stopping server...")
    server.stop(0)
    print("Server stopped")

if __name__ == '__main__':
    serve()
