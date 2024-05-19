from concurrent import futures
import signal
import grpc
import notifications_pb2
import notifications_pb2_grpc
from DataRepo import extract_csv_data, extract_memory_data


class NotificationServiceServicer(notifications_pb2_grpc.NotificationServiceServicer):
    def SendNotifications(self, request, context):

        data = []
        data_type = request.notification_header.data_type

        for notification in request.notification_list.notifications:

            notification_data = []
            for valor, tipo in zip(notification.data, data_type):
                # Supondo que o index corresponde à ordem dos campos no seu dicionário original
                notification_data.append(eval(f"{tipo}({repr(valor)})"))

            dictionary = dict(zip(request.notification_header.data_header , notification_data))
            data.append(dictionary)

        #print(f"Received data. id: {request.id}")

        return notifications_pb2.Empty()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    notifications_pb2_grpc.add_NotificationServiceServicer_to_server(NotificationServiceServicer(), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    print("Server started on port 50052")

    # Adiciona tratamento de sinal para encerrar o servidor
    signal.signal(signal.SIGINT, lambda sig, frame: stop_server(server))

    server.wait_for_termination()

def stop_server(server):
    print("Stopping server...")
    server.stop(0)
    print("Server stopped")

if __name__ == '__main__':
    serve()
