import threading
import subprocess
import random
import time

def call_client(numero1, numero2, index):
    # Define o comando para chamar o programa de soma com os números como argumentos
    comando = ["python", "client.py", str(numero1), str(numero2), str(index)]

    # Executa o comando
    processo = subprocess.Popen(comando, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Captura a saída do programa
    saida, erro = processo.communicate()

    # Imprime a saída e o erro, se houver
    # print("Saída:", saida.decode())
    # print("Erro:", erro.decode())

def call_server():
    # Define o comando para chamar o programa de soma com os números como argumentos
    comando = ["python", "server.py"]

    # Executa o comando
    processo = subprocess.Popen(comando, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Captura a saída do programa
    saida, erro = processo.communicate()

    # Imprime a saída e o erro, se houver
    # print("Saída:", saida.decode())
    # print("Erro:", erro.decode())

def main():

    # Inicia o servidor em uma thread separada
    server_thread = threading.Thread(target=call_server)
    server_thread.start()

    n = 5

    # Cria uma lista de parâmetros com números aleatórios
    parametros = []
    for i in range(n):
        numero1 = random.randint(10, 20)    # Número de dados do cliente i
        numero2 = random.randint(1, 3)      # Tempo de espera entre dados do cliente i
        parametros.append((numero1, numero2, i))

    # Cria uma thread para cada conjunto de parâmetros
    

    print("Iniciando simulação...\n")

    threads = []
    start_time = time.time()  # Tempo inicial
    for parametro in parametros:
        t = threading.Thread(target=call_client, args=parametro)
        threads.append(t)
        t.start()

    # Espera todas as threads terminarem
    for thread in threads:
        thread.join()
    end_time = time.time()  # Tempo final

    execution_time = end_time - start_time  # Calcula o tempo de execução
    print("Tempo de execução:", execution_time, "segundos")
    print("Pressione Ctrl+C para encerrar server.")

    # Aguarda a thread do servidor terminar
    server_thread.join()

if __name__ == "__main__":
    try:
        main()  # Chama a função main
    except KeyboardInterrupt:
        print("Fim da simulação")
