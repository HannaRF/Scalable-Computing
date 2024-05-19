import threading
import subprocess
import random
import time

def call_client(numero1, numero2, index, tempos):

    # Registra o tempo de início
    start_time = time.time()

    # Define o comando para chamar o programa de soma com os números como argumentos
    comando = ["python", "client.py", str(numero1), str(numero2), str(index)]

    # Executa o comando
    processo = subprocess.Popen(comando, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    saida, erro = processo.communicate()

    # Registra o tempo de término
    end_time = time.time()

    # Calcula a duração da chamada e a adiciona à lista de tempos
    tempos[index] = (end_time - start_time)

def call_server():
    # Define o comando para chamar o programa de soma com os números como argumentos
    comando = ["python", "server.py"]

    # Executa o comando
    processo = subprocess.Popen(comando, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    return processo

def stop_server(server):
    # Define o comando para encerrar o servidor
    comando = ["taskkill", "/F", "/T", "/PID", str(server.pid)]

    # Executa o comando para encerrar o servidor
    subprocess.run(comando)

    saida, erro = server.communicate()
    print("Saída:", saida.decode())


def main():

    # Inicia o servidor em uma thread separada
    server = call_server()

    n = 20
    tempos = [None] * n

    # Cria uma lista de parâmetros com números aleatórios
    parametros = []
    for i in range(n):
        numero1 = 50    # Número de dados do cliente i
        numero2 = 0     # Tempo de espera entre dados do cliente i
        parametros.append((numero1, numero2, i, tempos))
    

    print("Iniciando simulação...\n")
    # Cria uma thread para cada conjunto de parâmetros
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

    print("Encerrando servidor...")
    # Encerra o servidor
    stop_server(server)

    duracao_media = sum(tempos) / len(tempos)
    print("Duração média de cada chamada:", duracao_media, "segundos")
    print("Tempo   total   de   execução:", execution_time, "segundos")
    # Cada cliente envia 50 dados(numero1) CadeAnalytics com 200 linhas(num_requests) para o servidor

if __name__ == "__main__":
    try:
        main()  # Chama a função main
    except KeyboardInterrupt:
        print("Fim da simulação")
