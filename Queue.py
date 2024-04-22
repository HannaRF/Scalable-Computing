import threading

class Queue:
    def __init__(self):
        self.queue = []
        self.head = 0  # aponta para o primeiro elemento válido da fila
        self.tail = 0  # aponta para a próxima posição livre para inserção
        self.count = 0  # num de elementos na fila
        self.lock = threading.Lock()

    def enqueue(self, dataframe):
        with self.lock:
            if self.tail == len(self.queue):
                self.queue.append(dataframe)  # adiciona no espaço livre ou expande a lista
            else:
                self.queue[self.tail] = dataframe  # reutiliza um espaço já liberado
            self.tail += 1
            self.count += 1

    def dequeue(self):
        with self.lock:
            if self.count == 0:
                return None
            result = self.queue[self.head]
            self.head += 1
            self.count -= 1
            # reajuste para economizar espaço quando muitos elementos forem removidos
            if self.head > 50 and self.head > 2 * self.count:
                self.queue = self.queue[self.head:self.tail]
                self.tail -= self.head
                self.head = 0
            return result

    def is_empty(self):
        with self.lock:
            return self.count == 0

    def size(self):
        with self.lock:
            return self.count

    def __str__(self):
        with self.lock:
            return str(self.queue[self.head:self.tail])
    
    def peek(self):
        with self.lock:
            if self.count == 0:
                return None
            return self.queue[self.head]

# # Criando uma fila vazia
# fila = Queue()

# # Adicionando alguns elementos à fila
# fila.enqueue(1)
# fila.enqueue(2)
# fila.enqueue(3)

# # Imprimindo o tamanho da fila
# print("Tamanho da fila:", fila.size())  # Saída: 3

# # Verificando se a fila está vazia
# print("A fila está vazia?", fila.is_empty())  # Saída: False

# # Obtendo o primeiro elemento da fila (sem remover)
# print("Primeiro elemento da fila:", fila.peek())  # Saída: 1

# # Removendo um elemento da fila
# print("Elemento removido:", fila.dequeue())  # Saída: 1

# # Imprimindo os elementos restantes na fila
# print("Elementos restantes na fila:", fila)  # Saída: 2, 3

# # Criando outra fila
# outra_fila = Queue()

# # Adicionando elementos à outra fila
# outra_fila.enqueue("a")
# outra_fila.enqueue("b")
# outra_fila.enqueue("c")

# # Imprimindo os elementos da outra fila
# print("Elementos da outra fila:", outra_fila.peek())  # Saída: a, b, c
