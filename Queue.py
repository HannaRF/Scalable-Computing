class Queue:
    def __init__(self):
        self.items = []

    def is_empty(self):
        return len(self.items) == 0

    def enqueue(self, item):
        self.items.append(item)

    def dequeue(self):
        if not self.is_empty():
            return self.items.pop(0)
        else:
            raise IndexError("A fila está vazia")

    def peek(self):
        if not self.is_empty():
            return self.items[0]
        else:
            raise IndexError("A fila está vazia")

    def size(self):
        return len(self.items)

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
