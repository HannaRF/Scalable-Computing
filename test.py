from DataFrame import DataFrame
from Handler import TratadorMerge
from Queue import Queue



# Criando dataframes de exemplo
dataframe1 = DataFrame([
    ["José", 30, "Developer"],
    ["Maria", 25, "Engineer"]
], columns=["Nome", "Idade", "Profissão"])

dataframe2 = DataFrame([
    ["João", 35, "Designer"],
    ["Ana", 40, "Data Scientist"]
], columns=["Nome", "Idade", "Profissão"])

# Criando filas de entrada
input_queue1 = Queue()
input_queue2 = Queue()
input_queue1.enqueue(dataframe1)
input_queue2.enqueue(dataframe2)

# Aplicando o tratador de merge
tratador_merge = TratadorMerge([input_queue1, input_queue2], "Nome")
tratador_merge.handle()

# Obtendo o resultado após aplicar o tratador de merge
output_queue = tratador_merge.outputQueues[0]

# Exibindo o resultado
print("Resultado após aplicar o tratador de merge:")
while not output_queue.is_empty():
    print(output_queue.dequeue()[1])
