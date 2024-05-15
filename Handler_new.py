from abc import ABC, abstractmethod
from DataFrame import DataFrame
from Queue import Queue

"""
A1 Comp Escalável

Objetivo: framework para criar pipelines de processamento de dados de forma Concorrente e Paralela.

Por construção:
- Eficiência
- Balanceamento de carga
- Baixo acoplamento dos tratadores

Abstrações:
- Dataframe : Representa dados (possui um tipo, linha e coluna)
- Repositório de Dados: Extração e Carga 
- Tratador : Operação de uma etapa do Pipeline, abstract factory
- Trigger: iniciar a execução de um pipeline (TimerTrigger, RequestTrigger)
- Queue: comunicação entre tratadores, fila de dataframes a serem processados
	inserir dataframe no final da lista, remover dataframe no início da lista.

- Mocks:
O serviço ContaVerde fornece todos dados em planilhas rotineiramente
sobreescritas. Os tipos de dados são:
	- Usuários:	ID,
			Nome,
			Sobrenome,
			Endereço,
			Data de cadastro,
			Data de aniversário

	- Produtos: ID, Nome, Imagem, Descrição, Preço.

	- Estoque: ID do produto, Quantidade disponível.

	- Ordens de compra: 	ID do usuário,
				ID do produto,
				Quantidade,
				Data de criação,
				Data do pagamento,
				Data da entrega
	
- DashBoard:
	- nº produtos visualizados / minuto
	- nº produtos comprados / minuto
	- nº produtos comprados / minuto
	- nº usuários únicos visualizando cada produto / minuto
	- Ranking de produtos mais comprados na última hora
	- Ranking de produtos mais visualizados na última hora
	- Quantidade média de visualizações de um produto antes de efetuar uma compra.
	- nº produtos vendidos sem disponibilidade no estoque.

"""

class Handler:
    
    def __init__(self, queue_in, queue_out):
        self.queue_in = queue_in
        self.queue_out = queue_out
    
    def run(self):
        while True:
            dataframe = self.queue_in.get()
            if dataframe is None:
                break
            self.process(dataframe)
            self.queue_out.put(dataframe)
    
    def process(self, dataframe):
        pass

class FilterHandler(Handler):
    
    def __init__(self, queue_in, queue_out, condition):
        super().__init__(queue_in, queue_out)
        self.condition = condition
    
    def process(self, dataframe):
        dataframe.data = dataframe.data[dataframe.data.apply(self.condition, axis=1)]

class TransformHandler(Handler):
    
    def __init__(self, queue_in, queue_out, column, transform_function):
        super().__init__(queue_in, queue_out)
        self.column = column
        self.transform_function = transform_function
    
    def process(self, dataframe):
        dataframe.data[self.column] = dataframe.data[self.column].apply(self.transform_function)

class AggregateHandler(Handler):
    
    def __init__(self, queue_in, queue_out, group_by_column, aggregate_column, agg_func):
        super().__init__(queue_in, queue_out)
        self.group_by_column = group_by_column
        self.aggregate_column = aggregate_column
        self.agg_func = agg_func
    
    def process(self, dataframe):
        dataframe.data = dataframe.data.groupby(self.group_by_column)[self.aggregate_column].agg(self.agg_func).reset_index()

class LoggerHandler(Handler):
    
    def process(self, dataframe):
        print(dataframe.data)
        
"""
Exemplo de uso:
"""
if __name__ == "__main__":
    # Criar filas
    queue1 = Queue()
    queue2 = Queue()
    queue3 = Queue()
    queue4 = Queue()
    
    # Criar DataFrame de exemplo
    data = {'ID': [1, 2, 3, 4], 'Value': [10, 20, 30, 40]}
    df = DataFrame(data)

    # Adicionar DataFrame à fila de entrada
    queue1.put(df)
    queue1.put(None)  # Indicar final da fila
    
    # Criar handlers
    filter_handler = FilterHandler(queue1, queue2, lambda row: row['Value'] > 15)
    transform_handler = TransformHandler(queue2, queue3, 'Value', lambda x: x * 2)
    logger_handler = LoggerHandler(queue3, queue4)
    
    # Executar handlers
    filter_handler.run()
    transform_handler.run()
    logger_handler.run()
    
    # Pegar resultado final
    final_df = queue4.get()
    print(final_df.data)
