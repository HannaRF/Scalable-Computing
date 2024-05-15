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


class HandlerA(Handler):
    
    def process(self, dataframe):
        dataframe.data = dataframe.data + 1
    