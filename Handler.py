class Handler:
    def __init__(self, inputQueues: list, outputQueues: list):
        self.inputQueues = inputQueues
        self.outputQueues = outputQueues
    
    def handle(self):
        for queue in self.inputQueues:
            output_queue = Queue()
            while not queue.is_empty():
                dataframe = queue.dequeue()
                processed_dataframe = self.process(dataframe)
                output_queue.enqueue(processed_dataframe)
            self.outputQueues.append(output_queue)

    
    def process(self, dataframe: DataFrame):
        # Process the dataframe
        return processed_dataframe