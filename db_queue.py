import asyncio

class DBqueue:

    

    def __init__(self):

        self.queue = asyncio.Queue()

        self.tasks = [] # workers task list


    async def put(self,json):

        self.queue.put_nowait(json) #put workload to a queue
        task = asyncio.create_task(self.worker(f'worker-{i}', self.queue))
        self.tasks.append(task)
        #await queue.join()

        

    async def worker(self,routine,name):
        while True:
            json = await self.queue.get()
            await routine(json)
            self.queue.task_done()
            print(f'{name} did it!!')
            print(f'{name} did it!!')


# class DBhandler:

#     def __init__(self):
#         self.loop = asyncio.get_event_loop()

#     def get_a_queue(self,json):

#         insert_queue.put_nowait(json)

#     def worker
