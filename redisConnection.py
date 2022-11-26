# import redis

# pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
# r = redis.Redis(connection_pool=pool)
# result = r.ft('invoice-idx').search(query='@County: Polk')

# print(result.total)
# print(result.docs)
import redis
import threading
import time

QUERYS = ['@StoreNumber: [(4400 inf]', '@StoreNumber: [(4000 4500]', '@CategoryName: VODKA 80 PROOF', '@StoreNumber: [(4000 inf]', '@ItemNumber: [(100 20000]', '@County: Polk']

EXECUTE_QUERY = 0

class Controller():
  def __init__(self):
    self.thread_array = []

  def execute_querys(self, thread_index):
    try:
      pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
      r = redis.Redis(connection_pool=pool)
      counter = 0
      for i in range(0, 5):
        result = r.ft('invoice-idx').search(query=QUERYS[EXECUTE_QUERY])
        print(f'soy thread {thread_index} y terminé la consulta {counter}')
        counter+=1
    except Exception as e:
      print(e)

  def create_threads(self, numeber_of_threads):
    for i in range(0, numeber_of_threads):
      thread = threading.Thread(target=self.execute_querys, args=(i,))
      self.thread_array.append(thread)
      
  def start_threads(self):
    for thread in self.thread_array:
      thread.start()
    for thread in self.thread_array:
      thread.join()


def main():
    controller = Controller()
    start = time.perf_counter()
    controller.create_threads(20)
    
    controller.start_threads()
    end = time.perf_counter()
    print(f'It took {end - start: 0.2f} second(s) to complete.')


if __name__ == '__main__':
    main()
