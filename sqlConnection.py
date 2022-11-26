import pyodbc
import threading
import time

QUERYS = ['''select fs.InvoiceNumber, ds.City, dc.CountyKey, dc.CountyName, ds.StoreNumber from FactSales fs join DimStore ds on fs.StoreKey = ds.StoreNumber 
join DimCounty dc on dc.CountyKey = fs.CountyKey where ds.StoreNumber > 4400;''', '''select ds.City, ds.StoreNumber from FactSales fs join DimStore ds on fs.StoreKey = ds.StoreNumber where ds.StoreNumber > 
4000 and ds.storeNumber <= 4500''', '''select fs.InvoiceNumber, fs.DateKey from FactSales fs join DimItem di on fs.ProductKey = di.ItemKey where di.CategoryName = 'VODKA 80 PROOF';''',
          '''select di.ItemName, fs.ProductKey from FactSales fs join DimStore ds on fs.StoreKey = ds.StoreNumber 
join DimItem di on di.ItemKey = fs.ProductKey where ds.StoreNumber > 4000;''', '''select fs.InvoiceNumber, di.ItemName, ds.StoreName, fs.SaleCost from FactSales fs join DimStore ds on fs.StoreKey = ds.StoreNumber 
join DimItem di on di.ItemKey = fs.ProductKey where di.ItemKey > 100 and di.ItemKey <= 20000''', '''select fs.InvoiceNumber, dc.CountyName, ds.StoreName, ds.StoreNumber, di.ItemName from FactSales fs join DimItem di on fs.ProductKey = di.ItemKey 
join DimCounty dc on dc.CountyKey = fs.CountyKey join DimStore ds on ds.StoreNumber = fs.StoreKey where dc.CountyName = 'Polk';''']

EXECUTE_QUERY = 0


class Controller():
  def __init__(self):
    self.thread_array = []

  def execute_querys(self, thread_index):
    try:
      connection = pyodbc.connect('DRIVER={SQL Server}; SERVER=JAVIER-PC;DATABASE=ProyectoBases;Trusted_Connection=yes;')
      cursor = connection.cursor()
      counter = 0
      for i in range(0, 10):
        cursor.execute(QUERYS[EXECUTE_QUERY])
        row = cursor.fetchall()
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
    controller.create_threads(10)
    
    controller.start_threads()
    end = time.perf_counter()
    print(f'It took {end - start: 0.2f} second(s) to complete.')


if __name__ == '__main__':
    main()