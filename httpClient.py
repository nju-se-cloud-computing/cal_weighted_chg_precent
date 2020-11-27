import threading
import socket
from pymongo import MongoClient

encoding = 'utf-8'
class Reader(threading.Thread):
    def __init__(self, client):
        threading.Thread.__init__(self)
        self.client = client

    def run(self):
        while True:
            data = self.client.recv(1024)
            if (data):
                string = bytes.decode(data, encoding)
                print(string)
            # else:
            #     break

class Listener(threading.Thread):
    def __init__(self, port):
        threading.Thread.__init__(self)
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("localhost", port))
        self.sock.listen(0)

    def run(self):
        while True:
            client, cltadd = self.sock.accept()
            Reader(client).start()
            cltadd = cltadd


def linkToMongo():
    uri = "mongodb+srv://Frank:123456789shi@cluster0.2hsme.gcp.mongodb.net/<Homework>?retryWrites=true&w=majority"

    client = MongoClient(uri)
    dblist = client.list_database_names()
    # dblist = myclient.database_names()
    if "Homework" in dblist:
        print("数据库已存在！")
    db = client["Homework"]

    collist = db.list_collection_names()
    if "test3" in collist:  # 判断 sites 集合是否存在
        print("集合已存在！")


''' 
    写入单组数据
    传入参数：字典
'''


def writeToMongodbByOne(onedic):
    uri = "mongodb+srv://Frank:123456789shi@cluster0.2hsme.gcp.mongodb.net/<Homework>?retryWrites=true&w=majority"
    client = MongoClient(uri)

    db = client["Homework"]

    collection = db["source"]
    collection.insert_one(onedic)


''' 
    写入多组数据
    传入参数：数组（包含多个字典）
'''

def writeToMongodbByMany(array):
    uri = "mongodb+srv://Frank:123456789shi@cluster0.2hsme.gcp.mongodb.net/<Homework>?retryWrites=true&w=majority"
    client = MongoClient(uri)
    dblist = client.list_database_names()
    # dblist = myclient.database_names()
    if "Homework" in dblist:
        print("数据库已存在！")
    db = client["Homework"]

    collist = db.list_collection_names()
    if "source" in collist:  # 判断 sites 集合是否存在
        print("集合已存在！")
    collection = db["source"]
    collection.insert_many(array)
    print("done!")

lst = Listener(8000)
lst.start()
# linkToMongo()




