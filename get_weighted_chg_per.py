import glob
import random
import pandas as pd
import json
import time
import csv
from pymongo import MongoClient


csv_path = "data/"
modules_path = glob.glob("{}/*".format(csv_path))

tmp = 0  # 标志位
time_stamp = []  # 储存时间戳
weighted_mixed = []  # 二维数组，【股票【加权涨幅】】
out_dic = {}
uri = "mongodb+srv://Frank:123456789shi@cluster0.2hsme.gcp.mongodb.net/<Homework>?retryWrites=true&w=majority"
dbname='Homework'
writeCol='source'
readCol='test3'

def timeStamp(time_num):
    time_ = float(time_num / 1000)
    time_array = time.localtime(time_)
    other_style_time = time.strftime("%Y-%m-%d", time_array)
    return other_style_time


def linkToMongo():
    client = MongoClient(uri)
    dblist = client.list_database_names()
    # dblist = myclient.database_names()
    if "Homework" in dblist:
        print("数据库已存在！")
    db = client[dbname]

    collist = db.list_collection_names()
    if "source" in collist:  # 判断 sites 集合是否存在
        print("集合已存在！")


''' 
    写入单组数据
    传入参数：字典
'''


def writeToMongodbByOne(onedic):
    client = MongoClient(uri)

    db = client[dbname]

    collection = db[writeCol]
    collection.insert_one(onedic)


''' 
    写入多组数据
    传入参数：数组（包含多个字典）
'''

def writeToMongodbByMany(array):
    client = MongoClient(uri)
    dblist = client.list_database_names()
    # dblist = myclient.database_names()
    if "Homework" in dblist:
        print("数据库已存在！")
    db = client[dbname]

    collist = db.list_collection_names()
    if writeCol in collist:  # 判断 sites 集合是否存在
        print("集合已存在！")
    collection = db[writeCol]
    collection.insert_many(array)
    print("done!")

''' 
    读mongo
    输入：字典。格式：{'module': '航空运输','time': '2019-08-09'}
    返回：多个字典
    
'''

def readFromMongo(dic):
    client = MongoClient(uri)
    dblist = client.list_database_names()
    if "Homework" in dblist:
        print("数据库已找到！")
    db = client[dbname]
    collection = db[readCol]

    doc = collection.find(dic)
    print("#########————————————————————————————————")

    docarray=[]
    finalarray=[]
    flags={}
    for x in doc:
        flags[x['module']+x['data']]=True
        docarray.append(x)
    print("@@@@@@@@@@@@@@@@")
    for item in docarray:
        if flags[item['module']+item['data']]:
            flags[item['module'] + item['data']]=False
            finalarray.append(docClean(docarray,str(item['module']),str(item['data'])))

    print("————————————————————————————————————————————————————————————————")
    print(len(finalarray))
    return finalarray

def docClean(doc,module,date):
    max=-1
    for item in doc:
        if (str(item['data'])==date and item['module']== module):
            temp=int(str(item['_id'])[0:8], 16)
            if(temp>max):
                max=temp

    for x in doc:
        if int(str(x['_id'])[0:8], 16)==max and str(x['data'])==date and x['module']== module:
            print({'module':x['module'],'time':x['data'],'numerator':x['numerator']})
            return {'module':x['module'],'time':x['data'],'numerator':x['numerator']}

def writeToTXT():
    for module in modules_path:
        name = module[5:]
        dic=readFromMongo({'module':name})
        path='/Users/shiyufei/project/data/'+name+'.txt'
        for item in dic:
            writeLine(path,item['time'],item['numerator'])

def writeLine(path,timestamp,numerator):
    file = open(path, 'a')
    file.write(timestamp+' '+numerator+'\n')
    file.close()

# def writelocal():
#     path = '/Users/shiyufei/project/mongodata.txt'
#     file = open(path, 'w')
#     client = MongoClient(uri)
#     dblist = client.list_database_names()
#     if "Homework" in dblist:
#         print("数据库已找到！")
#     db = client[dbname]
#     collection = db[readCol]
#
#     doc = collection.find({'data': '2019-08-09'})
#     for x in doc:
#         file.write(x['module']+' '+x['data']+' '+x['numerator']+'\n')

def testWriteTotxt():
    for module in modules_path:
        name = module[5:]
        path='/Users/shiyufei/project/data/'+name+'.txt'

        a1 = (2019, 9, 1, 0, 0, 0, 0, 0, 0)  # 设置开始日期时间元组（1976-01-01 00：00：00）
        a2 = (2020, 10, 31, 23, 59, 59, 0, 0, 0)  # 设置结束日期时间元组（1990-12-31 23：59：59）

        start = time.mktime(a1)  # 生成开始时间戳
        end = time.mktime(a2)  # 生成结束时间戳

        # 随机生成10个日期字符串
        for i in range(20):
            t = random.randint(start, end)  # 在开始和结束时间戳中随机取出一个
            date_touple = time.localtime(t)  # 将时间戳生成时间元组
            date = time.strftime("%Y-%m-%d", date_touple)  # 将时间元组转成格式化字符串（1976-05-21）
            num=random.uniform(0, 5)
            writeLine(path,date,str(num))



if __name__ == '__main__':
    # testWriteTotxt()

    readFromMongo({'module': '半导体','data': '2019-08-09'})

    # dicArr=[]
    # with open("new_data.csv", "w") as csvfile:
    #     writer = csv.writer(csvfile, lineterminator='\n')
    #     writer.writerow(["module", "time", "percent", "compare"])
    #     # 对模块的处理
    #     for module in modules_path:
    #         circul_sum = 0  # 发行量的和
    #         output = [module[5:]]  # 一个模块的数据，以列写入csv
    #         shares_path = glob.glob("{}/*.csv".format(module))
    #         # 模块中股票的处理
    #         for share in shares_path:
    #             circulation = int(share.split('-')[1])  # 股票的发行量
    #             weighted_percent = []  # 每天的加权涨跌幅
    #             with open(share) as f:
    #                 f_csv = csv.reader(f)
    #                 for row in f_csv:
    #                     if tmp == 0:
    #                         if row[0] == "timestamp":
    #                             time_stamp.append(row[0])
    #                         else:
    #                             time_stamp.append(timeStamp(int(row[0])))
    #                     if row[0] == "timestamp": continue
    #                     weighted_percent.append(float(row[7]) * circulation)
    #                 tmp = 1
    #                 if tmp == 1:
    #                     out_dic["timestamp"] = time_stamp[1:]
    #                     tmp = 2
    #                 if len(weighted_percent) == 284:
    #                     weighted_mixed.append(weighted_percent)
    #                     circul_sum += circulation
    #         for i in range(284):
    #             mydict = {}
    #             new_out = [module[5:]]
    #             mydict["module"] = module[5:]
    #             weighted_sum = 0  # 加权数的和
    #             new_out.append(time_stamp[i + 1])
    #             mydict["time"] =time_stamp[i + 1]
    #             for share_per in weighted_mixed:
    #                 weighted_sum += share_per[i]
    #             if circul_sum == 0:
    #                 new_out.append(0)
    #                 output.append(0)
    #                 mydict["percent"]=str(0)
    #             else:
    #                 new_out.append(weighted_sum / circul_sum)
    #                 output.append(weighted_sum / circul_sum)
    #                 mydict["percent"] = str(weighted_sum / circul_sum)
    #             new_out.append(i)
    #
    #             print(mydict)
    #             dicArr.append(mydict)
    #             #writeToMongodbByOne(mydict)
    #             # print(new_out)
    #             writer.writerow(new_out)
    #         out_dic[output[0]] = output[1:]
    #         #print("outdic",out_dic)
    #
    #
    # writeToMongodbByMany(dicArr)
    # dataframe = pd.DataFrame(out_dic)
    # dataframe.to_csv(r"weighted_chg_percent.csv", sep=',')
    #
    # json_out = json.dumps(out_dic, ensure_ascii=False, indent=2)
    # with open('weighted_chg_percent.json', 'w') as f:
    #     f.write(json_out)
