import csv
import glob
import pandas as pd
import json
import time
import csv

csv_path = "data/"
modules_path = glob.glob("{}/*".format(csv_path))

tmp = 0  # 标志位
time_stamp = []  # 储存时间戳
weighted_mixed = []  # 二维数组，【股票【加权涨幅】】
out_dic = {}


def timeStamp(time_num):
    time_ = float(time_num / 1000)
    time_array = time.localtime(time_)
    other_style_time = time.strftime("%Y-%m-%d", time_array)
    return other_style_time


with open("new_data.csv", "w") as csvfile:
    writer = csv.writer(csvfile,lineterminator='\n')
    writer.writerow(["module", "time", "percent","compare"])
    # 对模块的处理
    for module in modules_path:
        circul_sum = 0  # 发行量的和
        output = [module[5:]]  # 一个模块的数据，以列写入csv
        shares_path = glob.glob("{}/*.csv".format(module))
        # 模块中股票的处理
        for share in shares_path:
            circulation = int(share.split('-')[1])  # 股票的发行量
            weighted_percent = []  # 每天的加权涨跌幅
            with open(share) as f:
                f_csv = csv.reader(f)
                for row in f_csv:
                    if tmp == 0:
                        if row[0] == "timestamp":
                            time_stamp.append(row[0])
                        else:
                            time_stamp.append(timeStamp(int(row[0])))
                    if row[0] == "timestamp": continue
                    weighted_percent.append(float(row[7]) * circulation)
                tmp = 1
                if tmp == 1:
                    out_dic["timestamp"] = time_stamp[1:]
                    tmp = 2
                if len(weighted_percent) == 284:
                    weighted_mixed.append(weighted_percent)
                    circul_sum += circulation
        for i in range(284):
            new_out = [module[5:]]
            weighted_sum = 0  # 加权数的和
            new_out.append(time_stamp[i+1])
            for share_per in weighted_mixed:
                weighted_sum += share_per[i]
            if circul_sum == 0:
                new_out.append(0)
                output.append(0)
            else:
                new_out.append(weighted_sum / circul_sum)
                output.append(weighted_sum / circul_sum)
            new_out.append(i)
            print(new_out)
            writer.writerow(new_out)
        out_dic[output[0]] = output[1:]
        # print(out_dic)

dataframe = pd.DataFrame(out_dic)
dataframe.to_csv(r"weighted_chg_percent.csv", sep=',')

json_out = json.dumps(out_dic, ensure_ascii=False, indent=2)
with open('weighted_chg_percent.json', 'w') as f:
    f.write(json_out)



