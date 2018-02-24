import os, os.path,re
import zipfile
import  gzip
import datetime
import shutil
from multiprocessing import Pool
import os, time, random
import threading
import subprocess
import pymysql
import datetime
import linecache
import re

now= datetime.datetime.now()  #->这是时间数组格式
#print(now)
#1读取文件夹下压缩文件
List = []
for filename in os.listdir(r'D:\Python'):
        List.append(filename)
    #print (List)
i = len(List)
# print(i)
datefile = []
date = []
for j in range(0, i):
    if  re.match('^.*.gz',List[j]) :
        file=List[j]
#print(file)
#2解压
# gz 类型解压
def un_gz(file_name):
   # """ungz zip file"""
    f_name = file_name.replace(".gz", "")
    print(f_name)
    #获取文件的名称，去掉
    g_file = gzip.GzipFile(file_name)
    #创建gzip对象
    fd = open(f_name, 'wb')
    for line in g_file:
         fd.write(line)
    #open(f_name, "wb").write(g_file.read()) 小文件此用法可行
    #gzip对象用read()打开后，写入open()建立的文件中。
    g_file.close()

if __name__ == '__main__':
    un_gz(r"D:\Python\%s"%file)

sqlfile=file.replace(".gz", "")

#2分离create table 语句
lines = [l for l in open('D:/Python/%s'%sqlfile, 'r',encoding='utf-8') if l.find("INSERT", 0, 6) != 0]
leng=len(lines)
#print(leng)
fd = open("D:/Python/create.txt", "w",encoding='utf-8')
for i in range(0,leng):
    fd.write(str(lines[i]))
fd.close()


#3 分离insert语句
lines = []
fd = open('D:/Python/insert.txt', 'w', encoding='utf-8')
for l in open('D:/Python/%s'%sqlfile, 'r', encoding='utf-8'):
    # if len(lines)<=2:
    if l.find("INSERT", 0, 6) == 0:
        # lines.append(l)
        ##    leng=len(lines)
        ##    print(leng)

        # for i in range(0,leng):
        fd.write(l)
fd.close()
##    lines.clear()
#now1 = datetime.datetime.now()  # ->这是时间数组格式
#print(now1 - now)


#4 create table 入库

def command_line():
    os.system('cd "D:\\Quarkp2p\\mysql-5.6.19-winx64\\bin\\" & mysql -uxedk -padmin -h172.16.4.119 -P3311 xedk < D:/Python/create.txt')
command_line()

#5 删除 D:\Pythontemp_part_file 文件夹
shutil.rmtree('D:\\Python\\insert')

6#切割insert文件

def splitfile(filepath, linesize=3000):
    filedir, name = os.path.split(filepath)
    name, ext = os.path.splitext(name)
    filedir = os.path.join(filedir, name)
    if not os.path.exists(filedir):
        os.mkdir(filedir)

    partno = 0
    stream = open(filepath, 'r', encoding='utf-8')
    while True:
        partfilename = os.path.join(filedir, name + '_' + str(partno) + ext)
        print('write start %s' % partfilename)
        part_stream = open(partfilename, 'w', encoding='utf-8')

        read_count = 0
        while read_count < linesize:
            read_content = stream.readline()
            if read_content:
                part_stream.write(read_content)
            else:
                break
            read_count += 1

        part_stream.close()
        if (read_count < linesize):
            break
        partno += 1

    print('done')


if __name__ == '__main__':
    splitfile(r'D:\Python\insert.txt', 7000)

filename = "D:/Python/%s"%file
os.remove(filename)
filename1="D:/Python/%s"%sqlfile
os.remove(filename1)

now1= datetime.datetime.now()  #->这是时间数组格式
print(now1-now)
