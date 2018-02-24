#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#__author__ = 'rongrongl'

from multiprocessing import Pool
import os, time, random
import threading
import subprocess
import pymysql
import datetime
import linecache
import re

fileList = []
for filename in os.listdir(r'D:\Python\insert'):
    fileList.append(filename)
number = len(fileList)

def long_time_task(name):
    print('Run task %s ...' % (name))
    start = time.time()
    def command_line():
        os.system( 'cd "D:\\Quarkp2p\\mysql-5.6.19-winx64\\bin\\" & mysql -uxedk -padmin -h172.16.4.119 -P3311 xedk < D:/Python/insert/insert_%s.txt' % (name))
    command_line()

    end = time.time()
    print('Task %s runs %0.2f seconds.' % (name, (end - start)))


if __name__ == '__main__':
    now = datetime.datetime.now()
    print(now)
    print('Parent process ...')
    p = Pool()
    for i in range(number):
        result = p.apply_async(long_time_task, args=(i,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print(result.successful())
    print('All subprocesses done.')
    now1 = datetime.datetime.now()
    print(now1 - now)
