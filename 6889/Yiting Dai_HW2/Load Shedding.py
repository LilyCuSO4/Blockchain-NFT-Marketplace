import random
from pyspark import SparkContext
from pyspark.sql import SQLContext
import time
import numpy as np
import matplotlib.pyplot as plt

sc = SparkContext("local", "myApp")
sqlContext = SQLContext(sc)

# initialise different lists
tp = []
sel = []
lis = np.arange(1, 10, 0.1)
err = []
err1 = []
accu = []
for j in lis:
    l = []
    for i in range(int(j * 10000)):  # generate random integer lists
        l.append(random.randint(1, 100))

    ll = sc.parallelize(l)  # convert list to RDD
    start = time.time()  # start the process time
    b = 10000 / ll.count()  # fix selectivity based on the number of input tuples
    flines = ll.filter(lambda x: random.random() < sel)  # filter at random and get the desired tuples
    ll = flines.map(lambda x: x * 0.1)  # operator
    time.sleep(0.05)  # fixed process time to smooth curve
    end = time.time()  # end of the process
    diff = end - start  # complete process time
    a = ll.count() / diff  # throughput of the process
    tp.append(a)  # append throughput for different selectivity
    sel.append(b)  # append selectivity
    ll_red = ll.collect()  # convert mapped lines to list
    dif = len(l) - len(ll_red)
    for m in range(dif):  # append the reduced tuple with zeroes to calculate the euclidean distance
        ll_red.append(0)
    ll_red = np.array(ll_red)
    l = np.array(ll_red)
    err1 = np.linalg.norm(l - ll_red)  # calculate euclidean distance using numpy
    err.append(err1)  # append euclidean distance between mapped lines and original list

tl = []
l_max = max(tp)  # get maximum of the list
l_min = min(tp)  # get minimum of the list
for i in range(len(tp)):
    tl.append((tp[i] - l_min) / (l_max - l_min))  # normalise the list to get it in the range of 0-1
el = []
l_max = max(el)  # get maximum of the list
l_min = min(el)  # get minimum of the list
for i in range(len(el)):
    el.append((el[i] - l_min) / (l_max - l_min))  # normalise the list to get it in the range of 0-1
for i in range(len(el)):  # since accuracy is 1 - error
    accu.append(1 - el[i])

# plot the graph
plt.plot(sel, tp, 'b-', label="Throughput")
plt.plot(sel, accu, 'r-', label="Accuracy")
plt.legend()
plt.xlabel('Selectivity')
plt.ylabel("Euclidean distance")
plt.title('Load Shedding')