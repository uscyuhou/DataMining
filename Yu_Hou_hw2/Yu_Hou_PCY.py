
from __future__ import print_function

import sys

from pyspark.sql import SparkSession


class Hash:
    def __init__(self,a,b,n):
        self.a = a
        self.b = b
        self.n = n
    def hashingFunc (self,i,j):

        number = (self.a * i + self.b * j) % self.n

        return number


def combinations(iterable, r):
    # combinations('ABCD', 2) --> AB AC AD BC BD CD
    # combinations(range(4), 3) --> 012 013 023 123
    pool = tuple(iterable)
    n = len(pool)
    if r > n:
        return
    indices = list(range(r))
    yield tuple(pool[i] for i in indices)
    while True:
        for i in reversed(range(r)):
            if indices[i] != i + n - r:
                break
        else:
            return
        indices[i] += 1
        for j in range(i+1, r):
            indices[j] = indices[j-1] + 1
        yield tuple(pool[i] for i in indices)

def singleMap(x):
    dict = {}
    for element in x:
        if element not in dict:
            dict[element] = 1
        else:
            dict[element] += 1
    return dict
def singleReduce(x,y):
    dict = {}
    for element in x.keys():
        if element not in dict:
            dict[element] = x[element]
        else:
            dict[element] = dict[element] + x[element]

    for element in y.keys():
        if element not in dict:
            dict[element] = y[element]
        else:
            dict[element] = dict[element] + y[element]

    return dict

def doubleMap(x):
    hash = Hash(a, b, n)
    hashDict = {}
    for i in range(n):
        hashDict[i] = {}

    for element in x:
        if int(element[0]) >= int(element[1]):
            print("Warn!!!!!!!#####")
        number = hash.hashingFunc(int(element[0]),int(element[1]))

        if element not in hashDict[number]:
            hashDict[number][element] = 1
        else:
            hashDict[number][element] += 1

    return hashDict

def doubleReduce(x,y):
    dict = {}
    keys = x.keys()
    for key in keys:
        dict1 = x[key]
        dict2 = y[key]
        dictNew = singleReduce(dict1,dict2)
        dict[key]=dictNew
    return dict

# def pairsSort():



if __name__ == "__main__":
    # print (sys.argv)
    if len(sys.argv) != 7:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    # input part
    importPath = sys.argv[1]
    a = int(sys.argv[2])
    b = int(sys.argv[3])
    n = int(sys.argv[4])
    s = int(sys.argv[5])
    outputPath = sys.argv[6]
    # print(a,b,n,s,outputPath)

    lines = spark.read.text(importPath).rdd.map(lambda r: r[0])
    lines = lines.map(lambda x: x.encode('ascii','replace').strip().split(","))
    lines = lines.map(lambda v: [int(x) for x in v])

    # # single part
    lines1 = lines.map(lambda x: singleMap(x))
    lines1 = lines1.reduce(lambda x,y: singleReduce(x,y))
    # print(lines1)

    # filter the single frequency
    overThreathhold = []
    for key, value in lines1.items():
        if value >= s:
            overThreathhold.append(key)
    # print(overThreathhold)


    # # double part
    # b3 = [val for val in b1 if val in b2]
    lines2 = lines.map(lambda x: [val for val in x if val in overThreathhold])
    lines2 = lines2.map(lambda x: list(combinations(x,2)))
    lines2 = lines2.map(lambda x: doubleMap(x))
    lines2 = lines2.reduce(lambda x,y: doubleReduce(x,y))

    # print(lines2)
    hashNumberList = []
    notInhashNumberList = []
    for key,value in lines2.items():
        count = 0
        for k,v in value.items():
            count=count + v
        if count >= s:
            hashNumberList.append(key)
        else:
            notInhashNumberList.append(key)

    # print(hashNumberList)
    # print(notInhashNumberList)
    print("False Positive Rate:",float("{0:.3f}".format(len(hashNumberList)/float(n))))

    # singletons:
    # overThreathhold = [int(x) for x in overThreathhold]
    overThreathhold.sort()
    # print(overThreathhold)


    # pairs:
    overThreathholdPairs = []
    notoverThreathholdPairs = []
    for i in hashNumberList:
        # print(lines2[i])
        for key in lines2[i].keys():
            # print(key)
            if lines2[i][key]>=s:
                overThreathholdPairs.append(key)
            else:
                notoverThreathholdPairs.append(key)
    # infrepairs=[]
    # for i in notInhashNumberList:
    #     # print(lines2[i])
    #     for key in lines2[i].keys():
    #         # print(key)
    #         infrepairs.append(key)

    overThreathholdPairs.sort()
    notoverThreathholdPairs.sort()

    # print(overThreathholdPairs)
    # print(notoverThreathholdPairs)
    # print(float(len(overThreathholdPairs))/(len(overThreathholdPairs)+len(notoverThreathholdPairs)+len(infrepairs)))

    candidate=[]
    # find the candidate
    for i in notInhashNumberList:
        for key in lines2[i].keys():
            # print(key)
            if key[0] in overThreathhold and key[1] in overThreathhold:
                candidate.append(key)
    candidate.sort()
    import os

    if not os.path.exists(outputPath):
        os.makedirs(outputPath)

    file = open(outputPath+"/"+"frequentset.txt", "w")
    for i in overThreathhold:
        file.write("%s\n" % (i))
    for i in overThreathholdPairs:

        file.write(str(i).replace(" ","") + "\n")
    file.close()

    file = open(outputPath+"/"+"candidates.txt", "w")

    for i in candidate:
        file.write(str(i).replace(" ","") + "\n")
    file.close()
