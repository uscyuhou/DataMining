from __future__ import print_function

import sys
from operator import add

from pyspark.shell import sc
# from pyspark import SparkContext as sc
from pyspark.sql import SparkSession
import string

ratio = 0.0
doubtCandidate=[]

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
    for element in x:
        if element not in dict:
            dict[element] = x[element]
        else:
            dict[element] = dict[element] + x[element]

    for element in y:
        if element not in dict:
            dict[element] = y[element]
        else:
            dict[element] = dict[element] + y[element]

    return dict
def doubleMap(x):
    dictionary = {}
    # print(x)
    for element in x:
        if int(element[0]) >= int(element[1]):
            print("Warn!!!!!!!#####")
        # number = hash.hashingFunc(int(element[0]),int(element[1]))

        if element not in dictionary:
            dictionary[element] = 1
        else:
            dictionary[element] += 1

    return dictionary

def doubleReduce(x,y):
    dict = {}
    # print (x,y)
    xkey = x.keys()
    ykey = y.keys()
    keys = set(xkey+ykey)
    for key in keys:
        if key in x and key in y:
            dict[key] = x[key] + y[key]
        elif key in x and key not in y:
            # print("KEY==",key)
            dict[key] = x[key]
        elif key in y and key not in x:
            dict[key] = y[key]
    return dict

def chain(*iterables):
    # chain((1,2), (1,3)) --> 1 2 3
    setList = set([])
    for it in iterables:
        for element in it:
            for ele in range(len(element)):
                setList.add(element[ele])
    listNew = list(setList)
    listNew.sort()
    return listNew

def iteration(original,over,number,start,result,s):
    # print(number)

    # print("###",over)
    # print("%%%",original)
    originalNew = map((lambda x: [val for val in x if val in over]),original)  # delete elements which are not in frequent
    originalNew = map((lambda x: chain(x)),originalNew)
    # print("^^^", originalNew)
    originalNew = map((lambda x: list(combinations(x, number))), originalNew)
    # print("%%%",originalNew)


    needTest = map(lambda x: doubleMap(x), originalNew)# tho doublemap, but it  works for triple qu...
    needTest = reduce(lambda x, y: doubleReduce(x, y), needTest)
    overNew = []
    check = False
    for key, value in needTest.items():
        if value >= s:
            check = True
            overNew.append(key)
    if check == False:
        start = False
    # print("###",overNew)
    # print("%%%",originalNew)
    if start == False:
        return result
    overNew.sort()
    result.append(overNew)

    # s=2
    result = iteration(originalNew,overNew,number+1,start,result,s)

    return result


def aprioriF(iterator):
    count = 0
    sum = []
    singleOriginal = []
    for v in iterator:
        # print("@@@@@",v)
        v = v.encode('ascii','replace').strip().split(",")
        v = [int(x) for x in v]
        # print("#####",v)
        singleOriginal.append(v)
        sum.append(singleMap(v))
        count += 1

    singletons = reduce((lambda x, y: singleReduce(x,y)), sum)


    # Find frequent singletons:
    s = count*ratio
    # print(s)
    singleOverThreathhold = []
    for key, value in singletons.items():
        if value >= s:
            singleOverThreathhold.append(key)
    # print(singleOverThreathhold)

    ##
    # overThreathhold = [int(x) for x in overThreathhold]
    ##


    # Find frequent pairs:
    pairsOriginal = map((lambda x: [val for val in x if val in singleOverThreathhold]),singleOriginal)    # delete elements which are not in frequent

    pairsOriginal = map((lambda x: list(combinations(x, 2))),pairsOriginal)
    pairs = map(lambda x: doubleMap(x),pairsOriginal)
    pairs = reduce(lambda x, y: doubleReduce(x, y),pairs)
    pairsOverThreathhold = []
    for key, value in pairs.items():
        if value >= s:
            pairsOverThreathhold.append(key)
    # print(pairsOverThreathhold)

    # Find frequent more than two:
    original = pairsOriginal
    over=pairsOverThreathhold
    number = 3
    start= True
    result = []
    result.append(singleOverThreathhold)
    pairsOverThreathhold.sort()
    result.append(pairsOverThreathhold)
    result=iteration(original,over,number,start,result,s)


    # yield (singleOverThreathhold,pairsOverThreathhold, count)
    yield (result,count)


def agg(x,y):
    newList=[]
    xNumber=len(x[0])
    yNumber=len(y[0])
    if xNumber==yNumber:
        for i in range(xNumber):
            # print(x[0][i])
            # print(y[0][i])
            lineListUnion = list(set(x[0][i]+y[0][i]))   # Union list
            lineIntersection = [val for val in y[0][i] if val in x[0][i]]   # intersection
            list1 = [val for val in x[0][i] if val not in lineIntersection] # not sure elements, need check again
            list2 = [val for val in y[0][i] if val not in lineIntersection]
            lineListNotSure = list1 + list2

            lineIntersection.sort()
            lineListNotSure.sort()
            lineListUnion.sort()

            doubtCandidate.append(lineListNotSure)
            newList.append(lineListUnion)
    if xNumber>yNumber:
        for i in range(yNumber):
            lineListUnion = list(set(x[0][i] + y[0][i]))
            lineIntersection = [val for val in y[0][i] if val in x[0][i]]   # intersection
            list1 = [val for val in x[0][i] if val not in lineIntersection]  # not sure elements, need check again
            list2 = [val for val in y[0][i] if val not in lineIntersection]
            lineListNotSure = list1 + list2


            lineIntersection.sort()
            lineListNotSure.sort()
            lineListUnion.sort()

            doubtCandidate.append(lineListNotSure)
            newList.append(lineListUnion)

        for i in range(yNumber,xNumber):
            doubtCandidate.append(x[0][i])
    if yNumber>xNumber:
        for i in range(xNumber):
            lineListUnion = list(set(x[0][i] + y[0][i]))
            lineIntersection = [val for val in y[0][i] if val in x[0][i]]   # intersection
            list1 = [val for val in x[0][i] if val not in lineIntersection]  # not sure elements, need check again
            list2 = [val for val in y[0][i] if val not in lineIntersection]
            lineListNotSure = list1 + list2

            lineIntersection.sort()
            lineListNotSure.sort()
            lineListUnion.sort()

            doubtCandidate.append(lineListNotSure)
            newList.append(lineListUnion)

        for i in range(xNumber,yNumber):
            doubtCandidate.append(y[0][i])




    return newList

def stageTwo(iterator):
    doubtDictionary = {}
    doubtList = []
    count = 0
    for element in doubtCandidate:
        for ele in element:
            doubtDictionary[ele]=0
            doubtList.append(ele)

    for v in iterator:
        # print("@@@@@",v)
        v = v.encode('ascii','replace').strip().split(",")
        v = set([int(x) for x in v])
        count +=1
        for element in doubtList:
            if type(element) == tuple:
                check = True
                for ele in element:
                    if ele not in v:
                        check = False
                if check:
                    doubtDictionary[element]+=1
            else:
                if element in v:
                    doubtDictionary[element]+=1
    yield (doubtDictionary,count)

def aggStageTwo(x,y):
    dict = {}
    # print (x,y)
    xkey = x[0].keys()
    ykey = y[0].keys()
    keys = set(xkey + ykey)
    for key in keys:
        if key in x[0] and key in y[0]:
            dict[key] = x[0][key] + y[0][key]
        elif key in x[0] and key not in y[0]:
            # print("KEY==",key)
            dict[key] = x[0][key]
        elif key in y[0] and key not in x[0]:
            dict[key] = y[0][key]
    count = x[1]+y[1]
    return (dict,count)
############################



if __name__ == "__main__":
    # print (sys.argv)
    import time

    start_time = time.time()

    if len(sys.argv) != 4:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    # input part
    importPath = sys.argv[1]
    supportRate = float(sys.argv[2])
    outputPath = sys.argv[3]
    # print(importPath, supportRate, outputPath)

    chunk = 2
    # lines = spark.read.text(importPath).rdd.map(lambda r: r)
    # sc = SparkContext("ee")

    lines = sc.textFile(importPath).map(lambda r: r)
    totalNum=len(lines.collect())
    originallines = lines
    # lines = spark.read.text(sys.argv[1],2).rdd.map(lambda r: r[0])

    ratio=supportRate

    # Stage1
    lines = lines.mapPartitions(aprioriF,2)
    # print(type(lines))
    # print(lines.distinct().collect())
    lines = lines.reduce(agg)
    # print(type(lines))
    # print(lines)

    # print(doubtCandidate)

    # Stage2
    stage2 = originallines.mapPartitions(stageTwo,2)
    # print(stage2.collect())
    stage2 = stage2.reduce(aggStageTwo)
    # print(stage2)

    shoudlDelete = set([])
    supportNum = totalNum*supportRate
    for element,value in stage2[0].items():
        if value<supportNum:
            shoudlDelete.add(element)




    # Output
    file = open(outputPath, "w")

    for i in lines:
        for element in i:
            if element not in shoudlDelete:
                s=str(element).replace(" ","")
                file.write(s + "\n")
    file.close()
    # print("--- %s seconds ---" % (time.time() - start_time))
