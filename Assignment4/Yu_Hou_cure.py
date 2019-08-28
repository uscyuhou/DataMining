from __future__ import print_function
import timeit
from math import sqrt
import sys
import numpy as np
import heapq


import matplotlib.pyplot as plt

def distance(p1,p2):
    return sqrt( (p1[0]-p2[0])**2 + (p1[1]-p2[1])**2 )

def unionCandidate(list):

    # example: list = (1,2) or list = ((1,2),3) or list = ((1,2,3),(4,5,6,7,8))
    newList = ()
    if type(list[0]) == int:
        newList = newList + (list[0],)
    else:
        newList = newList + list[0]

    if type(list[1]) == int:
        newList = newList + (list[1],)
    else:
        newList = newList + list[1]

    return newList


def check(list1,list2):

    if type(list1)==int:
        if list1 in list2:
            return True
        else:
            return False

    else:
        list3 = [val for val in list1 if val in list2]
        if len(list3)==0:
            return False
        else:
            return True

def clusterDistance(cluster1, cluster2):

    # example: cluster1 = (1,2,3); cluster2 = 1; or (1,2,3) (4,5,6,7) or 1 (2,3)
    smallest = float("inf")
    point = ()
    if type(cluster1) == tuple and type(cluster2) == tuple:
        for i in cluster1:
            for j in cluster2:
                d = distance(sampleData[i],sampleData[j])
                if d < smallest:
                    smallest = d
                    point = (i,j)
    if type(cluster1) == tuple and type(cluster2) != tuple:
        for i in cluster1:
            d = distance(sampleData[i],sampleData[cluster2])
            if d < smallest:
                smallest = d
                point = (i,cluster2)
    if type(cluster1) != tuple and type(cluster2) == tuple:
        for i in cluster2:
            d = distance(sampleData[i],sampleData[cluster1])
            if d < smallest:
                smallest = d
                point = (i,cluster1)
    #print(point)
    return smallest
def distanceOfPointAndSelectPoints (point,points):
    if len(points) == 1:
        # just smallest point is selected
        return distance(sampleData[point],sampleData[points[0]])
    else:
        return clusterDistance(tuple(points),point)


def representatives(cluster,n,p):

    # parameter: cluster:(1, 2, 3, 4, 5......99, 100)
    # return :# cluster 1: [[[0.1, 0.1], [0.1, 0.1], [0.1, 0.1], [0.1, 0.1]], (1, 2, 3, 4, 5......99, 100)]
    # print(cluster)
    selectPoints = []
    length = len(cluster)
    sumX=0
    sumY=0

    smallestX=float("inf")
    smallestY=float("inf")

    smallestPoint = -1

    for element in cluster:

        if sampleData[element][0] < smallestX:
            smallestPoint = element
            smallestX = sampleData[element][0]
            smallestY = sampleData[element][1]
        if sampleData[element][0] == smallestX:
            if sampleData[element][1] < smallestY:
                smallestPoint = element
                smallestX = sampleData[element][0]
                smallestY = sampleData[element][1]

        sumX = sumX + sampleData[element][0]
        sumY = sumY + sampleData[element][1]

    # print(sampleData[smallestPoint][0],sampleData[smallestPoint][1])

    centroidX=sumX/length
    centroidY=sumY/length
    # print(centroidX,centroidY)

    # print(smallestPoint)
    selectPoints.append(smallestPoint)




    while len(selectPoints) != n:
        furtherestDistance = float('-inf')
        furtherestPoint = -1
        for element in cluster:
            if element not in selectPoints:
                distanceCalculation = distanceOfPointAndSelectPoints(element,selectPoints)
                if distanceCalculation > furtherestDistance:
                    furtherestDistance = distanceCalculation
                    furtherestPoint = element

        selectPoints.append(furtherestPoint)

    # print(selectPoints)

    selectPointsCoord = []

    for element in selectPoints:

        x = sampleData[element][0] + (centroidX-sampleData[element][0])*p
        y = sampleData[element][1] + (centroidY-sampleData[element][1])*p

        selectPointsCoord.append([x,y])
    # print(selectPointsCoord)

    returnThing = []
    returnThing.append(selectPointsCoord)
    returnThing.append(cluster)

    # print(selectPoints)
    return returnThing,selectPoints


if __name__ == "__main__":
    # print (sys.argv)
    if len(sys.argv) != 7:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
    importPath1 = sys.argv[1]
    importPath2 = sys.argv[2]
    k = int(sys.argv[3])
    n = int(sys.argv[4])
    p = float(sys.argv[5])
    outPath = sys.argv[6]
    clusterSet = set([])

    t = timeit.default_timer()

    # print(k,n,p)

    # read sample data
    sampleData = []
    fullData = []
    file = open(importPath1, "r")
    lines = file.readlines()
    for line in lines:
        element = line.strip().split(",")
        sampleData.append((float(element[0]),float(element[1])))
    file.close()

    # read full data
    file = open(importPath2, "r")
    lines = file.readlines()
    for line in lines:
        element = line.strip().split(",")
        fullData.append((float(element[0]),float(element[1])))
    file.close()

    # print(sampleData)
    # print(fullData)

    '''
    Create the heap
    
    '''
    heapque = []
    ## First phase
    for i in range(len(sampleData)):
        clusterSet.add(i)
        for j in range(i+1,len(sampleData)):
            dist = distance(sampleData[i],sampleData[j])
            heapq.heappush(heapque, (dist, (i,j)))
    heapque.sort()
    # print(heapque)

    while len(clusterSet)!= k :
        popNode = heapq.heappop(heapque)
        # print(popNode)
        union = unionCandidate(popNode[1])

        ## Check the invalid node:
        if popNode[1][0] in clusterSet and popNode[1][1] in clusterSet:
            # print("11111")
            clusterSet.remove(popNode[1][0])
            clusterSet.remove(popNode[1][1])
            clusterSet.add(union)


            # print(clusterSet)
            # print (len(clusterSet))
            # print(union)

            for element in clusterSet:
                if element != union:
                    newDistance = clusterDistance(element, union)
                    heapq.heappush(heapque, (newDistance, (element,union)))


        heapque.sort()
        # print(heapque)
    # print(clusterSet)



    # ## print the cluster
    # import matplotlib.pyplot as plt
    #
    # x = []
    # y = []
    # color = ['r','b','g','w']
    # i = 0
    # for element in clusterSet:
    #     for point in element:
    #         x.append(sampleData[point][0])
    #         y.append(sampleData[point][1])
    #     plt.scatter(x, y, c= color[i])
    #     i = i + 1
    #     x = []
    #     y = []
    # plt.show()

    '''
    find representatives
    '''

    clusterList = []
    clusterRep = []
    for element in clusterSet:
        tem,points = representatives(element,n,p)
        clusterList.append(tem)
        clusterRep.append(points)

    # '''
    # print the representatives:
    # '''
    #
    # x = []
    # y = []
    # color = ['r','b','g','w','k']
    # i = 0
    # for element in clusterList:
    #     for point in element[0]:
    #         x.append(point[0])
    #         y.append(point[1])
    #     plt.scatter(x, y, c= color[i])
    #     i = i + 1
    #     x = []
    #     y = []
    # plt.show()

    '''assign points to clusters:'''

    belongTo = -1
    smallestDistance = float('inf')

    CLUSTER = []
    for element in fullData:
        c = 0
        smallestDistance = float('inf')
        for cluster in clusterList:
            ## cluster will looks like cluster 1 : [[[0.1, 0.1],[0.1, 0.1],[0.1, 0.1],[0.1, 0.1]], (1,2,3,4,5......99,100)]
            for rep in cluster[0]:
                # print(rep,element)
                d = distance(rep,element)
                if d < smallestDistance:
                    smallestDistance = d
                    belongTo = c
            c = c + 1
        CLUSTER.append(belongTo)

    # # print(CLUSTER)
    # x = []
    # y = []
    # color = ['r','b','g','w','k']
    # i = 0
    # for i in range(len(fullData)):
    #     x=fullData[i][0]
    #     y=fullData[i][1]
    #     plt.scatter(x, y, c= color[CLUSTER[i]])
    #
    # plt.show()

    #
    # # count c:
    # dict = {}
    # for i in CLUSTER:
    #     if i in dict:
    #         dict[i]+=1
    #     else:
    #         dict[i]=1
    # print(dict)

    '''
    this representitives are before 20%
    '''
    for cluster in clusterRep:
        list1 = []
        for point in cluster:
            list1.append(list(sampleData[point]))
        print(list1)


    file = open(outPath, "w")

    for i in range(len(fullData)):
        file.write(str(fullData[i][0])+","+str(fullData[i][1])+","+str(CLUSTER[i])+"\n")

    file.close()

