from __future__ import print_function

import re
import sys
from operator import add

import os
import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
from numpy import linalg as LA


def second_smallest(numbers):
     m1, m2 = float('inf'), float('inf')
     for x in numbers:
         if x.real <= m1.real:
             m1, m2 = x, m1
         elif x.real < m2.real:
             m2 = x
     return numbers.index(m2)

if __name__ == "__main__":
    # print(sys.argv)
    # print(sys.argv[0])
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        exit(-1)

    inPath = sys.argv[1]
    k = int(sys.argv[2])
    outPath = sys.argv[3]

    ## input:

    lines = [line.rstrip('\n') for line in open(inPath)]

    # print(lines)
    G = nx.Graph()

    edgesList = []
    for i in lines:
        edge = i.split(' ')
        edgesList.append((int(edge[0]),int(edge[1])))

    G.add_edges_from(edgesList)

    #################################################################
    # # draw the network
    # plt.subplot(121)
    # # nx.draw(G, with_labels=True, font_weight='bold')
    # nx.draw(G)
    # plt.show()

    #####################################################################

    # adjacency = nx.adjacency_matrix(G)
    # eigen = nx.laplacian_spectrum(G)
    # matrix = nx.to_numpy_matrix(G)
    # print(type(laplacian))

    # print(len(eigen))
    # print(matrix)
    # print(len(nx.nodes(G)))
    clustList = []
    nodesList = G.nodes()
    # nodesList = map(lambda x: x, nodesList)
    nodesList = list(nodesList)
    nodesList.sort()
    clustList.append(nodesList)




    while len(clustList) < k:
        # print("*****")
        # print(clustList)

        number = 0
        location = 0
        for i in range(len(clustList)):
            if len(clustList[i]) > number:
                number = len(clustList[i])
                location = i
        nodesList = clustList[location]
        clustList.remove(nodesList)

        # nodesSliceList = map(lambda x:x-1, nodesList)
        nodesPos =[]
        nodesNeg =[]

        # # create new network:
        # nodesSet = set(nodesList)
        # edgesList = []
        # for i in lines:
        #     edge = i.split(' ')
        #     if int(edge[0]) in nodesSet and int(edge[1]) in nodesSet:
        #         edgesList.append((int(edge[0]), int(edge[1])))
        # G = nx.Graph()
        # G.add_edges_from(edgesList)

        # create a subgraph
        Gprime = G.subgraph(nodesList)


        # print(laplacian)
        # lap = laplacian[nodesSliceList,:][:,nodesSliceList]
        # print("nodesSliceList")
        # print(nodesSliceList)
        nodesList = list(Gprime.nodes())
        # print(nodesList)
        adj = nx.to_numpy_matrix(Gprime)
        # print("adj")
        # print(adj)


        # degree
        # degree = np.zeros((len(adj), len(adj)))
        # degreeSum = adj.sum(axis=0)
        # for i in range(0, len(adj)):
        #     degree[i, i] = int(degreeSum[0, i])
        degree = np.diag([d for n, d in Gprime.degree()])

        # print("degree")
        # print(degree)

        laplacian = degree - adj
        # print("lap")
        # print(laplacian)

        w, v = LA.eig(laplacian)
        # print("eig",w)

        # find the second small
        eigenValue = np.array(w).tolist()
        secondSmall = second_smallest(eigenValue)
        # print("ss",secondSmall)
        # print(eigenValue[secondSmall])

        # check the neg and pos
        check = np.array(v).tolist()
        # print(check)
        location = 0
        for i in range(len(check)):
            if check[i][secondSmall].real > 0:
                nodesPos.append(nodesList[location])
            if check[i][secondSmall].real < 0:
                nodesNeg.append(nodesList[location])
            location = location + 1
        nodesPos.sort()
        nodesNeg.sort()
        clustList.append(nodesPos)
        clustList.append(nodesNeg)

        # for element in clustList:
        #     print(len(element))

    # for element in clustList:
    #     print(element)


    # output:
    f = open(outPath, "w")

    for element in clustList:
        # for char in element:
        #     f.write(str(char)+",")
        element = map(lambda x: str(x), element)
        f.write(",".join(element))
        f.write("\n")
    f.close()


