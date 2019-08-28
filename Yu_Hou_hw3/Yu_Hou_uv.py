from __future__ import print_function
# from sympy import *
import timeit
from math import sqrt
import sys
import numpy as np

def decomU(M,U,V,i,j):

    Mprime = np.delete(U[i,...],j,0).dot(np.delete(V,j,0))

    diff = M[i,...] - Mprime
    # print M[i,...]
    # print np.where(M[i,...] == 0)[0]
    delet = np.where(M[i,...] == 0)[0]
    Vsj = V[j,...]

    Bottom = np.sum(np.delete(np.power(Vsj, 2),delet,0))

    Top = np.sum(np.delete(Vsj*diff,delet,0))


    return Top/Bottom


def decomV(M,U,V,i,j):

    # print (i,j)
    # print np.delete(U,i,1)
    # print np.delete(V[...,j],i,0)
    # print np.delete(U,i,1).dot(np.delete(V[...,j],i,0))
    Mprime = np.delete(U,i,1).dot(np.delete(V[...,j],i,0))
    diff = M[...,j]-Mprime
    delet = np.where(M[...,j] == 0)[0]


    Uir = U[..., i]
    Top = np.sum(np.delete(Uir*diff,delet,0))

    Uir = U[...,i]
    Bottom = np.sum(np.delete(np.power(Uir, 2),delet,0))

    return Top/Bottom



def RMSE(M1,M2):
    # M1 is original
    # M2 is test
    M3 = M1-M2
    sum = 0
    num = 0
    for i in range(len(M3)):
        for j in range(len(M3[i])):
            if M1[i][j]!=0:
                num = num + 1
                sum = sum + M3[i][j]**2

    rmse = format(sqrt(sum/num),'.4f')
    return rmse



if __name__ == "__main__":
    # print (sys.argv)
    if len(sys.argv) != 6:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
    importPath = sys.argv[1]
    n = int(sys.argv[2])
    m = int(sys.argv[3])
    f = int(sys.argv[4])
    k = int(sys.argv[5])
    t = timeit.default_timer()

    # Create M
    s = (n,m)
    M = np.zeros(s)
    # print(len(M[99]))
    file = open(importPath, "r")
    lines = file.readlines()[1:]
    setMovie = set()
    for line in lines:
        lineList=line.strip().split(",")
        i=int(lineList[0])
        j=int(lineList[1])
        value=float(lineList[2])
        setMovie.add(j)

    listMovie = list(setMovie)
    listMovie.sort()
    #############################
    for line in lines:
        lineList = line.strip().split(",")
        i = int(lineList[0])
        jprime = int(lineList[1])
        j = listMovie.index(jprime)
        value = float(lineList[2])
        if j<=m and i<=n:
            M[i-1][j-1]=value


    # print(M)
    # Create U and V:
    U = np.ones((n,f))
    V = np.ones((f,m))
    # print(U)
    # print(V)

    # print(RMSE(M, U.dot(V)))

    # iteration:
    for iter in range(k):
        # print (iter)
        # iteration for U
        for i in range(n):
            for j in range(f):
                value = decomU(M,U,V,i,j)
                U[i][j]=value
                # print (U)
        # print("time:", timeit.default_timer() - t)
        # print(k)
        # iteration for V
        for j in range(m):
            if np.count_nonzero(M[...,j]) != 0:

                for i in range(f):
                    value = decomV(M, U, V, i, j)
                    V[i][j] = value

        print (RMSE(M, U.dot(V)))

    # print ("time:", timeit.default_timer() - t)

