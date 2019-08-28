from math import sqrt

def distance(p1,p2):
    return sqrt( (p1[0]-p2[0])**2 + (p1[1]-p2[1])**2 )

def reoresentatives(cluster,n,p):

    selectPoints = []
    length = len(cluster)
    sumX=0
    sumY=0
    for element in cluster:
        sumX = sumX + element[0]
        sumY = sumY + element[1]

    centroidX=sumX/length
    centroidY=sumY/length

    smallestX=float("inf")
    smallestY=float("inf")

    for element in selectPoints:

        element[0]=element[0]-centroidX
        element[1]=element[1]-centroidY




# for element in heapque:
        #     if check(element[1][0],union):
        #         newDistance = clusterDistance(element[1][1],union)
        #
        #         # print("previous",element)
        #
        #         y = element[1][1]
        #         element = (newDistance, (union,y))
        #         # element[1][0]=union
        #         # element[0] = newDistance
        #
        #         # print("after",element)
        #         # print(heapque)
        #
        #     if check(element[1][1], union):
        #         newDistance = clusterDistance(element[1][0],union)
        #
        #         # print("previous",element)
        #
        #         x = element[1][0]
        #         element = (newDistance,(x,union))
        #         # element[1][1]=union
        #         # element[0]=newDistance
        #
        #         # print("after",element)