
"""
original:
Example Usage:
bin/spark-submit examples/src/main/python/pagerank.py data/mllib/pagerank_data.txt 10


New:
Example Usage:
bin/spark-submit examples/src/main/python/Yu_Hou_hits.py Wiki-Vote.txt 5 out-dir
spark-submit FirstName_Lastname_hits.py <input_file> <iterations> <output-dir>


"""
from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession
import os

def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    # num_urls = len(urls)
    for url in urls:
        yield (url, rank)   # dont need divided by the length


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]
    # return int(parts[0].encode('ascii')), int(parts[1].encode('ascii'))


def parseNeighborsT(urls):
    parts = re.split(r'\s+', urls)
    return parts[1], parts[0]
    # return int(parts[1].encode('ascii')), int(parts[0].encode('ascii'))



if __name__ == "__main__":
    # print(sys.argv)
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        exit(-1)

    print("WARN: This is a naive implementation of PageRank and is given as an example!\n" +
          "Please refer to PageRank implementation provided by graphx",
          file=sys.stderr)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    # Loads in input file. It should be in format of:
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     ...
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])



    # Loads all URLs from input file and initialize their neighbors.
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
    linksT = lines.map(lambda urls: parseNeighborsT(urls)).distinct().groupByKey().cache()

    # print(links.collect())

    # # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    # h1 = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))
    # # print("len", h1.collect())
    # h2 = linksT.map(lambda url_neighbors: (url_neighbors[0], 1.0))
    # # print("len", h2.collect())
    # h = h1.fullOuterJoin(h2).map(lambda url_neighbors: (url_neighbors[0], 1.0)).distinct()

    h = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))
    # print("len", len(h.collect()))

    hValue = h.sortByKey().collect()


    # Calculates and updates URL ranks continuously using PageRank algorithm.
    for iteration in range(int(sys.argv[2])):
        # Calculates URL contributions to the rank of other URLs.
        a = links.join(h).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

        tempa = a.reduceByKey(add)
        value = tempa.max(lambda x: x[1])
        a = tempa.mapValues(lambda rank: (rank + 0.0) / value[1])
        aValue = a.sortByKey().collect()


        h = linksT.join(a).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

        temph = h.reduceByKey(add)
        value = temph.max(lambda x: x[1])
        h = temph.mapValues(lambda rank: (rank + 0.0) /value[1])
        hValue = h.sortByKey().collect()

        # print("####", hValue)
        # print("!!!!", aValue)

    # Collects all URL ranks and dump them to console.
    outputDir = sys.argv[3]

    if not os.path.exists(outputDir):
        os.makedirs(outputDir)

    f = open(outputDir+"/authority.txt","w")

    AthsList = []
    for (link, rank) in aValue:
        AthsList.append([int(link), rank])
    AthsList.sort()

    for (link, rank) in AthsList:
        f.write("%s,%.5f\n" % (link, rank))

    f.close()

    hubsList = []
    for (link, rank) in hValue:
        hubsList.append([int(link), rank])
    hubsList.sort()

    f = open(outputDir + "/hub.txt", "w")

    for (link, rank) in hubsList:
        f.write("%s,%.5f\n" % (link, rank))

    f.close()

    spark.stop()
