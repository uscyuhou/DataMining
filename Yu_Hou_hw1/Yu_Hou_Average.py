#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession
import string


if __name__ == "__main__":
    # print (sys.argv)
    if len(sys.argv) != 3:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    header = lines.first()
    head = lines.collect()[0].split(",")
    eventNumber = head.index("event")
    pageNumber = head.index("page_count")

    # print(eventNumber,pageNumber)
    # print(string.punctuation)
    #
    # s = u"string. With. Punctuation?"  # Sample string
    # print(s.encode('ascii','replace').translate(None, string.punctuation))
    # remove all punctuation
    # lines = lines.map(lambda x:  x.encode('ascii','replace').translate(string.maketrans("",""), string.punctuation))
    # lines = lines.map(lambda x: x.translate(string.maketrans(string.punctuation, u" "*32),))


    # Remove header from the txt file
    lines = lines.filter(lambda line: line != header)



    # data cleaning delet " ' ", and "-"
    lines = lines.map(lambda x: x.replace("-","").replace("'",""))


    # mapper 1 lowercase; 2 strip; 3 split and then join for extra space
    # key = event; value = (count, totalpage)
    # ' ' join is for delete extra white space
    # strip() is similar to trim()
    # .encode('ascii','replace').translate(None, string.punctuation) means remove all punctuations
    lines = lines.map(lambda x: (' '.join(x.split(",")[eventNumber].lower().strip().encode('ascii','replace').translate(string.maketrans(string.punctuation, u" "*32)).split()) , (1,int(x.split(",")[pageNumber])) ))

    # remove the lines with empty event
    lines = lines.filter(lambda x: x[0]!="" )

    # print(lines.collect())

    # reducer
    output = lines.reduceByKey(lambda a,b: (a[0]+b[0],  a[1]+b[1]))
    output = output.map(lambda x: (x[0], (x[1][0], round(float(x[1][1])/ float(x[1][0]),3),3 )))

    # sort by the event count
    # output = output.sortBy(lambda a: -a[1][0])
    output = output.sortByKey()

    # print (output.collect())


    # # Output
    # import os
    # import errno
    #
    #
    # def make_sure_path_exists(path):
    #     try:
    #         os.makedirs(path)
    #     except OSError as exception:
    #         if exception.errno != errno.EEXIST:
    #             raise

    import os

    if not os.path.exists(sys.argv[2]):
        os.makedirs(sys.argv[2])

    file = open(sys.argv[2]+"/"+"Yu_Hou_task2.txt", "w")

    output = output.collect()
    for (word, count) in output:
        # file.write("%s: %i" % (word, count))
        file.write("%s\t%i\t%.3f\n" % (word.encode('ascii','replace'),int(count[0]),float(count[1])))
        # file.write(word.encode('ascii','replace')+"\t"+str(count[0])+"\t"+str(count[1])+"\n")

    file.close()


    # counts = lines.flatMap(lambda x: x.split(' ')) \
    #               .map(lambda x: (x, 1)) \
    #               .reduceByKey(add)



    spark.stop()
