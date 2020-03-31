#!/usr/bin/python
# -*-coding:utf-8 -*

import os, sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from copy import copy
import time

def ccf_iterate_map(X):
    return [(X[0],X[1]),(X[1], X[0])]

def ccf_iterate_reduce(X):
    global PAIR_COUNTER

    key    = X[0]
    values = X[1]
    L      = []
    valueList = []
    minim  = key

    for value in values:
        if value < minim:
            minim = value
        valueList.append(value)

    if minim < key:
        L.append((key,minim))
        for value in valueList:
            if minim != value:
                PAIR_COUNTER += 1
                L.append((value,minim))

    return L

if __name__ == "__main__":
    start_time   = time.time()
    nbiter       = 0
    sc           = SparkContext(appName = "Connected Component Finder")
    PAIR_COUNTER = sc.accumulator(1)

    # Mettre en entrée un fichier sous le format (start_node end_node)
    lines        = sc.textFile("webGoogle10000.txt")
    rows         = lines.map(lambda x : x.strip().split())
    graphNodes   = rows.map(lambda x : (int(x[0]), int(x[1])))
    
    OLD_COUNTER  = -1
    while OLD_COUNTER != PAIR_COUNTER.value:
        nbiter        += 1
        OLD_COUNTER    = PAIR_COUNTER.value
        
        # Partie Iterate Map
        graphNodes     = graphNodes.flatMap(ccf_iterate_map)
        graphNodes     = graphNodes.map(lambda x : (x[0], [x[1]])).reduceByKey(lambda a,b : a + b)
        graphNodes     = graphNodes.flatMap(ccf_iterate_reduce)

        # Partie Dedup
        graphNodes     = graphNodes.distinct()
        graphNodes     = graphNodes.sortBy(lambda x: x[1], True)
        graphNodes     = graphNodes.sortBy(lambda x: x[0], True)
    
    L = graphNodes.collect()
    f = open("outputwG10000.txt", "w")
    for elem in L:
        f.write("{} {}\n".format(elem[0], elem[1]))
    f.close()
    sc.stop()
    
    print("Temps d'exécution : %s secondes ---" % (time.time() - start_time))
    print("Nombre d'itérations : %s" %(nbiter))
