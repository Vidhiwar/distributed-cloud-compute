

import pyspark as ps
from pyspark import SparkContext, SparkConf


conf = SparkConf().setAppName('PageRank')
sc = SparkContext(conf=conf)

def parseLinks(line):
    line = line.split(' ')
    return line[0], line[1]
def computeRank(frontiers, rank):
    num_frontiers = len(frontiers)
    for l in frontiers:
        yield (l,rank/num_frontiers)



lines = sc.textFile('input.txt')
links = lines.map(lambda l:parseLinks(l)).groupByKey().cache().mapValues(list)
all_nodes = links.values().flatMap(lambda x:x).distinct()
connected_nodes = links.keys()
dangling_nodes = all_nodes.subtract(connected_nodes).distinct()
d = dangling_nodes.collect()


print("Input Data",lines.collect())
print("All Nodes",all_nodes.collect())
print("Links",links.collect())
print("Connected Nodes",connected_nodes.collect())
print("Dangling Nodes",dangling_nodes.collect())

num_iter = 10
alpha = 0.1
mass = 1.0
init_rank = mass/all_nodes.count()
count = all_nodes.count()

ranks = all_nodes.map(lambda x: (x, init_rank))

page_ranks = links.map(lambda u: (u[0],init_rank )).union(dangling_nodes.map(lambda x: (x,init_rank)))


print("Initial Ranks", page_ranks.collect())




for i in range(num_iter):
    dangling_masses = page_ranks.filter(lambda x: x[0] in d).values().sum()
    reduced_ranks = links.join(page_ranks).flatMap(lambda u: computeRank(u[1][0], u[1][1]))
    mapped = links.join(page_ranks).flatMap(lambda u: computeRank(u[1][0], u[1][1]))
    page_ranks = mapped.reduceByKey(lambda x,y: x+y).mapValues(lambda x: alpha/count + (1.0- alpha) * (dangling_masses/count) + (1.0-alpha)*x)
    print("--------------",page_ranks.collect())
    print('sum: ',page_ranks.values().sum())




