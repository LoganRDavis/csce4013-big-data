import os
import shutil
import re
import psutil
from pyspark.sql import SparkSession
from operator import add

def clearResult():
	try:
		shutil.rmtree("result/page-rank")
	except:
		pass

def getEdge(urls):
	edge = re.split(r'\s+', urls)
	return edge[0], edge[1]

def getContributions(urls, rank):
	localNumUrls = len(urls)
	for url in urls:
		yield (url, rank / localNumUrls)

if __name__ == "__main__":
	clearResult()
	iterations = 40
	beta = 0.8
	spark = SparkSession.builder\
		.appName("page-rank").getOrCreate()

	data = spark.read.text("graph.txt").rdd
	matrix = data.map(lambda r: r[0]).map(lambda e: getEdge(e))\
		.distinct().groupByKey().cache()
	numUrls = matrix.count()
	
	ranks = matrix.map(lambda url_neighbors: (url_neighbors[0], 1 / numUrls))
	for iteration in range(iterations):
		contributions = matrix.join(ranks).flatMap(
		lambda url_urls_rank: getContributions(url_urls_rank[1][0], url_urls_rank[1][1]))
		ranks = contributions.reduceByKey(add).mapValues(lambda rank: rank * beta + (1 - beta) / numUrls)

	finalRanks = ranks.collect()
	finalRanks.sort(key=lambda tup: tup[1], reverse=True)
	
	try:
		os.mkdir("result")
	except:
		pass
	os.mkdir("result/page-rank")
	with open("result/page-rank/rankings.txt", "w") as outfile:
		for index, (link, rank) in enumerate(finalRanks):
			outfile.write("POS: %s URL: %s RANK: %s. \n" % (index + 1, link, rank))

	with open("result/page-rank/summary.txt", "w") as outfile:
		outfile.write("HIGHEST RANKS\n")
		for index, (link, rank) in enumerate(finalRanks):
			outfile.write("POS: %s URL: %s RANK: %s \n" % (index + 1, link, rank))
			if index >= 4:
				break
		outfile.write("LOWEST RANKS\n")
		for index, (link, rank) in reversed(list(enumerate(finalRanks))):
			outfile.write("POS: %s URL: %s RANK: %s \n" % (index + 1, link, rank))
			if index <  (numUrls- 4):
				break
	spark.stop()