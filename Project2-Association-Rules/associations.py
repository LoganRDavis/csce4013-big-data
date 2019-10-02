import shutil
from pyspark import SparkContext
import re
import sys

print("Starting...")

def clearOutputs():
	try:
		shutil.rmtree("canidatePairs")
	except:
		pass
	try:
		shutil.rmtree("canidateTriples")
	except:
		pass

def splitToId(line):
	line = re.sub(r'^\W+|\W+$', '', line)
	return re.split(r'\W+', line)

def findSingleCandiates(lines):
	baskets = list(map(splitToId, lines))
	counts = {}
	threshold = support / partitions
	for basket in baskets:
		for id in basket:
			if id not in counts:
				counts[id] = 1
			else:
				counts[id] += 1
	print(len(list(counts)))
	for id in list(counts):
		if counts[id] < threshold:
			del counts[id]
	return list(counts)

clearOutputs()
support = 100
partitions = 5
sc = SparkContext("local", "SonApriori")
rddToProcess = sc.textFile('browsing.txt', partitions)

#First Phase
firstPhaseArray = rddToProcess \
				.mapPartitions(findSingleCandiates) \
				.distinct() \
				.collect() 

print(len(firstPhaseArray))
print("Finished.")