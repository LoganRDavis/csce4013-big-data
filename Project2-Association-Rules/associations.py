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

def splitToPair(line):
	line = re.sub(r'^\W+|\W+$', '', line)
	ids = re.split(r'\W+', line)
	pairs = []
	for index, id in enumerate(ids):
		for index2, id2 in enumerate(ids):
			if index2 <= index:
				continue
			if id > id2:
				pairs.append((id, id2))	
			else:
				pairs.append((id2, id))
	return pairs

clearOutputs()

sc = SparkContext("local", "associations")

text_file = sc.textFile('browsing.txt')
counts = text_file \
			.flatMap(splitToId) \
			.map(lambda id: (id, 1)) \
            .reduceByKey(lambda a, b: a + b)

counts = text_file \
			.flatMap(splitToPair) \
			.map(lambda id: (id, 1)) \
            .reduceByKey(lambda a, b: a + b)
			
			
counts.saveAsTextFile("canidatePairs")
