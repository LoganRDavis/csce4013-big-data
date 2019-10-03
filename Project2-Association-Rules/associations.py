import shutil
from pyspark import SparkContext
import re
import os
import itertools

print("Starting...")

def clearResult():
	try:
		shutil.rmtree("result")
	except:
		pass

def splitToId(line):
	line = re.sub(r'^\W+|\W+$', '', line)
	return re.split(r'\W+', line)

def apriori(lines):
	baskets = list(map(splitToId, lines))
	counts = {}
	threshold = support / partitions
	
	for basket in baskets:
		prevCombos = itertools.combinations(basket, 1)
		for prevCombo in prevCombos:
			key = tuple(i for i in prevCombo)
			if key not in counts:
				counts[key] = 1
			else:
				counts[key] += 1
	for id in list(counts):
		if counts[id] < threshold:
			del counts[id]
			
	depth = 2
	prevFound = len(list(counts))
	while True:
		if prevFound == 0 or depth > maxDepth:
			break
		prevFound = 0
		for basket in baskets:
			sortedBasket = sorted(set(basket)) 
			combos = itertools.combinations(sortedBasket, depth)
			for combo in combos:
				key = tuple(i for i in combo)
				if key[0:depth - 1] not in counts:
					continue		
				if key not in counts:
					prevFound += 1
					counts[key] = 1
				else:
					counts[key] += 1
		depth += 1		
		for id in list(counts):
			if counts[id] < threshold:
				del counts[id]
	return list(counts)

def countItems(lines):
	baskets = list(map(splitToId, lines))
	counts = { i : 0 for i in firstPhaseArray }
	depth = 1
	prevFound = 1
	
	while True:
		if prevFound == 0 or depth > maxDepth:
			break
		prevFound = 0
		for index, basket in enumerate(baskets):
			sortedBasket = sorted(set(basket)) 
			combos = itertools.combinations(sortedBasket, depth)
			for combo in combos:				
				key = tuple(i for i in combo)
				if key not in counts:
					continue
				prevFound += 1
				counts[key] += 1
		depth += 1
		
	returnArray = []
	for key in counts:
		returnArray.append((key, counts[key]))
	return returnArray
	
def getConfidence(counts):
	countList = list(counts)
	countDict = { count[0] : count[1] for count in countList }
	for index, count in enumerate(countList):
		key = count[0]
		depth = len(key) - 1
		if depth == 0:
			continue
		combos = list(itertools.combinations(key, depth))
		for consequentKey in key:
			for combo in combos:
				skip = False
				for subKey in combo:
					if subKey == consequentKey:
						skip = True
				if skip:
					continue
				confidence = round(count[1] / countDict[combo], 3)
				countList[index] += ((str(list(combo)) + "=>" + str(consequentKey)) + "=" + str(confidence),)
	return countList			

clearResult()
support = 100
partitions = 5
maxDepth = 3
sc = SparkContext("local", "SonApriori")
rddToProcess = sc.textFile('browsing.txt', partitions)

#First Phase
firstPhaseArray = rddToProcess \
					.mapPartitions(apriori) \
					.distinct() \
					.collect() 

#Second Phase
result = sc.textFile('browsing.txt', 1) \
			.mapPartitions(countItems) \
			.filter(lambda count: ((count[1] > support))) \
			.mapPartitions(getConfidence) \
			.sortBy(lambda a: -a[1]) \
			.sortBy(lambda a: len(a[0])) \
			.collect()
			
#Results	
os.mkdir("result")
i = 0
while i < maxDepth:
	with open("result/depth" + str(i + 1) + ".txt", 'w') as outfile:
		for line in result:
			if len(line[0]) == i + 1:
				outfile.write(str(line) + "\n")
	i += 1
	
print("Finished.")