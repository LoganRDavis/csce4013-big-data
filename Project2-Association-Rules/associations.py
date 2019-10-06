import shutil
from pyspark import SparkContext
import re
import os
import itertools

print("Starting...")

def clearResult():
	try:
		shutil.rmtree("results")
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
	countDict = { count[0] : count[1] for count in counts }
	confidenceAssociations = []
	for countKey in countDict:
		depth = len(countKey) - 1
		if depth == 0:
			continue
		combos = list(itertools.combinations(countKey, depth))
		for consequentKey in countKey:
			for combo in combos:
				skip = False
				for subKey in combo:
					if subKey == consequentKey:
						skip = True
						break
				if skip:
					continue
				confidence = round(countDict[countKey] / countDict[combo], 3)
				confidenceAssociations.append((list(combo), consequentKey, confidence,))
	return confidenceAssociations			

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
supportResult = sc.textFile('browsing.txt', 1) \
			.mapPartitions(countItems) \
			.filter(lambda count: ((count[1] > support))) \
			.sortBy(lambda a: -a[1]) \
			.sortBy(lambda a: len(a[0])) \
		
#Get confidence
confidenceResult = supportResult \
			.mapPartitions(getConfidence) \
			.sortBy(lambda a: -a[2]) \
			.sortBy(lambda a: len(a[0])) \
			.collect()

#Write Results	
os.mkdir("results")
os.mkdir("results/support")
os.mkdir("results/confidence")

supportResult = supportResult.collect()
i = 0
while i < maxDepth:
	with open("results/support/depth" + str(i + 1) + ".txt", 'w') as outfile:
		for line in supportResult:
			if len(line[0]) == i + 1:
				outfile.write(str(line) + "\n")
	i += 1
	
i = 1
while i < maxDepth:
	with open("results/confidence/depth" + str(i + 1) + ".txt", 'w') as outfile:
		for line in confidenceResult:
			if len(line[0]) == i:
				outfile.write(str(line) + "\n")
	i += 1	
	
print("Finished.")