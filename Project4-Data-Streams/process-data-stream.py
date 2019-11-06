import math

def createHash(a, b, p, numBuckets, x):
	y = x % p
	val = (a*y + b) % p
	return val % numBuckets


p = 123457
delta = math.exp(-5)
epsilon = math.e * 10**-4
numHashFunctions = math.ceil(math.log(1/delta))
numBuckets = math.ceil(math.e / epsilon)
hashParams = [
	(3, 1561),
	(17, 277),
	(38, 394),
	(61, 13),
	(78, 246)
]
buckets = []
maxNum = -1

for i in range(numHashFunctions):
	buckets.append([])
	for j in range(numBuckets):
		buckets[i].append(0)

with open('data_stream.txt') as infile:
	for line in infile:
		value = int(line)
		if value > maxNum:
			maxNum = value
		for i in range(numHashFunctions):
			hash = createHash(hashParams[i][0], hashParams[i][1],
				p, numBuckets, value)
			buckets[i][hash] += 1
			
with open('result.txt', 'w') as outfile:
	for i in range(maxNum):
		value = i + 1
		estimates = []
		for j in range(numHashFunctions):
			hash = createHash(hashParams[j][0], hashParams[j][1],
				p, numBuckets, value)
			estimates.append(buckets[j][hash])
		estimateValue = None
		for estimate in estimates:
			if estimateValue is None:
				estimateValue = estimate
			elif estimateValue > estimate:
				estimateValue = estimate				
		outfile.write(str(value) + "\t" + str(estimateValue) + "\n")