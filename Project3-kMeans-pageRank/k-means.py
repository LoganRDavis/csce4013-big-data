import shutil
from pyspark import SparkContext
import re
import os
import itertools
import math
import matplotlib.pyplot as plt

print("Starting...")

def clearResult():
	try:
		shutil.rmtree("result")
	except:
		pass
		
def getPoints(d):
	s_point = d.split("\t")
	d_point = []
	for i in range(0, dimensions):
		d_point.append(float(s_point[i]))
	return d_point

def getDistance(list1, list2):
	distance = 0
	for i in range(0, dimensions):
		distance += math.pow((list1[i] - list2[i]), 2)
	return math.sqrt(distance)

def assignPoints():
	clusters = {}
	for i in range(0, numClusters):
		clusters[i] = []
	for point in points:
		centroidDistances = []
		for centroid in centroids:
			centroidDistances.append(getDistance(point, centroid))
		smallestDistance = None
		smallestDistanceIndex = None
		for index, centroidDistance in enumerate(centroidDistances):
			if smallestDistance is None or centroidDistance < smallestDistance:
				smallestDistance = centroidDistance
				smallestDistanceIndex = index
		clusters[smallestDistanceIndex].append(point)
	return clusters

def updateCentroids(clusters):
	for clusterKey in clusters:
		cluster = clusters[clusterKey]
		numClusterPoints = len(cluster)
		if numClusterPoints:
			averages = []
			for i in range(0, dimensions):
				averages.append(0)
			for point in cluster:
				for index, coordinate in enumerate(point):
					averages[index] += coordinate
			for index, average in enumerate(averages):
				averages[index] = (average / numClusterPoints)
			centroids[clusterKey] = averages

def getCost(clusters):
	cost = 0
	for clusterKey in clusters:
		cluster = clusters[clusterKey]
		for point in cluster:
			cost += math.pow(getDistance(point, centroids[clusterKey]), 2)
	return cost	
		
clearResult()

numClusters = 10
dimensions = 20
iterations = 20

sc = SparkContext("local", "k-means")

points = sc.textFile('data.txt') \
		.map(lambda d: getPoints(d)) \
		.collect()
centroids = sc.textFile('centroid.txt') \
				.map(lambda d: getPoints(d)) \
				.collect()
centroids = centroids[0:numClusters]		


costs = []
for i in range(0, iterations):
	clusters = assignPoints()
	if i == 0:
		costs.append(getCost(clusters))
	updateCentroids(clusters)
	costs.append(getCost(clusters))

os.mkdir("result")
os.mkdir("result/k-means")
with open("result/k-means/final-centroids.txt", "w") as outfile:
	for i in range(0, numClusters):
		outfile.write(str(centroids[i]) + "\n")

with open("result/k-means/iteration-vs-cost.txt", "w") as outfile:
	for i in range(0, iterations + 1):
		outfile.write("Iteration: " + str(i) + " Cost: " + str(costs[i]) + "\n")

xi = list(range(iterations + 1))
plt.plot(xi, costs, 'ro')
plt.xticks(xi)
plt.xlabel("Iteration")
plt.ylabel("Cost")
plt.savefig('result/k-means/iteration-vs-cost.png')