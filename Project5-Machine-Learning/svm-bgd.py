import numpy as np
import math
import matplotlib.pyplot as plt

LEARNING_RATE = 0.0000003
CONVERGENCE = 0.25
REGULARIZER = 100

features = []
with open('features.txt', 'r') as infile:
	for line in infile:
		intArray = []
		lineArray = (line.rstrip()).split(',')
		for val in lineArray:
			intArray.append(int(val))
		intArray.append(-1)
		features.append(intArray)	
features = np.array(features)

targets = []
with open('targets.txt', 'r') as infile:
	for line in infile:
		val = int(line.rstrip())
		targets.append(val)	
targets = np.array(targets)

def svm_sgd(X, Y):
	costs = []
	w = np.zeros(len(X[0]))
	prevW = None
	eta = LEARNING_RATE
	iterations = 0
	
	while (True):
		iterations += 1
		prevW = w
		for i, x in enumerate(X):
			if (Y[i]*np.dot(X[i], w)) < 1:
				w = w + eta * ( (X[i] * Y[i]) + (-2  *(1/REGULARIZER)* w) )
			else:
				w = w + eta * (-2  *(1/REGULARIZER)* w)
		
		if (iterations == 1):
			continue
		curVal = 0
		for val in w:
			curVal += val**2
		curVal = math.sqrt(curVal)

		prevVal = 0
		for val in prevW:
			prevVal += val**2
		prevVal = math.sqrt(prevVal)
		
		cost = (abs(prevVal - curVal) * 100)/prevVal
		costs.append(cost)
		print(len(costs), cost)
		if (cost < CONVERGENCE and iterations > 1):
			break;	
	
	plt.plot(costs)
	plt.xlabel("Iteration")
	plt.ylabel("Cost")
	plt.savefig('iteration-vs-cost.png')		
	print(w)
	plt.show()

svm_sgd(features, targets)