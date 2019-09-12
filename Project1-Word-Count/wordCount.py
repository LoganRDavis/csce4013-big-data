from pyspark import SparkContext
import re

print("Starting...")

def splitter(line):
	line = re.sub(r'^\W+|\W+$', '', line)
	return map(str.lower, re.split(r'\W+', line))
	
sc = SparkContext("local", "wordcount")

#input = sc.textFile("pg100.txt", 1)
#words = input.flatMap(splitter)
#words_mapped = words.map(lambda x: (x,1))

#sorted_map = words_mapped.sortByKey()
#counts = sorted_map.reduceByKey("add")

#counts.saveAsTextFile("./wordCount.txt")

text_file = sc.textFile("pg100.txt")
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("wordCountOutput")