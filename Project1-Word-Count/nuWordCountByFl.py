# Non-unique word count by first letter
import shutil
from pyspark import SparkContext
import re

print("Starting...")
shutil.rmtree("nuWordCountByFl")

def splitter(line):
	line = re.sub(r'^\W+|\W+$', '', line)
	return map(str.lower, re.split(r'\W+', line))
	
sc = SparkContext("local", "wordcount")

text_file = sc.textFile("pg100.txt")
counts = text_file.flatMap(splitter) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b) \
			 .filter(lambda wordCount: ((wordCount[1] > 1))) \
			 .map(lambda wordCount: (wordCount[0][0], wordCount[1]) if (len(wordCount[0])) else ("", wordCount[1])) \
			 .reduceByKey(lambda a, b: a + b) \
			 .sortByKey(True)
			 
counts.saveAsTextFile("nuWordCountByFl")