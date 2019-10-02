# Word count match whole word ignore case.
import shutil
from pyspark import SparkContext
import re

print("Starting...")
try:
	shutil.rmtree("wordCountOutput")
except:
	pass

def splitter(line):
	line = re.sub(r'^\W+|\W+$', '', line)
	return map(str.lower, re.split(r'\W+', line))
	
sc = SparkContext("local", "wordcount")

text_file = sc.textFile("pg100.txt")
counts = text_file.flatMap(splitter) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b) \
			 .sortByKey(True)

counts.saveAsTextFile("wordCountOutput")