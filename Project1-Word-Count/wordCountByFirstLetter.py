from pyspark import SparkContext
import re

print("Starting...")

def splitter(line):

	def format(word):
		word = word.lower()
		return word[0] if (len(word)) else ""
		
	line = re.sub(r'^\W+|\W+$', '', line)
	mappedWords = map(format, re.split(r'\W+', line))
	return mappedWords
	
sc = SparkContext("local", "wordcount")

text_file = sc.textFile("pg100.txt")
counts = text_file.flatMap(splitter) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("wordCountByFirstLetterOutput")