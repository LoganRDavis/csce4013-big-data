from pyspark import SparkContext

print("Starting...")
sc = SparkContext("local", "test")

text = sc.textFile('pg100.txt', 8)
print(text.take(10))