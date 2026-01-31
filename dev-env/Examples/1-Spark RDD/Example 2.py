'''
How many words are in the book?
'''
from pyspark import SparkContext
sc = SparkContext.getOrCreate()

# Read a text file
text = sc\
    .textFile(r"/home/naya/Notebooks/melville-moby_dick.txt")

# We wish to clean all the non-letter characters using map(), so we write an auxiliary function called clean_word.
def clean_word(s):
    return ''.join([ch for ch in s if ch.isalpha()])

# We want to make an RDD of separate words,
# so we will go through the several processing steps. First, we split the lines into words using flatMap().
words_rdd = text\
    .flatMap(lambda line: line.split())\
    .map(clean_word)\
    .filter(len) # The filtering with len utilizes the fact that when checking an integer, then Python regards 0 as False and anything else as True.

print(words_rdd.count()) # We can apply the action count() to simply find the number of elements in the RDD words.