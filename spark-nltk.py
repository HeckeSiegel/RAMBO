from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf()
conf.setMaster('yarn-client')
conf.setAppName('spark-nltk')
sc = SparkContext(conf=conf)

'''data = sc.textFile('file:///usr/share/nltk_data/corpora/state_union/1972-Nixon.txt')

def word_tokenize(x):
    import nltk
    return nltk.word_tokenize(x)

def pos_tag(x):
    import nltk
    return nltk.pos_tag([x])

words = data.flatMap(word_tokenize)
print words.take(10)

pos_word = words.map(pos_tag)
print pos_word.take(5)'''
