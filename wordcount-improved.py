
import re, string

from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary

wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))


def words_once_modified(line):
    words = wordsep.split(line)
    words = filter(None, words)
    for w in words:
        yield (w.lower(),1)

def add(x, y):
    return x + y

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

def main(inputs, output):
    # main logic starts here

	text = sc.textFile(inputs)
	text = text.repartition(32)
	words = text.flatMap(words_once_modified)
	
	wordcount = words.reduceByKey(add)

	outdata = wordcount.sortBy(get_key).map(output_format)
	outdata.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('wordcount improved')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)


