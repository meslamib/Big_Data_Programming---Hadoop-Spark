
import json


from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary

def get_subreddit_score(line):
    lines = json.loads(line)
    subreddit = lines.get("subreddit")
    score = lines.get("score")
    return (subreddit,(1,int(score)))

def add_pairs(kv1,kv2):
    return (kv1[0]+kv2[0],kv1[1]+kv2[1])
	

def output_format(kv):
    k, v = kv
    return json.dumps([k, v[1]/v[0]])
   

def main(inputs, output):
    # main logic starts here
		

	text = sc.textFile(inputs)

	records = text.map(get_subreddit_score)

	count = records.reduceByKey(add_pairs)

	outdata = count.map(output_format)
	outdata.saveAsTextFile(output)	



if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit average')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

