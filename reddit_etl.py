
import json


from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary

def get_subreddit_score_author(line):
    lines = json.loads(line)
    subreddit = lines.get("subreddit")
    score = lines.get("score")
    author = lines.get("author")
    return (subreddit, int(score), author)
	

def main(inputs, output):
    # main logic starts here
		

	text = sc.textFile(inputs)

	records = text.map(get_subreddit_score_author)

	records_filtered = records.filter(lambda c: "e" in c[0]).cache()
	records_pos = records_filtered.filter(lambda c: c[1] > 0)
	records_neg = records_filtered.filter(lambda c: c[1] < 0)

	records_filtered.map(json.dumps).saveAsTextFile(output)

	records_pos.map(json.dumps).saveAsTextFile(output + '/positive')
	records_neg.map(json.dumps).saveAsTextFile(output + '/negative')


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit average')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

