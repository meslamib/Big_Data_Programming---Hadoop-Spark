
import random

from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary


def add(x,y):
    return x+y

def f(x):
    random.seed()
    iterations = 0
    sum = 0.0
    while sum < 1:
        sum += random.random()
        iterations += 1
    return (iterations)

def main(input_number):
    # main logic starts here
        samples = int(input_number)
        rdd = sc.parallelize(range(samples), numSlices=100)
        rdd_new = rdd.map(f)
        total_iterations = rdd_new.reduce(add)

        print(total_iterations/samples)



if __name__ == '__main__':
    conf = SparkConf().setAppName('euler')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    input_number = sys.argv[1]
    main(input_number)

