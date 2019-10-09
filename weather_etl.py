
from pyspark.sql import functions, types

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('weather etl').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

# add more functions as necessary

def main(inputs, output):
    # main logic starts here
    
	observation_schema = types.StructType([
    		types.StructField('station', types.StringType()),
    		types.StructField('date', types.StringType()),
    		types.StructField('observation', types.StringType()),
    		types.StructField('value', types.IntegerType()),
    		types.StructField('mflag', types.StringType()),
    		types.StructField('qflag', types.StringType()),
    		types.StructField('sflag', types.StringType()),
    		types.StructField('obstime', types.StringType()),
	])
	

	weather = spark.read.csv(inputs, schema=observation_schema)
	filter1 = weather['qflag'].isNull()
	filter2 = weather['station'].startswith('CA')
	filter3 = weather['observation'] == 'TMAX'
	weather = weather.filter(filter1).filter(filter2).filter(filter3)
	
	weather = weather.withColumn('tmax', weather.value/10) 
	
	etl_data = weather.select('station','date','tmax')   	
	
	etl_data.write.json(output, compression='gzip', mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
