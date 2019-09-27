/**
* The RedditAverage  program implements an application that 
* determines the average score for each subreddit of the subset of the Reddit Comments Corpus.In the 
* mapper we parse input data which is in JSON format.We extract the values of subreddit and score keys for each    
* record.Using the provided LongPairWritable implementation we pass the pairs of (1,"record value") to each "subreddit" 
* as for the mapper output.I wrote a combiner code which (for each input splits) iterates over and sums over the values of
* ones and scores; and I wrote a reducer which sums over the LongPairWritables for each input split, summing over the
* scores and divding the total  by the number of iterations to calculate the average.This will output (Text , DoubleWritable) pair
* as for the (subreddit, average score). 

* @author  Mohammad Javad Eslamibidgoli (Student #: 301195360)
* @since   2019-09-13 
*/

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONObject;

  

public class RedditAverage extends Configured implements Tool {

	public static class RedditMapper
	extends Mapper<LongWritable, Text, Text, LongPairWritable>{

		private final static LongPairWritable pair = new LongPairWritable();
		private final static long one = 1;



		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			JSONObject record = new JSONObject(value.toString());
	
			String subreddit =(String) record.get("subreddit");
			Integer score = (Integer) record.get("score");
			
			pair.set(one, score);
			context.write(new Text(subreddit), pair);
							
			
		}
	}


        public static class RedditCombiner
        extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
                private LongPairWritable result = new LongPairWritable();

               @Override
                public void reduce(Text key, Iterable<LongPairWritable> values,
                                Context context
                                ) throws IOException, InterruptedException {
                        long sum1 = 0;
                  	long sum2 =0;
                        for (LongPairWritable val : values) {
                                sum1 += val.get_0();
				sum2 += val.get_1(); 
                        }
                        result.set(sum1,sum2);
                        context.write(key, result);
                }
        }


	public static class RedditReducer
	extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
			double sum = 0.0;
			int count = 0; 
			for (LongPairWritable val : values) {
				sum += val.get_1();
				count++;
			}
			result.set(sum/count);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "reddit average");
		job.setJarByClass(RedditAverage.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(RedditMapper.class);
		job.setCombinerClass(RedditCombiner.class);
		job.setReducerClass(RedditReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongPairWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
