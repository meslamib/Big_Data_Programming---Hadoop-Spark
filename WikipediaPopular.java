/**
* The WikiPediaPopular program implements an applilcation that finds the number of times the most-visited page of wikipedia visited each hour.The mapper maps the particular hour and page views as key value pair.We only want to report English Wikipedia pages, remove those with title == "Main_Page" and title.startsWith("Special:")   

* @author  Mohammad Javad Eslamibidgoli (Student #: 301195360)
* @since   2019-09-20 
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
  

public class WikipediaPopular extends Configured implements Tool {

	public static class WikiPediaMapper
	extends Mapper<LongWritable, Text, Text, IntWritable>{
	

		private Text time = new Text();
		private IntWritable count = new IntWritable();


		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {

					
                        String line = value.toString();
                        String[] fields = line.split("\\s+");
			
			if (fields[1].equals("en") && !fields[2].equals("Main_Page") && !fields[2].startsWith("Special:")){							
					
			time.set(fields[0]);
			count.set(Integer.parseInt(fields[3]));
				
			context.write(time, count);
						
			}
		}
	}


   
	public static class WikiPediaReducer
	extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
		 
			int max = 0;

			Iterator<IntWritable> iterator = values.iterator(); 

			while (iterator.hasNext()) {

				int value = iterator.next().get();

				if (value > max) { 

					max = value;

					}

				
			}
			result.set(max);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "wikipedia popular");
		job.setJarByClass(WikipediaPopular.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(WikiPediaMapper.class);
		job.setReducerClass(WikiPediaReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
