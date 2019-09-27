/**
* The WordCountImproved program implements an application that
* first update the mapper from the original WordCount code to produce
* (Text, LongWritable) pairs; secondly, I simplified the original code by removing the inner class 
* IntSumReducer and importing the predefined reducer class (LongSum Reducer) that outputs the sum
* of integer values associated with each reducer input key.Finally, I used a regular 
* expression along with the Pattern.split() method to improve the WordCount code
* in which StringTokenizer could not correctly capture the individual words.Moreover, I used
* .toLowerCase() and .trim() to ignore case and ignore any length 0 words, respectively.   
*
* @author  Mohammad Javad Eslamibidgoli (Student #: 301195360)
* @since   2019-09-13 
*/

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import java.util.regex.Pattern;

public class WordCountImproved extends Configured implements Tool {

        public static class MyMapper
        extends Mapper<LongWritable, Text, Text, LongWritable>{
	
		private final static LongWritable one = new LongWritable(1);
		

		@Override	
                public void map(LongWritable key, Text value, Context context
                                ) throws IOException, InterruptedException {
			Pattern word_sep = Pattern.compile("[\\p{Punct}\\s]+");
			String line = value.toString();
                        String[] words = word_sep.split(line);
                        for (String word : words) {
                  		Text wordKey = new Text(word.toLowerCase().trim());
                                context.write(wordKey, one);
                        }
                }
        }



	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCountImproved(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "word count improved");
		job.setJarByClass(WordCountImproved.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(LongSumReducer.class);
		job.setReducerClass(LongSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
