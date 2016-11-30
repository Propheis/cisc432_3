package cisc.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduce {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		// The dictionary of positive and negative words
		private static Hashtable<String, Integer> wordSentiments;

		/*
		 * Initialize the MapReduce job - Loads the word lists into memory
		 */
		public TokenizerMapper() {
			
			// Temporary hard-coding until I figure out passing these in as program arguments
			// Both of the file paths are correct ("sentimen") is the username..
			Path positiveWordPath = new Path("hdfs:/user/sentimen/positive.txt");
			Path negativeWordPath = new Path("hdfs:/user/sentimen/negative.txt");
			
			try {
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader pWordReader = new BufferedReader(new InputStreamReader(fs.open(positiveWordPath)));
				BufferedReader nWordReader = new BufferedReader(new InputStreamReader(fs.open(negativeWordPath)));
				
				wordSentiments = readIntoDict(pWordReader, nWordReader);
				
				pWordReader.close();
				nWordReader.close();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(-1);
			}						
		}

		/*
		 * Reads the positive words into wordSentiments with a score of 1
		 * and negative words with a score of -1
		 */
		private Hashtable<String, Integer> readIntoDict(BufferedReader positiveWordReader, BufferedReader negativeWordReader) throws IOException {
			Hashtable<String, Integer> dict = new Hashtable<String, Integer>();
			
			String pLine, nLine;
			
			pLine = positiveWordReader.readLine();
			nLine = negativeWordReader.readLine();
			
			while (pLine != null || nLine != null) {
				if (pLine != null) {
					dict.put(pLine, 1);
					pLine = positiveWordReader.readLine();
				}
				if (nLine != null) {
					dict.put(nLine, -1);
					nLine = negativeWordReader.readLine();
				}
			}
			
			return dict;
		}

		/*
		 * Fetches the sentiment score for a given word.
		 * If word not found, returns 0
		 */
		private Integer getScore(String word) {
			if ( wordSentiments.containsKey(word) )
				return wordSentiments.get(word);
			return 0;
		}


		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			String productId = tokens[1];
			String[] review = tokens[7].split(" ");
			
			// Scaling factor for the quality of the reviewer
			// (high percentage of helpful reviews means this reviewer produces quality reviews.
			float reviewerQuality = Float.parseFloat(tokens[4]) / Float.parseFloat(tokens[5]) + 1;
			
			float overallSentiment = 0;

			for (int i = 0; i < review.length; i++)
				overallSentiment += getScore(review[i]);
			
			overallSentiment *= reviewerQuality;
			
			// Write the sentiment as a value relative to the number of wrods
			context.write(new Text(productId), new IntWritable(Math.round(overallSentiment / review.length)));
		} // End map
} // End TokenizerMapper

public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<Integer> values, Context context) throws IOException, InterruptedException {
		HashMap<Text, Integer> map = new HashMap<Text, Integer>();
		
		Integer accumulator = 0,
				numValues = 0;
		
		for (Integer score : values) {
			accumulator += score;
			numValues++;
		}
		
		// Provide sentiment as a relative value to number of reviews
		accumulator /= numValues;
		
		context.write(key, new IntWritable(accumulator));
	}

	}

public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "Product Sentiment Analysis");
	
	job.setJarByClass(MapReduce.class);
	job.setMapperClass(TokenizerMapper.class);
	job.setCombinerClass(IntSumReducer.class);
	job.setReducerClass(IntSumReducer.class);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}