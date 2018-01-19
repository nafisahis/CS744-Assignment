import org.apache.hadoop.mapred.MapReduceBase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapred.Reducer;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

public class AnagramSorter {

	public static class AnagramMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> results, Reporter reporter)
				throws IOException {
			Text sortedText = new Text();
			Text orginalText = new Text();
			String word = value.toString();
			char[] wordChars = word.toCharArray();
			Arrays.sort(wordChars);
			String sortedWord = new String(wordChars);
			sortedText.set(sortedWord);
			orginalText.set(word);
			results.collect(sortedText, orginalText);
		}
	}

	public static class AnagramReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text anagramKey, Iterator<Text> anagramValues, OutputCollector<Text, Text> results,
				Reporter reporter) throws IOException {
			Text outputValue = new Text();
			String output = "";
			while (anagramValues.hasNext()) {
				Text anagram = anagramValues.next();
				output = output + anagram.toString() + "~";
			}
			StringTokenizer outputTokenizer = new StringTokenizer(output, "~");
			if (outputTokenizer.countTokens() >= 2) {
				output = output.replace("~", ",");
				//outputKey.set(anagramKey.toString());
				outputValue.set(output);
				results.collect(new Text(), outputValue);
			}
		}

	}

	public static class AnagramMapper2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> results, Reporter reporter)
				throws IOException {
			Text orginalText = new Text();
			Text dummyKey = new Text();
			String word = value.toString();
			orginalText.set(word);
			dummyKey.set("test");
			results.collect(dummyKey, orginalText);
		}
	}

	public static class LengthComparator implements Comparator<String> {
		@Override
		public int compare(String o1, String o2) {
			String trimmed = o1.toString().trim();
			int noOfWords1 = trimmed.isEmpty() ? 0 : trimmed.split(",").length;
			String trimmed2 = o2.toString().trim();
			int noOfWords2 = trimmed2.isEmpty() ? 0 : trimmed2.split(",").length;
			if (noOfWords2 == noOfWords1) {
				return 0;
			}
			return noOfWords1 > noOfWords2 ? -1 : 1;
		}
	}

	public static class AnagramReducer2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text anagramKey, Iterator<Text> anagramValues, OutputCollector<Text, Text> results,
				Reporter reporter) throws IOException {
			Text outputKey = new Text();
			Text outputValue = new Text();
			String output = "";
			ArrayList<String> listOfLines = new ArrayList<String>();
			while (anagramValues.hasNext()) {
				Text anagram = anagramValues.next();
				listOfLines.add(anagram.toString());
			}
			Collections.sort(listOfLines, new LengthComparator());
			StringTokenizer outputTokenizer = new StringTokenizer(output, "~");
			for (String line : listOfLines) {
				line = line.trim();
				line = line.replaceAll(",", " ");
				output += line + "\n";
			}
			outputKey.set("");
			outputValue.set(output);
			results.collect(outputKey, outputValue);

		}

	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(AnagramSorter.class);
		conf.setJobName("anagramcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(AnagramMapper.class);
		conf.setReducerClass(AnagramReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path("/int_output"));
		conf.setJar("ac.jar");
		JobClient.runJob(conf);

		conf.setJobName("anagramsort");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(AnagramMapper2.class);
		conf.setReducerClass(AnagramReducer2.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path("/int_output"));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		conf.setJar("ac.jar");
		JobClient.runJob(conf);

	}

}
