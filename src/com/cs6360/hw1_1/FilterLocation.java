package com.cs6360.hw1_1;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.omg.CORBA.PRIVATE_MEMBER;

public class FilterLocation {
	public static class Map 
	extends Mapper<LongWritable, Text, Text, Text>{
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String [] columns = value.toString().split("::");
			String addressString = columns[1];
			if(addressString.contains("Palo Alto")){
				Text address = new Text(); 
				address.set("Palo Alto");
				
				String catListString = columns[2].substring(5, columns[2].length() - 1);
				String [] catList = catListString.split(", ");
				
				for(String cat : catList){
					if(cat.equals("")){
						continue;
					}
					Text category = new Text();
					category.set(cat);
					context.write(category, address);
				}
			}
		}
	}
	
	public static class Reduce
	extends Reducer<Text, Text, Text, Text>{
		HashSet<String> set = new HashSet<>();
		
		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException{
			for(Text val : values){
				boolean success = set.add(key.toString());
				if(success){
					context.write(key, new Text(""));
				}
			}
		}
	}
	
	public static void main(String [] args) throws Exception{
		Configuration conf = new Configuration();
		String [] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(otherArgs.length != 2){
			System.out.println("Usage: WordCount <in><out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "Q1");
		job.setJarByClass(FilterLocation.class);
		
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}
}
