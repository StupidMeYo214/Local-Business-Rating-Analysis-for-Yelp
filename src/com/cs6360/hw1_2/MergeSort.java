package com.cs6360.hw1_2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MergeSort {
	
	public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		
		public void map(LongWritable keyIn, Text value, Context context) throws IOException, InterruptedException{
			String [] row = value.toString().split("::");
			String b_ID = row[2];
			Double rate = Double.valueOf(row[3]);
			Text businessID = new Text(b_ID);
			DoubleWritable rateWritable = new DoubleWritable(rate);
			context.write(businessID, rateWritable);
		}
		
	}
	
	
	public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		ArrayList<Pair> list = new ArrayList<>();
		
		public void reduce(Text b_id, Iterable<DoubleWritable> rates, Context context) throws IOException, InterruptedException{
			double sum = 0;
			double count = 0;
			for(DoubleWritable rate : rates){
				sum += rate.get();
				count += 1;
			}
			double avg = sum / count;
			list.add(new Pair(b_id.toString(), avg));
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException{
			Collections.sort(list, new Comparator<Pair>(){
				@Override
				public int compare(Pair o1, Pair o2) {
					// TODO Auto-generated method stub
					if(o1.getAvg() - o2.getAvg() > 0){
						return -1;
					}else if(o1.getAvg() - o2.getAvg() < 0){
						return 1;
					}else{
						return 0;
					}
				}
			});
			
			for(int i = 0; i < 10; i++){
				Pair p = list.get(i);
				context.write(new Text(p.getId()), new DoubleWritable(p.getAvg()));
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		String [] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
		
		if(otherArgs.length != 2){
			System.out.println("Wrong Command");
			System.exit(2);
		}
		
		Job job = new Job(configuration, "Q2");
		job.setJarByClass(MergeSort.class);
		
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}
	
}