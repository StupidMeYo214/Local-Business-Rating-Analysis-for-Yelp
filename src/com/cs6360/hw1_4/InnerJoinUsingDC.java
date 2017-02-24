package com.cs6360.hw1_4;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
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

public class InnerJoinUsingDC {
	@SuppressWarnings("deprecation")
	public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		HashSet<String> set = new HashSet<>();
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			
			Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			// 我们这里只缓存了一个文件，所以取第一个即可，创建BufferReader去读取
			BufferedReader reader = new BufferedReader(new FileReader(paths[0].toString()));

			String line = null;
			try {
				// 一行一行读取
				while ((line = reader.readLine()) != null) {
					String [] row = line.split("::");
					String businessID = row[0];
					String address = row[1];
					if(address.contains("Stanford")){
						set.add(businessID);
					}	
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				reader.close();
			}	
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String rowIn = value.toString();
			String [] row = rowIn.split("::");
			String businessID = row[2];
			String userID = row[1];
			Double star = Double.valueOf(row[3]);
			if(set.contains(businessID)){
				context.write(new Text(userID), new DoubleWritable(star));
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			double count = 0;
			double sum = 0;
			for(DoubleWritable val : values){
				double cur = val.get();
				sum += cur;
				count += 1;
			}
			double avg = sum/count;
			context.write(key, new DoubleWritable(avg));
		}
	}
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration configuration = new Configuration();
			
		String [] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
		
		if(otherArgs.length != 3){
			System.out.println("<Chachefile> <review.csv> <out>");
			System.exit(2);
		}

		FileSystem fileSystem = FileSystem.get(new URI(otherArgs[2]), configuration);
		if (fileSystem.exists(new Path(otherArgs[2]))) {
			fileSystem.delete(new Path(otherArgs[2]), true);
		}
		DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), configuration);

		
		Job job = new Job(configuration, "Inmem Join");
		job.setJarByClass(InnerJoinUsingDC.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		
		System.exit(job.waitForCompletion(true)? 0 : 1);
		
	}
	
}
