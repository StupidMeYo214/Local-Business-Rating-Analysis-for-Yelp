package com.cs6360.hw1_3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReducerSideJoin {
	
	public static class ReviewMap extends 
	Mapper<LongWritable, Text, Text, Text>{	
		public void map(LongWritable keyIn, Text value, Context context) throws IOException, InterruptedException{
			String [] row = value.toString().split("::");
			String b_ID = row[2];
			//add mark to recognize the source
			Text rate = new Text("rate121::::"+row[3]);
			Text businessID = new Text(b_ID);
			context.write(businessID, rate);
		}
	}
	
	public static class BusinessMap 
	extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String [] row = value.toString().split("::");
			String businessID = row[0];
			String address = row[1];
			String catList = row[2];
			Text newKey = new Text(businessID);
			Text newVal = new Text(address+"::::"+catList);
			context.write(newKey, newVal);
		}
	}	
	
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		ArrayList<Pair2> list = new ArrayList<>();
		HashMap<String, String> map = new HashMap<>();
		public void reduce(Text b_id, Iterable<Text> otherInfo, Context context) throws IOException, InterruptedException{
			String bInfo="";
			double sum = 0;
			double count = 0;
			
			for(Text info : otherInfo){
				String [] curInfo = info.toString().split("::::");
				//comes from review
				if(curInfo[0].equals("rate121")){
					String curRate = curInfo[1];
					Double rate = Double.valueOf(curRate);
					sum += rate;
					count += 1;
				}else{
					//come from business
					if(bInfo.equals("")){
						//address categoryList
						bInfo = curInfo[0] + "\t" + curInfo[1];
					}
				}
			}
			
			if(!bInfo.equals("") && count != 0){
				double avg = sum / count;
				map.put(b_id.toString(), bInfo);
				Pair2 pair = new Pair2(b_id.toString(), avg);
				list.add(pair);
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException{
			Collections.sort(list, new Comparator<Pair2>(){
				@Override
				public int compare(Pair2 o1, Pair2 o2) {
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
				Pair2 p = list.get(i);
				String idString = p.getId();
				String newVal = map.get(idString) + "\t" + String.valueOf(p.getAvg());
				context.write(new Text(idString), new Text(newVal));
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		String [] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
		
		if(otherArgs.length != 3){
			System.out.println("Wrong Command");
			System.exit(2);
		}
		
		Job job = new Job(configuration, "Q2");
		job.setJarByClass(ReducerSideJoin.class);
		
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, ReviewMap.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, BusinessMap.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}
	
}