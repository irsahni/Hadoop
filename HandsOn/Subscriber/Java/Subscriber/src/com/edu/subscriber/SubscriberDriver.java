package com.edu.subscriber;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SubscriberDriver  extends Configured implements Tool{
	
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		//Job job = Job.getInstance(getConf());
		 Job job = new Job(conf);
		 
		  job.setMapperClass(SubscriberMapper.class);
		  job.setReducerClass(SubscriberReducer.class);
		  
		  job.setInputFormatClass(TextInputFormat.class);
		  //job.setMapOutputKeyClass(ActorCompositeKey.class);
		  job.setMapOutputValueClass(DoubleWritable.class);
		  
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(DoubleWritable.class);
		  
		  //job.setNumReduceTasks(2);
		  
		  job.setMapOutputKeyClass(Text.class);


		job.setJarByClass(SubscriberDriver.class);


		  //job.setPartitionerClass(ActualKeyPartitioner.class);
		  //job.setGroupingComparatorClass(ActualKeyGroupingPartitioner.class);
		  //job.setSortComparatorClass(ActorCompositeKeyComparator.class);
		  
		  FileInputFormat.addInputPath(job, new Path(args[0]));
		  //FileOutputFormat.setOutputPath(job, new Path(arg0[0]));
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//  job.waitForCompletion(true);

		  Path outputFilePath = new Path(args[1]);
		/* Delete output filepath if already exists */
		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		SubscriberDriver invertedIndexDriver = new SubscriberDriver();
		int res = ToolRunner.run(invertedIndexDriver, args);
	    System.exit(res);
	}
		
	}


