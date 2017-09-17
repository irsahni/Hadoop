package com.edu.subscriber;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SubscriberMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	@SuppressWarnings("unused")
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String customerid;
		double bytes ;		
		
		//subId=00001111911128052627towerid=11232w34532543456345623453456984756894756bytes=122112212212212218.4621702216543667E17
		String line = value.toString();
		
		StringTokenizer token = new StringTokenizer(line);
		while(token.hasMoreTokens())
		{
							
			if(line==null)
				context.write(new Text("0"), new DoubleWritable(0));
			else
			{
				//customerid = line.substring(line.lastIndexOf("subid="), line.indexOf("towerid=")).toString();
				//bytes = Double.parseDouble(line.substring(line.lastIndexOf("bytes=")).toString());
				customerid = line.substring(15,26);
				bytes = Double.parseDouble(line.substring(87,97));
				context.write(new Text(customerid), new DoubleWritable(bytes));
			}				
		}
	}
}
