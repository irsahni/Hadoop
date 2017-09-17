package com.edu.subscriber;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SubscriberReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	
	public void reduce(Text key, Iterator<DoubleWritable> value, Context context)
			throws IOException, InterruptedException {
		
		double _TotalBytes = 0;
		
		while(value.hasNext())
		{
			_TotalBytes +=value.next().get();						
		}		
		context.write(key, new DoubleWritable(_TotalBytes));
	}

}
