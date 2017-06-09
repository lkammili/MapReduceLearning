package com.learning.foo.mapreducelearning;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;


public class WeatherDriver extends Configured implements Tool {
	
	public int run (String[] args) throws Exception
	{
		if (args.length != 2) {
			System.err.println("Usage :Max Temp <input path> <output path>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		conf.setBoolean("Job.MAP_OUTPUT_COMPRESS", true);
		conf.setClass("Job.MAP_OUTPUT_COMPRESS_CODEC", GzipCodec.class,CompressionCodec.class);
		conf.set("mapreduce.map.output.compress", "true");
		conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
		
				
		Job job = new Job(conf);
		
		job.setJarByClass(WeatherDriver.class);
		job.setJobName("WeatherDriver");
		
	
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		/*FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);*/
		
		job.setMapperClass(HighestMapper.class);
		job.setReducerClass(HighestReducer.class);
		/*job.setCombinerClass(HighestCombiner.class);*/

		/*System.exit(job.waitForCompletion(true) ? 0 : 1);*/
		
		if (job.waitForCompletion(true)){
		    return 0;
		} else {
		    return 1; 	
		}
	}
		
	
	 public static void main(String[] args) throws Exception {
	        int res = ToolRunner.run(new Configuration(), new WeatherDriver(), args);
	        System.exit(res);
	    }

}
