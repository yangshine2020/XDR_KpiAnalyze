package cn.com.dtmobile.biz.filter;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class DataJob  {
	

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
//		conf.setBoolean("mapred.output.compress", true);
//		conf.setClass("mapred.output.compression.codec", GzipCodec.class,CompressionCodec.class);
		
//	    FileSystem hdfs = FileSystem.get(conf) ;
//
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//		hdfs.delete(new Path(otherArgs[1]), true) ;


		
//		String date = otherArgs[2];
//
//		if(date == null){
//			throw new Exception("date is null") ;
//		}
//		conf.set("hour", date);
		
		
		Job job = Job.getInstance(conf) ;
		job.setJobName("S1mme");
		job.setJarByClass(DataJob.class);
		job.setMapperClass(DataMap.class);
		job.setReducerClass(DataReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		int reduceNum = 100;
		if (otherArgs.length >2) {
			  reduceNum =Integer.parseInt(otherArgs[2]);
		 }
		job.setNumReduceTasks(reduceNum); 
		MultipleOutputs.addNamedOutput(job, "s1mmeorgn",
				TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "s1uhttporgn",
				TextOutputFormat.class, NullWritable.class, Text.class);


		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(out)){
			// hadoop fs -rm -r skipTrash
			fs.delete(out, true);
			System.out.println(job.getJobName() + "'s old path output id deleted!");
		}
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);


		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
