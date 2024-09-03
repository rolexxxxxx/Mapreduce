package main.java.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopK {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2 && otherArgs.length != 4 && otherArgs.length != 6) {
			System.err.println("Usage: TopK <inputFile> <outputFile> [-k K] [-skip skipPatternFile]");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Top K Words");
		job.setJarByClass(TopK.class);
		job.setMapperClass(TopKmapper.class);
		job.setCombinerClass(TopKreducer.class);
		job.setReducerClass(TopKreducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		for (int i = 2; i < otherArgs.length; i++) {
			if ("-k".equals(otherArgs[i])) {
				conf.set("K", otherArgs[++i]);

			}
			if ("-skip".equals(otherArgs[i])) {
    			job.addCacheFile(new Path(otherArgs[++i]).toUri());
    			job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
    		}
    	}
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
