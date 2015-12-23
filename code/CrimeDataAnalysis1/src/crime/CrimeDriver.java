package crime;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CrimeDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length!=2) {
			System.err.println("Usage: crimeDriver <in> <out>");
			System.exit(2);
		}
		System.out.println("In Driver Program");
		System.out.println("Input: "+otherArgs[0]);
		System.out.println("Output: "+otherArgs[1]);
		Job job = new Job(conf,"CrimeDataAnalysis");
		job.setJarByClass(CrimeDriver.class);
		job.setMapperClass(CrimeMapper.class);
		job.setReducerClass(CrimeReducer.class);
		//job.setCombinerClass(CrimeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//job.setNumReduceTasks(5);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		long start = new Date().getTime();
		job.waitForCompletion(true);
		long end = new Date().getTime();
		System.out.println("Job took "+(end-start) + "milliseconds");
		System.exit(0);
	}
}
