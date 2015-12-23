package crime;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CrimeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	public int getThreeDigits(int number){
		int div = (int) Math.pow(10, 3);
		while (number / div > 0)
		    number /= 10;
		return number;
	}
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String line = value.toString();
		if(!line.split(",")[0].equals("Crime ID") && line.split(",").length>=8){
			
			int easting = (line.split(",")[4].length()!=0)?Integer.parseInt(line.split(",")[4]):0;
			int northing = (line.split(",")[5].length()!=0)?Integer.parseInt(line.split(",")[5]):0;
			String crime_type = line.split(",")[7];
			String new_key = getThreeDigits(easting)+" "+getThreeDigits(northing)+" "+crime_type;
			context.write(new Text(new_key), new IntWritable(1));			
		}
	}
}
