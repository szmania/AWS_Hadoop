import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.Scanner;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MagnitudeRange extends Configured implements Tool{
	//public static String usrCh;
	public static int colFlag = 0;
	
	
	public static class MagnitudeRangeMapper extends MapReduceBase
	implements Mapper <LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter rep) 
				throws IOException {
			//reading each line and splitting by ,
			String line = value.toString();
			String [] data = line.split(",");
			
			System.out.println("\tMapper Starts Here");
			System.out.println();
			System.out.println("|"+data+"|");
			
			String str_mag = data[4].toString();
			System.out.println("Magnitude: " +str_mag);
			
			Double mag = Double.parseDouble(str_mag);
			String mag_range;
			
			if(mag>=1.00&&mag<2.00){
				mag_range = "1-2";
			} else if(mag>=2.00&&mag<3.00){
				mag_range = "2-3";
			} else if(mag>=3.00&&mag<4.00){
				mag_range = "3-4";
			} else if(mag>=4.00&&mag<5.00){
				mag_range = "4-5";
			} else if (mag>=5.00&&mag<6.00){
				mag_range = "5-6";
			} else{
				mag_range = "0-0";
			}
			
			int count =1;
			
			output.collect(new Text(mag_range), new IntWritable(count));
		}
	}
	public static class MagnitudeRangeReducer extends MapReduceBase 
	implements Reducer <Text, IntWritable, Text, Text> {
		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text,Text> output, Reporter rep)
				throws IOException {
			
			System.out.println("\tReducer Starts Here");
			System.out.println();
			int sum = 0;
		    			
			if(colFlag==0){
				output.collect(new Text("Range"),new Text("Total"));
				colFlag = 1;
			} 
			while(values.hasNext()){
				IntWritable val = values.next();
				sum += val.get();
			}
			String str_sum = sum+"";
			output.collect(key ,new Text(str_sum));
			}
	}

    //@Override
    public int run(String[] args) throws IOException
    {
        return 0;
    }

	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		/*Scanner s = new Scanner(System.in);
	    System.out.print("\nEnter Col: ");
	    String col = s.next();
		System.out.println("User Input: " + col);*/
		
		BufferedReader br = new BufferedReader(new FileReader("/home/ubuntu/user_input/uip.txt"));
		String line = br.readLine();
		
		String [] input = line.split("_");
		
		System.out.println("User Input: " + input[0] + " | " + input[1]);
		int mapperCount = Integer.parseInt(input[0]);
		int reducerCount = Integer.parseInt(input[1]);
				
		JobConf conf = new JobConf(MagnitudeRange.class); 
	    conf.setJobName("magRangeAnalysis"); 
	    
		conf.setNumMapTasks(mapperCount);
        conf.setNumReduceTasks(reducerCount);
		
		conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(IntWritable.class); 
		
	    conf.setMapperClass(MagnitudeRangeMapper.class); 
		//conf.setCombinerClass(MagnitudeRangeReducer.class);
	    conf.setReducerClass(MagnitudeRangeReducer.class); 
	    
		conf.setInputFormat(TextInputFormat.class); 
	    conf.setOutputFormat(TextOutputFormat.class); 
	      
	    FileInputFormat.setInputPaths(conf, new Path(args[0])); 
	    FileOutputFormat.setOutputPath(conf, new Path(args[1])); 
	    
		long startTime = System.currentTimeMillis();
		JobClient.runJob(conf); 
		long totalTime = System.currentTimeMillis()- startTime;
		System.out.println("\t Total Execution Time:\t"+ (totalTime/1000)+" seconds");
		
	}
}
