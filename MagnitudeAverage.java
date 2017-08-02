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

public class MagnitudeAverage  extends Configured implements Tool{
	//public static String usrCh;
	public static int colFlag = 0;
	public static String umonth;
	
	public static class MagnitudeAverageMapper extends MapReduceBase
	implements Mapper <LongWritable, Text, Text, DoubleWritable> {

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter rep) 
				throws IOException {
			//reading each line and splitting by ,
			String line = value.toString();
			String [] data = line.split(",");
			
			if(data[0].substring(6,7).equals(umonth)){
				System.out.println("\tMapper Starts Here");
				System.out.println();
				System.out.println("|"+data+"|");
						
				String str_date = data[0].substring(0,10);
				String str_mag = data[4].toString();
				System.out.println("Magnitude: " +str_mag);
			
				Double mag = Double.parseDouble(str_mag);
				
				output.collect(new Text(str_date), new DoubleWritable(mag));
			} else {
				Double mag1 = 0.0;
				String str_date1 = data[0].substring(0,10);
				output.collect(new Text(str_date1), new DoubleWritable(mag1));
			}
		}
	}
	public static class MagnitudeAverageReducer extends MapReduceBase 
	implements Reducer <Text, DoubleWritable, Text, Text> {
		@Override
		public void reduce(Text key, Iterator<DoubleWritable> values,
				OutputCollector<Text,Text> output, Reporter rep)
				throws IOException {
			
			System.out.println("\tReducer Starts Here");
			System.out.println();
			Double sum = 0.0;
			int cnt = 0;
		    			
			if(colFlag==0){
				output.collect(new Text("Date"),new Text("Average"));
				colFlag = 1;
			} else {			
				
				/*while(values.hasNext()){
					IntWritable val = values.next();
					sum += val.get();
				}*/
				//for (DoubleWritable value: values) {
				while(values.hasNext()){
					DoubleWritable value = values.next();
					sum = sum + value.get();
					cnt = cnt + 1;
				}
			
				Double avg_mag =  sum/((double)cnt);
				String str_avg = avg_mag+"";
				output.collect(key ,new Text(str_avg));
			}
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
		
		System.out.println("User Input: " + input[0] + " | " + input[1] + " | " + input[2]);
		
		int mapperCount = Integer.parseInt(input[0]);
		int reducerCount = Integer.parseInt(input[1]);
		umonth = input[2];				
		System.out.println("Umonth: " + umonth);
		
		JobConf conf = new JobConf(MagnitudeAverage.class); 
	    conf.setJobName("magRangeAnalysis"); 
	    
		conf.setNumMapTasks(mapperCount);
        conf.setNumReduceTasks(reducerCount);
		
		conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(DoubleWritable.class); 
		
	    conf.setMapperClass(MagnitudeAverageMapper.class); 
	    conf.setReducerClass(MagnitudeAverageReducer.class); 
	    
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
