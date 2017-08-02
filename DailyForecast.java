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
import org.apache.hadoop.util.GenericOptionsParser;

public class DailyForecast  extends Configured implements Tool{
	//public static String usrCh;
	public static int colFlag = 0;
	public static String umonth;
	
	public static class DailyForecastMapper extends MapReduceBase
	implements Mapper <LongWritable, Text, Text, DoubleWritable> {

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter rep) 
				throws IOException {
			//reading each line and splitting by ,
			String line = value.toString();
			String [] data = line.split(",");
			
			Double mag;
			String str_date = data[0].substring(0,10);
			if (data[4].toString() == null || data[4].isEmpty() ) {
				mag = 0.0;
			}
			else {
				mag = Double.parseDouble(data[4].toString());
			}
			output.collect(new Text(str_date), new DoubleWritable(mag));
		}
		
		/* a little method to print debug output
		private void printKeyAndValues(String data, String month)
		{
		  System.err.println(String.format("Data[0].substring(6,7): |(%s)|, umonth: |(%s)|", data, month));
		}*/

	}
	public static class DailyForecastReducer extends MapReduceBase 
	implements Reducer <Text, DoubleWritable, Text, Text> {
		@Override
		public void reduce(Text key, Iterator<DoubleWritable> values,
				OutputCollector<Text,Text> output, Reporter rep)
				throws IOException {
			
			Double sum = 0.0;
			int cnt = 0;
		    			
			if(colFlag==0){
				output.collect(new Text("Date"),new Text("Average"));
				colFlag = 1;
			} 
				
			while(values.hasNext()){
				DoubleWritable value = values.next();
				sum += value.get();
				cnt += 1;
			}
			
			Double avg_mag =  sum/cnt;
			String str_avg = avg_mag+"";
			output.collect(key ,new Text(str_avg));
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
		System.out.println("Umonth:|" + umonth+"|");
		
		JobConf conf = new JobConf(DailyForecast.class); 
	    conf.setJobName("magRangeAnalysis"); 
	    
		conf.setNumMapTasks(mapperCount);
        conf.setNumReduceTasks(reducerCount);
		
		conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(DoubleWritable.class); 
		
	    conf.setMapperClass(DailyForecastMapper.class); 
	    conf.setReducerClass(DailyForecastReducer.class); 
	    
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
