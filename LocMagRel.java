import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.*;


public class LocMagRel {
	
	public static class LocMagRelMapper extends MapReduceBase
	implements Mapper <LongWritable, Text, Text, DoubleWritable> {
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter rep) 
			throws IOException {
			
				//reading each line and splitting by ,
				String line = value.toString();
				String [] data = line.split(",");
				
				String location = data[21];
				Double mag;
				
				if (data[4].toString() == null || data[4].isEmpty() ) {
					mag = 0.0;
				}
				else {
					mag = Double.parseDouble(data[4].toString());
				}
				output.collect(new Text(location), new DoubleWritable(mag));
			}
		
		}
	
	
	public static class LocMagRelReducer extends MapReduceBase
	implements Reducer <Text, DoubleWritable, Text, DoubleWritable> {
		
		@Override
		public void reduce(Text key, Iterator<DoubleWritable> values,
				OutputCollector<Text, DoubleWritable> output, Reporter rep)
				throws IOException {
					int count = 0;
					double range = 0.0;
					while(values.hasNext()) {
						count += 1;
						DoubleWritable val = values.next();
						range += val.get();
					}
					
					double avg = range/count;
					output.collect(key, new DoubleWritable(avg));
			}
		}
		
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			// TODO Auto-generated method stub
			JobConf conf = new JobConf(LocMagRel.class); 
		      
		      conf.setJobName("locationMagAnalysis"); 
		      
		      //Set number of Mappers and Reducer
		      conf.setNumMapTasks(4);
		      conf.setNumReduceTasks(1); 
		      
		      conf.setOutputKeyClass(Text.class);
		      conf.setOutputValueClass(DoubleWritable.class); 
		      
		      conf.setMapperClass(LocMagRelMapper.class); 
		      conf.setReducerClass(LocMagRelReducer.class); 
		      
		      conf.setInputFormat(TextInputFormat.class); 
		      conf.setOutputFormat(TextOutputFormat.class); 
		      
		      FileInputFormat.setInputPaths(conf, new Path(args[0])); 
		      FileOutputFormat.setOutputPath(conf, new Path(args[1])); 
		      
		  	  //calling the run method and calculating time
			  long StartTime = System.currentTimeMillis();
			  
		      JobClient.runJob(conf); 
		      
		      long EstimatedTime = System.currentTimeMillis() - StartTime;
		      System.out.println("Time taken: "+EstimatedTime+ " ms");
	    
		}
}
