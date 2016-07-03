package hortonworks.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WordCountOldAPI {
	
	public static class MyMapper extends MapReduceBase implements 
		Mapper<LongWritable, Text, Text, IntWritable> {
		private Text word = new Text();
		public void map(LongWritable key, Text value, 
					OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while(tokenizer.hasMoreTokens()){
				int doctor_bill=0; 
			    int medicine_bill=0; 
			    int room_bill=0; 
			    int other_allowences=0; 
			    String cust_name=" "; 
			    String room_no=" "; 
			    String date=" "; 
			    if(tokenizer.hasMoreElements()) 
			    { 
			    	date =tokenizer.nextToken(); 
			    } 
			    if(tokenizer.hasMoreElements()) 
			    { 
			    	cust_name =tokenizer.nextToken(); 
			    } 
			    if(tokenizer.hasMoreElements()) 
			    { 
			    	room_no=tokenizer.nextToken(); 
			    } 
			    if(tokenizer.hasMoreElements()) 
			    { 
			    	doctor_bill = Integer.parseInt(tokenizer.nextToken()); 
			    } 
			    if(tokenizer.hasMoreElements()) 
			    { 
			    	medicine_bill = Integer.parseInt(tokenizer.nextToken()); 
			    } 
			    if(tokenizer.hasMoreElements()) 
			    { 
			    	room_bill = Integer.parseInt(tokenizer.nextToken()); 
			    } 
			    if(tokenizer.hasMoreElements()) 
			    { 
			    	other_allowences = Integer.parseInt(tokenizer.nextToken()); 
			    }
			    
			    int total_bill= doctor_bill+medicine_bill+room_bill+other_allowences; 
			    String l =cust_name+" "+room_no; 

			    word.set(l); 
			    IntWritable tbill = new IntWritable(total_bill); 
			    output.collect(word, tbill);
			}
		}
	}
	
	public static class MyReducer extends MapReduceBase implements 
		Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, 
				OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while(values.hasNext()){
				sum += values.next().get();
			}
			output.collect(key,  new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(WordCountOldAPI.class);
		conf.setJobName("wordcount");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(MyMapper.class);
		conf.setCombinerClass(MyReducer.class);
		conf.setReducerClass(MyReducer.class);
		conf.setNumReduceTasks(1);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf,  new Path(args[1]));
		JobClient.runJob(conf);
	}
}
