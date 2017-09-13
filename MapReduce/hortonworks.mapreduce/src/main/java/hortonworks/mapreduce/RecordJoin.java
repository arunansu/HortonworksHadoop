package hortonworks.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RecordJoin {
	
	public static class JoinGroupingComparator extends WritableComparator {
	    public JoinGroupingComparator() {
	        super (ProductIdKey.class, true);
	    }                             

	    public int compare (WritableComparable a, WritableComparable b){
	        ProductIdKey first = (ProductIdKey) a;
	        ProductIdKey second = (ProductIdKey) b;
	                      
	        return first.productId.compareTo(second.productId);
	    }
	}
	
	public static class JoinSortingComparator extends WritableComparator {
	    public JoinSortingComparator()
	    {
	        super (ProductIdKey.class, true);
	    }
	                               
	    @Override
	    public int compare (WritableComparable a, WritableComparable b){
	        ProductIdKey first = (ProductIdKey) a;
	        ProductIdKey second = (ProductIdKey) b;
	                                 
	        return first.compareTo(second);
	    }
	}
	
	public static class SalesOrderDataMapper extends Mapper<LongWritable, Text, ProductIdKey, JoinGenericWritable>{
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {                           
	        String[] recordFields = value.toString().split("\\t");
	        int productId = Integer.parseInt(recordFields[4]);
	        int orderQty = Integer.parseInt(recordFields[3]);
	        double lineTotal = Double.parseDouble(recordFields[8]);
	                                               
	        ProductIdKey recordKey = new ProductIdKey(productId, ProductIdKey.DATA_RECORD);
	        SalesOrderDataRecord record = new SalesOrderDataRecord(orderQty, lineTotal);
	                                               
	        JoinGenericWritable genericRecord = new JoinGenericWritable(record);
	        context.write(recordKey, genericRecord);
	    }
	}
	               
	public static class ProductMapper extends Mapper<LongWritable, Text, ProductIdKey, JoinGenericWritable>{
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String[] recordFields = value.toString().split("\\t");
	        int productId = Integer.parseInt(recordFields[0]);
	        String productName = recordFields[1];
	        String productNumber = recordFields[2];
	                                               
	        ProductIdKey recordKey = new ProductIdKey(productId, ProductIdKey.PRODUCT_RECORD);
	        ProductRecord record = new ProductRecord(productName, productNumber);
	        JoinGenericWritable genericRecord = new JoinGenericWritable(record);
	        context.write(recordKey, genericRecord);
	    }
	}
	
	public static class JoinRecuder extends Reducer<ProductIdKey, JoinGenericWritable, NullWritable, Text>{
	    public void reduce(ProductIdKey key, Iterable<JoinGenericWritable> values, Context context) throws IOException, InterruptedException{
	        StringBuilder output = new StringBuilder();
	        int sumOrderQty = 0;
	        double sumLineTotal = 0.0;
	                                               
	        for (JoinGenericWritable v : values) {
	            Writable record = v.get();
	            if (key.recordType.equals(ProductIdKey.PRODUCT_RECORD)){
	                ProductRecord pRecord = (ProductRecord)record;
	                output.append(Integer.parseInt(key.productId.toString())).append(", ");
	                output.append(pRecord.productName.toString()).append(", ");
	                output.append(pRecord.productNumber.toString()).append(", ");
	            } else {
	                SalesOrderDataRecord record2 = (SalesOrderDataRecord)record;
	                sumOrderQty += Integer.parseInt(record2.orderQty.toString());
	                sumLineTotal += Double.parseDouble(record2.lineTotal.toString());
	            }
	        }
	        
	        if (sumOrderQty > 0) {
	            context.write(NullWritable.get(), new Text(output.toString() + sumOrderQty + ", " + sumLineTotal));
	        }
	    }
	}
	
	public int getPartition(ProductIdKey key, Text value, int numReduceTasks) {
	    return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
	    Job job = Job.getInstance(new Configuration());
	    job.setJarByClass(RecordJoin.class);
	                               
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	                               
	    job.setMapOutputKeyClass(ProductIdKey.class);
	    job.setMapOutputValueClass(JoinGenericWritable.class);
	                               
	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, SalesOrderDataMapper.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ProductMapper.class);
	                              
	    job.setReducerClass(JoinRecuder.class);
	                         
	    job.setSortComparatorClass(JoinSortingComparator.class);
	    job.setGroupingComparatorClass(JoinGroupingComparator.class);
	                               
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	                               
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    boolean status = job.waitForCompletion(true);
	    if(status) {
			System.exit(0);
		}
		else {
			System.exit(1);
		}
	}

}
