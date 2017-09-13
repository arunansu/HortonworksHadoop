package hortonworks.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class ProductIdKey implements WritableComparable<ProductIdKey> {
	public static final IntWritable PRODUCT_RECORD = new IntWritable(0);
	public static final IntWritable DATA_RECORD = new IntWritable(1);
	
	public IntWritable productId = new IntWritable();
	public IntWritable recordType = new IntWritable();
	
	public ProductIdKey(){}
	public ProductIdKey(int productId, IntWritable recordType) {
	    this.productId.set(productId);
	    this.recordType = recordType;
	}
	
	public void write(DataOutput out) throws IOException {
	    this.productId.write(out);
	    this.recordType.write(out);
	}

	public void readFields(DataInput in) throws IOException {
	    this.productId.readFields(in);
	    this.recordType.readFields(in); 
	}
	
	public int compareTo(ProductIdKey other) {
	    if (this.productId.equals(other.productId )) {
	        return this.recordType.compareTo(other.recordType);
	    } else {
	        return this.productId.compareTo(other.productId);
	    }
	}
	
	public boolean equals (ProductIdKey other) {
	    return this.productId.equals(other.productId) && this.recordType.equals(other.recordType );
	}

	public int hashCode() {
	    return this.productId.hashCode();
	}
	
}
