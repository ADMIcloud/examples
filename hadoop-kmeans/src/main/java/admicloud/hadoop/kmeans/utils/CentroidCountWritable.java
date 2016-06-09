package admicloud.hadoop.kmeans.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class CentroidCountWritable implements Writable{
	
	  private int vectorSize;
	  private double[] value;
	  private int count;
	  
	  public CentroidCountWritable(){
		  
	  }

	  public CentroidCountWritable( int vectorSize) { 
		  this.vectorSize = vectorSize;
		  value = new double[vectorSize];
		  this.count = 0;
	  }
	  
	  public CentroidCountWritable(double[] inValue, int vectorSize, int count) { 
		  this.value = inValue;
		  this.vectorSize = vectorSize;
		  this.count = count;
	  }

	  public int getcCount() { return (int) this.count; }
	  
	  public double[] getValueArrary() { return this.value; }
	  
	  public void readFields (DataInput in) throws IOException {
		  vectorSize = in.readInt();
		  value = new double[vectorSize];
		  for (int i = 0; i< vectorSize; i++){
		    value[i] = in.readDouble();
		  }
		  count = in.readInt();
	  }	  

	  public void write(DataOutput out) throws IOException {
		  out.writeInt(vectorSize);
		  for (int i = 0; i< vectorSize; i++){
			    out.writeDouble(value[i]);
		  }
		  out.writeInt(count);
	  }

	  public String toString() {
		  String result = "";
		  for (int i = 0; i < vectorSize ; i++)
			  result+= Double.toString(value[i]) +"\t";
		  return result;
	  }
}
