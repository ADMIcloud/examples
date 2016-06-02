package cgl.mr.hadoop.kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.FloatWritable.Comparator;

public class V2DDataWritable implements WritableComparable{
	private double value1;
	private double value2;
	private double[] value = new double[20];

	  public V2DDataWritable() {}

	  public V2DDataWritable(double value1, double value2) { set(value1,value2); }
	  public V2DDataWritable(double[] inValue) { set(inValue); }

	  /** Set the value of this FloatWritable. */
	  public void set(double value1,double value2) { this.value1 = value1; this.value2=value2; }
	  public void set(double[] inValue) { this.value = inValue; }

	  /** Return the value of this FloatWritable. */
	  public double getValue1() { return value1; }
	  public double getValue2() { return value2; }
	  
	  public double[] getValueArrary() { return this.value; }

//	  public void readFields(DataInput in) throws IOException {
//	    value1 = in.readDouble();
//	    value2 = in.readDouble();
//	  }
	  
	  public void readFields (DataInput in) throws IOException {
		  for (int i = 0; i< 20; i++){
		    value[i] = in.readDouble();
		  }
	  }	  

	  public void write(DataOutput out) throws IOException {
//	    out.writeDouble(value1);
//	    out.writeDouble(value2);
		  for (int i = 0; i< 20; i++){
			    //value[i] = in.readDouble();
			    out.writeDouble(value[i]);
		  }	    
	  }

//	  /** Returns true iff <code>o</code> is a FloatWritable with the same value. */
//	  public boolean equals(Object o) {
//	    if (!(o instanceof V2DDataWritable))
//	      return false;
//	    V2DDataWritable other = (V2DDataWritable)o;
//	    return (this.value1 == other.value1 && this.value2==other.value2);
//	  }
//
//	  public int hashCode() {
//	    return (int)Double.doubleToLongBits(value1+value2);
//	  }
//
	  /** Compares two FloatWritables. */
	  public int compareTo(Object o) {
	    double thisValue1 = this.value1;
	    double thisValue2 = this.value2;
	    double thatValue1 = ((V2DDataWritable)o).getValue1();
	    double thatValue2 = ((V2DDataWritable)o).getValue2();
	    
	    if(thisValue1<thatValue1 && thisValue2<thatValue2){
	    	return -1;
	    }else if(thisValue1>thatValue1 && thisValue2>thatValue2){
	    	return 1;
	    }else if(thisValue1==thatValue1 && thisValue2==thatValue2){
	    	return 0;
	    }else{
	    	return 2;
	    }    
	
	  }
//
//	  public String toString() {
//	    return Double.toString(value1)+Double.toString(value2);
//	  }
//
//	  /** A Comparator optimized for FloatWritable. */ 
//	  public static class Comparator extends WritableComparator {
//	    public Comparator() {
//	      super(V2DDataWritable.class);
//	    }
//
//	    public int compare(byte[] b1, int s1, int l1,byte[] b2, int s2, int l2,byte[] b3, int s3, int l3,
//	                       byte[] b4, int s4, int l4) {
//	      double thisValue1 = readDouble(b1, s1);
//	      double thisValue2 = readDouble(b2, s2);
//	      double thatValue1 = readDouble(b3, s3);
//	      double thatValue2 = readDouble(b4, s4);
//	      if(thisValue1<thatValue1 && thisValue2<thatValue2){
//		    	return -1;
//		    }else if(thisValue1>thatValue1 && thisValue2>thatValue2){
//		    	return 1;
//		    }else if(thisValue1==thatValue1 && thisValue2==thatValue2){
//		    	return 0;
//		    }else{
//		    	return 2;
//		    }    
//	    }
//	  }

	  static {                                        // register this comparator
	    WritableComparator.define(V2DDataWritable.class, new Comparator());
	  }


}
