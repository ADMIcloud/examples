package cgl.mr.hadoop.kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.FloatWritable.Comparator;

public class CopyOfV2DDataWritable implements WritableComparable{
//	private double value1;
//	private double value2;
	//private int cCount;
	//private static int VECTOR_SIZE = 21;
	private double[] value = new double[21];

	  public CopyOfV2DDataWritable() {}

	  //public CopyOfV2DDataWritable(double value1, double value2) { set(value1,value2); }
	  public CopyOfV2DDataWritable(double[] inValue) { set(inValue); }
	  //public CopyOfV2DDataWritable(double[] inValue, int count) { set(inValue, count); }

	  /** Set the value of this FloatWritable. */
	  //public void set(double value1,double value2) { this.value1 = value1; this.value2=value2; }
	  public void set(double[] inValue) { this.value = inValue; }
	  //public void set(double[] inValue, int count) { this.value = inValue; this.cCount = count; }

	  /** Return the value of this FloatWritable. */
//	  public double getValue1() { return value1; }
//	  public double getValue2() { return value2; }
	  public int getcCount() { return (int) this.value[20]; }
	  
	  public double[] getValueArrary() { return this.value; }

//	  public void readFields(DataInput in) throws IOException {
//	    value1 = in.readDouble();
//	    value2 = in.readDouble();
//	  }
	  
	  public void readFields (DataInput in) throws IOException {
		  for (int i = 0; i< 21; i++){
		    value[i] = in.readDouble();
		  }
	  }	  

	  public void write(DataOutput out) throws IOException {
//	    out.writeDouble(value1);
//	    out.writeDouble(value2);
		  for (int i = 0; i< 21; i++){
			    //value[i] = in.readDouble();
			    out.writeDouble(value[i]);
		  }
	  }

	  /** Returns true iff <code>o</code> is a FloatWritable with the same value. */
//	  public boolean equals(Object o) {
//	    if (!(o instanceof CopyOfV2DDataWritable))
//	      return false;
//	    CopyOfV2DDataWritable other = (CopyOfV2DDataWritable)o;
//	    return (this.value1 == other.value1 && this.value2==other.value2);
//	  }
	  
	  public boolean equals(Object o) {
		    if (!(o instanceof CopyOfV2DDataWritable))
		      return false;
		    CopyOfV2DDataWritable other = (CopyOfV2DDataWritable)o;
		    int count = 0;
		    for (int i = 0; i < value.length; i++){
		    		if (value[i] == other.value[i])
		    			count++;
		    }
		    
		    if (count == value.length)
		    	return true;
		    		
		    return false;
		  }	  

//	  public int hashCode() {
//	    return (int)Double.doubleToLongBits(value1+value2);
//	  }

	  public int hashCode() {
		    double result = 0.0;
		    for (int i = 0; i < 21 ; i++)
		    	result += value[i];
		    return (int) Double.doubleToLongBits(result);
	  }	  

	  /** Compares two FloatWritables. */
//	  public int compareTo(Object o) {
//	    double thisValue1 = value[0];
//	    double thisValue2 = value[1];
//	    double thatValue1 = ((CopyOfV2DDataWritable)o).getValue1();
//	    double thatValue2 = ((CopyOfV2DDataWritable)o).getValue2();
//	    
//	    if(thisValue1<thatValue1 && thisValue2<thatValue2){
//	    	return -1;
//	    }else if(thisValue1>thatValue1 && thisValue2>thatValue2){
//	    	return 1;
//	    }else if(thisValue1==thatValue1 && thisValue2==thatValue2){
//	    	return 0;
//	    }else{
//	    	return 2;
//	    }    
//	
//	  }
	  
	  public int compareTo(Object o) {
		    double[] thisValue1 = value;
//		    double thisValue2 = value[1];
		    double[] thatValue1 = ((CopyOfV2DDataWritable)o).getValueArrary();
//		    double thatValue2 = ((CopyOfV2DDataWritable)o).getValue2();
		    
		    int countLess = 0;
		    int countLarger = 0;
		    int countEq = 0;
		    for (int i = 0; i < 21; i++){
		    	if (thisValue1[i] == thatValue1[i])
		    		countEq++;
		    	
		    	if (thisValue1[i] > thatValue1[i])
		    		countLarger++;
		    	else
		    		countLess++;		
		    }
		    
		    if(countLess == 21)
		    	return -1;
		    else if(countLarger == 21)
		    	return -1;
		    else if (countEq == 21)
		    	return 0;
		    else
		    	return 2;
		    
//		    if(thisValue1 < thatValue1 && thisValue2 < thatValue2){
//		    	return -1;
//		    }else if(thisValue1>thatValue1 && thisValue2>thatValue2){
//		    	return 1;
//		    }else if(thisValue1==thatValue1 && thisValue2==thatValue2){
//		    	return 0;
//		    }else{
//		    	return 2;
//		    }    
		
		  }	  

	  public String toString() {
		  String result = "";
		  for (int i = 0; i < 21 ; i++)
			  result+=Double.toString(value[i]);
	    //return Double.toString(value1)+Double.toString(value2);
		  return result;
	  }

//	  /** A Comparator optimized for FloatWritable. */ 
//	  public static class Comparator extends WritableComparator {
//	    public Comparator() {
//	      super(CopyOfV2DDataWritable.class);
//	    }
//
//	    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2, byte[] b3, int s3, int l3,
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
	    WritableComparator.define(CopyOfV2DDataWritable.class, new Comparator());
	  }
}
