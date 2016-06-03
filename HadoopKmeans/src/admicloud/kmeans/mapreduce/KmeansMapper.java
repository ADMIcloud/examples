package admicloud.kmeans.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import admicloud.kmeans.utils.CentroidCountWritable;
import admicloud.kmeans.utils.KmeansConstants;
import admicloud.kmeans.utils.Utils;


public class KmeansMapper extends Mapper<LongWritable, Text, IntWritable, CentroidCountWritable> {
	int VECTOR_SIZE;
	int NUM_CENTROIDS;
	
	double[][] centroids;
	double[][] newCentroids;
	int[] cCounts;
	String centroidFile;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		//0.0. setup, get configuratoins.
		Configuration configuration = context.getConfiguration();
		centroidFile =configuration.get(KmeansConstants.CENTROID_FILE);
		VECTOR_SIZE = configuration.getInt(KmeansConstants.SIZE_OF_VECTOR, 3);
		NUM_CENTROIDS = configuration.getInt(KmeansConstants.NUM_OF_CENTROIDS, 10);
		centroids = new double[NUM_CENTROIDS][VECTOR_SIZE];
		newCentroids = new double[NUM_CENTROIDS][VECTOR_SIZE+1];
		cCounts = new int[NUM_CENTROIDS];
		System.out.println("loading centroids");
		//0.1. load centroids from a centroid file.
		loadCentroids(centroids, configuration, VECTOR_SIZE);
		System.out.println("finished centroids");
	}

	/*
	 * key: the position in the input file, non-use here; 
	 * val: a line of the text in the input file
	 */
	public void map(LongWritable key, Text val, Context context)
			throws IOException, InterruptedException {

		//1.2 construct the input data point
		String valStr[] = val.toString().split("\t");
		double data[] = new double[VECTOR_SIZE];
		for (int i = 0; i < VECTOR_SIZE; i++)
			data[i] = (double) Integer.parseInt(valStr[i]);		

		//1.3 find nearest centroid for the input data point.
		double distance = 0;
		int minCentroid = 0;
		double minDistance=0;
		for (int i = 0; i < NUM_CENTROIDS; i++) {
			distance = Utils.getEuclidean2(centroids[i], data, VECTOR_SIZE);
			if(i == 0){
				minDistance = distance;
				minCentroid = i;
			}
			else if (distance < minDistance) {
				minDistance = distance;
				minCentroid = i;
			}
		}
		for (int i = 0; i < VECTOR_SIZE; i++){
			newCentroids[minCentroid][i] += data[i];
		}
		cCounts[minCentroid] += 1;
	}
	
	
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		try {
			//2.1 output 
			//a key is a centroid id
			//a value is the sum of data assigned to this centroid, and the count of these data points.
			for (int i = 0; i < NUM_CENTROIDS; i++) {
				context.write(new IntWritable(i), new CentroidCountWritable(newCentroids[i], VECTOR_SIZE, cCounts[i]));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	/*
	 * load centroids from centroidFile
	 */
	private void loadCentroids(double[][] centroids, Configuration configuration, int vectorSize) throws IOException {
		
		 FileSystem fs = FileSystem.get(configuration);
		 Path pt=new Path(KmeansConstants.CENTROIDS_DIR+"/"+centroidFile);
         BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
         String line;
         line=br.readLine();
         int row = 0;
         while (line != null){
                 String lineSplit[] = line.split("\t");
                 if(lineSplit.length != vectorSize){
                	 System.out.println("load centroids failed.");
                	 System.exit(-1);
                 }
                 
                 for(int i=0; i < vectorSize; i++){
                	 centroids[row][i] = Double.parseDouble(lineSplit[i]);
                 }
                 line=br.readLine();
                 row ++;
         }
	}
}
