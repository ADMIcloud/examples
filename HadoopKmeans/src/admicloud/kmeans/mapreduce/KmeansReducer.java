package admicloud.kmeans.mapreduce;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import admicloud.kmeans.utils.CentroidCountWritable;
import admicloud.kmeans.utils.KmeansConstants;

public class KmeansReducer extends Reducer<IntWritable, CentroidCountWritable, Writable, Writable> {
	double newCentroids[][];
	int cCounts[];
	int VECTOR_SIZE;
	int NUM_CENTROIDS;
	int iter;
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		//0.0. setup, get configuratoins.
		Configuration configuration = context.getConfiguration();
		VECTOR_SIZE = configuration.getInt(KmeansConstants.SIZE_OF_VECTOR, 3);
		NUM_CENTROIDS = configuration.getInt(KmeansConstants.NUM_OF_CENTROIDS, 10);
		iter=configuration.getInt(KmeansConstants.ITERATION, 0);
		newCentroids = new double[NUM_CENTROIDS][VECTOR_SIZE];
		cCounts = new int[NUM_CENTROIDS];
	}
	/*
	 * key: centroid id
	 * values: the sum of data assigned to this centroid, and the count of these data points.
	 */
	@Override
	public void reduce(IntWritable key, Iterable<CentroidCountWritable> values, Context context)
			throws IOException, InterruptedException {
		//1.1 reduce centroids
		int index = key.get();
		for (CentroidCountWritable value : values) {
			double[] strData = value.getValueArrary();
			for (int i = 0; i < VECTOR_SIZE; i++)
				newCentroids[index][i] += strData[i];
			cCounts[index] += value.getcCount();
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		
		//2.1 output all new centroids. These are final centroids in this iteration.
		Path newCentroidDir = new Path(KmeansConstants.CENTROIDS_DIR);
		Path newCentroidFile = new Path(newCentroidDir, KmeansConstants.CENTROID_FILE_PREFIX+(iter+1));
		
		Configuration configuration = context.getConfiguration();
		FileSystem fs = FileSystem.get(configuration);
		
		if(fs.exists(newCentroidFile))
			fs.delete(newCentroidFile, true);
		
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(newCentroidFile,true)));
		
		String aCentroid;
		for (int i = 0; i < NUM_CENTROIDS; i++) {
			aCentroid="";
			if (cCounts[i] != 0) {
				for (int j = 0 ; j < VECTOR_SIZE ; j++ ){
					newCentroids[i][j] /= (double) cCounts[i];
					if( j != VECTOR_SIZE-1 )
						aCentroid += newCentroids[i][j]+"\t";
					else
						aCentroid += newCentroids[i][j] + "\n";
				}
			}else{
				for (int j = 0 ; j < VECTOR_SIZE ; j++ ){
					if( j != VECTOR_SIZE-1 )
						aCentroid += newCentroids[i][j]+"\t";
					else
						aCentroid += newCentroids[i][j] + "\n";
				}
			}
			br.append(aCentroid);
		}
		br.close();
		
	}
}
