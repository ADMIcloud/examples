package cgl.mr.hadoop.kmeans;

import java.io.IOException;

import java.io.DataInputStream;
import java.io.FileInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KmeansMapper extends Mapper<LongWritable, Text, IntWritable, CopyOfV2DDataWritable> {

	static int VECTOR_SIZE = 21;
	static int NUM_CENTROIDS = IterativeMapReduce.NUM_CENTROIDS;

	double[][] cData;
	double[][] newCData;
	int[] cCounts;

	/**
	 * Mapper configuration.
	 * 
	 */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		// super.setup(context);

		cData = new double[NUM_CENTROIDS][VECTOR_SIZE];
		newCData = new double[NUM_CENTROIDS][VECTOR_SIZE];
		cCounts = new int[NUM_CENTROIDS];

		Configuration configuration = context.getConfiguration();
		loadCentroidsFileFromCache(cData, configuration);
	}

	public static void loadCentroidsFileFromCache(double[][] centroidsData,
			Configuration configuration) throws IOException {
		String cPath = configuration.get("c-file");
		
//		System.out.println("path in "+cPath);
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(configuration);
		if (null != cacheFiles && cacheFiles.length > 0) {
			for (Path cachePath : cacheFiles) {
				if (cachePath.getName().equals(cPath)) {
					FileSystem fs = FileSystem.getLocal(configuration);
					Path path = new Path(cPath);
					SequenceFile.Reader reader = new SequenceFile.Reader(fs,
							path, configuration);
					try {
						IntWritable key = new IntWritable();
						CopyOfV2DDataWritable val = new CopyOfV2DDataWritable();
						double[] centerArrary = new double [21];
						int count = 0;
						while (reader.next(key, val)) {
							centerArrary = val.getValueArrary();
//							centroidsData[count][0] = val.getValueArrary()[0];
//							centroidsData[count][1] = val.getValueArrary()[1];
							//centroidsData[count] = centerArrary;
							for (int i = 0 ; i < 20; i++ ){
								centroidsData[count][i] = centerArrary[i];
								//System.out.println("centroidsData[count][i] = " + centroidsData[count][i]);
							}
							//System.out.println("centroidsData[count][0] = " + centroidsData[count][0]);
							count++;
						}
					} finally {
						reader.close();
					}
					break;
				}
			}
		}
	}

	/**
	 * Map method.
	 * 
	 * @param key
	 * @param val
	 *            not-used
	 * @param out
	 * @param reporter
	 */
	public void map(LongWritable key, Text val, Context context)
			throws IOException, InterruptedException {
		String valStr[] = val.toString().split(" ");
		// data array for each record
		double data[] = new double[VECTOR_SIZE];
		for (int i = 0; i < 20; i++)
			data[i] = (double) Integer.valueOf(valStr[i]);		

		double distance = 0;
		int minCentroid = 0;
		double minDistance = Double.MAX_VALUE;
		for (int i = 0; i < NUM_CENTROIDS; i++) {
			distance = getEuclidean2(cData[i], data);
			if (distance < minDistance) {
				minDistance = distance;
				minCentroid = i;
			}
		}
		for (int i = 0; i < 20; i++){
			newCData[minCentroid][i] += data[i];
		}
		cCounts[minCentroid] += 1;

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		String cDataresult = "";
		try {
			for (int i = 0; i < NUM_CENTROIDS; i++) {
				//for (int j = 0; j < VECTOR_SIZE; j++){
					//cDataresult += Double.toString(newCData[i][j]) + " "; 
				//}
//				context.write(new IntWritable(i), new Text(newCData[i][0] + " "
//						+ newCData[i][1] + " " + cCounts[i]));
				
				// write key value pairs to reduce task, last cell is cCount
				newCData[i][20] = (double) cCounts[i];
				context.write(new IntWritable(i), new CopyOfV2DDataWritable(newCData[i]));
				//System.out.println("cCount = " + cCounts[i]);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public double getEuclidean2(double[] v1, double[] v2) {
		double sum = 0;
		for (int i = 0; i < 20; i++) {
			sum += ((v1[i] - v2[i]) * (v1[i] - v2[i]));
		}
		return sum;
	}
}
