package cgl.mr.hadoop.kmeans;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansReducer extends
		Reducer<IntWritable, CopyOfV2DDataWritable, WritableComparable, Writable> {

	int VECTOR_SIZE = 21;
	int NUM_CENTROIDS = IterativeMapReduce.NUM_CENTROIDS;

	double[][] cData;

	double[][] newCData;

	int[] cCounts;

	String iteration;
	long startTime;

	/**
	 * Reducer configuration.
	 * 
	 */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		cData = new double[NUM_CENTROIDS][VECTOR_SIZE];
		newCData = new double[NUM_CENTROIDS][VECTOR_SIZE];
		cCounts = new int[NUM_CENTROIDS];

		Configuration configuration = context.getConfiguration();
		iteration=configuration.get("iteration");
		KmeansMapper.loadCentroidsFileFromCache(cData, configuration);

	}

	/**
	 * Reduce method.
	 * 
	 * @param key
	 * @param values
	 * @param reporter
	 */
	@Override
	public void reduce(IntWritable key, Iterable<CopyOfV2DDataWritable> values, Context context)
			throws IOException, InterruptedException {
		this.startTime = System.currentTimeMillis();
		int index = key.get();
		//String strVal;
		//String strData[];
		for (CopyOfV2DDataWritable value : values) {
			//strVal = value.toString();
			//strData = strVal.split(" ");
			// change by stephen			
			//newCData[index] = value.getValueArrary();
			double[] strData = value.getValueArrary();
			for (int i = 0; i < 20; i++)
				newCData[index][i] += strData[i];
//			newCData[index][1] += Double.valueOf(strData[1]);
			//cCounts[index] += Integer.valueOf(strData[strData.length-1]);
			cCounts[index] += value.getcCount();
			//System.out.println("cCount = " + cCounts[index] + ", newCData[index] = " + newCData[0]);
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {

		// Calculates the new Centroids
		for (int i = 0; i < NUM_CENTROIDS; i++) {
			if (cCounts[i] != 0) {
				// change by stephen
				for (int j = 0 ; j < 20 ; j++ ){
					newCData[i][j] /= (double) cCounts[i];
					//System.out.println("newCData[i][j] = " + newCData[i][j] + ", cCount = " + cCounts[i]);
				}
////				newCData[i][1] /= (double) cCounts[i];
//				System.out.println("newCData[i][0] = " + newCData[i][0] + ", cCount = " + cCounts[i]);
			}

		}

		// Get the total error
		double totalError = getError(cData, newCData);

		Path tmpDir = new Path("test-my-k");
		Path cPath = new Path(tmpDir, "centroids");

		// Write the new centroids.
		System.out.println("iteration = " + iteration);
		Path cFile = new Path(cPath, "centroid_"+(Integer.parseInt(iteration)+1));

		//System.out.println("centr out path "+cFile.getName());
		
		// Delete the old centroid file.
		Configuration configuration = context.getConfiguration();
		FileSystem fileSys = FileSystem.get(configuration);
		fileSys.delete(cFile, true);

		SequenceFile.Writer cWriter = SequenceFile.createWriter(fileSys,
				configuration, cFile, IntWritable.class, CopyOfV2DDataWritable.class,
				CompressionType.NONE);
		for (int i = 0; i < NUM_CENTROIDS; i++) {
			cWriter.append(new IntWritable(i), new CopyOfV2DDataWritable(
					newCData[i]));	
//			cWriter.append(new IntWritable(i), new V2DDataWritable(
//					newCData[i][0], newCData[i][1]));
		}
		cWriter.close();

		// Write the new error

		Path outDir = new Path(tmpDir, "out-" + iteration);
		Path outFile = new Path(outDir, "reduce-out");

		SequenceFile.Writer writer = SequenceFile.createWriter(fileSys,
				configuration, outFile, IntWritable.class, Text.class,
				CompressionType.NONE);
		
		System.out.println("totalError = " + totalError);
		writer.append(new IntWritable(0), new Text(String.valueOf(totalError)));
		writer.close();
		
		System.out.println("Reduce Finished in "+ (System.currentTimeMillis() - this.startTime) / 1000.0 + " seconds");
	}

	private double getError(double[][] centroids, double[][] newCentroids) {
		double totalError = 0;
		for (int i = 0; i < NUM_CENTROIDS; i++) {
			totalError += getEuclidean2(centroids[i], newCentroids[i]);
		}
		return totalError;
	}

	public double getEuclidean2(double[] v1, double[] v2) {
		double sum = 0;
		for (int i = 0; i < 20 ; i++) {
			sum += ((v1[i] - v2[i]) * (v1[i] - v2[i]));
		}
		// return Math.sqrt(sum);
		return sum;

	}
}
