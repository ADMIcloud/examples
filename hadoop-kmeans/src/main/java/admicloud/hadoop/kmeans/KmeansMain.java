package admicloud.hadoop.kmeans;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import admicloud.hadoop.kmeans.utils.CentroidCountWritable;
import admicloud.hadoop.kmeans.utils.KmeansConstants;
import admicloud.hadoop.kmeans.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KmeansMain  extends Configured implements Tool {
	public void launch(int numDataPoints, int vectorSize, int numCentroids, int numMapTasks, int numIteration)
			throws IOException, URISyntaxException {
		Configuration configuration = getConf();
		FileSystem fs = FileSystem.get(configuration);
		
		//0. generate data
		Path dataDir = new Path(KmeansConstants.DATA_DIR);
		if(fs.exists(dataDir)){
			fs.delete(dataDir,true);
		}
		fs.mkdirs(dataDir);
		int numFiles = numMapTasks;
		Utils.generateDataFiles(fs, dataDir,  numDataPoints, vectorSize, numFiles );
		
		//1. generate initial centroids, save to centroids_0 file.
		Path cDir = new Path(KmeansConstants.CENTROIDS_DIR);
		if(fs.exists(cDir)){
			fs.delete(cDir, true);
			fs.mkdirs(cDir);
		}
		Utils.generateInitialCentroids(fs, cDir, numCentroids, vectorSize);
		
		//2. for each iteration, configure a job and launch it.
		Job job = null;
		Path outDir = new Path(KmeansConstants.OUT_DIR);
		
		for(int iter = 0; iter < numIteration; iter++){
			//delete output directory if existed
			if( fs.exists(outDir)){
				fs.delete(outDir, true);
			}
			
			job = configureAJob(configuration, iter, vectorSize, numCentroids,dataDir, outDir );
			
			try {			
				job.waitForCompletion(true);
			}catch (Exception e){
				e.printStackTrace();
			}
			System.out.println("---------------------------| Iteration #" +iter + " Finished |-------------------------------");
		}
		
	}
	
	public Job configureAJob (Configuration configuration, int iter, int vectorSize, int numCentroids, Path dataDir, Path outputDir) throws IOException{
		Job job = Job.getInstance(configuration,"kmeans_"+iter);
		Configuration jobConfig= job.getConfiguration();
		jobConfig.set(KmeansConstants.CENTROID_FILE, KmeansConstants.CENTROID_FILE_PREFIX+iter);
		jobConfig.set(KmeansConstants.ITERATION, String.valueOf(iter));
		jobConfig.setInt(KmeansConstants.SIZE_OF_VECTOR, vectorSize);
		jobConfig.setInt(KmeansConstants.NUM_OF_CENTROIDS, numCentroids);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setJarByClass(KmeansMain.class);
		job.setMapperClass(KmeansMapper.class);
		job.setReducerClass(KmeansReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(CentroidCountWritable.class);
		job.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(job, dataDir);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		return job;
	}
	public int run(String[] args) throws Exception {
		if (args.length != 5) {
			System.err.println("Usage: KmeansMain <num Of Data Points> <size of a vector> <num of Centroids> <number of map tasks> <number of iteration>");			
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		int numOfDataPoints = Integer.parseInt(args[0]);
		int vectorSize =  Integer.parseInt(args[1]);
		int numCentroids = Integer.parseInt(args[2]);
		int numMapTasks = Integer.parseInt(args[3]);
		int numIteration = Integer.parseInt(args[4]);
		
		System.out.println("Number of Data Points = " + numOfDataPoints);
		System.out.println("Size of a Vector = " + vectorSize);
		System.out.println("Num of Centroids = " + numCentroids );
		System.out.println( "Number of Map Tasks = "	+ numMapTasks);
		System.out.println( "Number of Iterations = "	+ numIteration);
		launch(numOfDataPoints, vectorSize, numCentroids, numMapTasks, numIteration);

		return 0;
	}
	
	
	public static void main(String[] argv) throws Exception {
		int res = ToolRunner.run(new Configuration(), new KmeansMain(), argv);
		System.exit(res);
	}
}
