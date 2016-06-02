package cgl.mr.hadoop.kmeans;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;

import java.io.DataOutputStream;
import java.io.FileOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class IterativeMapReduce extends Configured implements Tool {
	
	public static int NUM_CENTROIDS = 1000;
	public static int NUM_ITERATION = 10;
	static int VECTOR_SIZE = 20;

	double launch(int numOfDataPoints, int numCentroids, int numMapTasks, int numIteration, String localInputDir, String jt, String dfs)
			throws IOException, URISyntaxException {
		
		//===================Data and the Settings===================================//
		//int dataLen = 64;
		//int numMapTasks = 4;
		//int numCentroids = 10;
		//String filePrefix="/globalhome/jaliya/projects/cglmr/kmeans_data/new_data/mth_4/256_";
        //String initCentroidData="/globalhome/thilina/hadoop_jars/programs/kmeans_data/init_clusters.txt";
		//String initCentroidData = clustersFile;
        //===========================================================================//
		IterativeMapReduce.NUM_CENTROIDS = numCentroids;
		
        //Copying the data files and the initial centroids to the files in hdfs.
		Configuration configuration = getConf();
		if (jt != null) {
			configuration.set("mapred.job.tracker", jt);
		}
		if (dfs != null) {			
			FileSystem.setDefaultUri(configuration, dfs);
		}
		
		Random random = new Random();
		
		/////////////Added to test the file writing and reading from map//////////
		//First get the file system handler, delete any previous file, add the file
		//and write the data to it, then pass its name as a parameter to jobconf
		Path testDir = new Path("test-my-k");
		FileSystem fs = FileSystem.get(configuration);
		//fs.delete(testDir,true);

		Path vDir = new Path(testDir, "data");
		
		if (!fs.mkdirs(vDir)) {
			throw new IOException("Mkdirs failed to create " + vDir.toString());
		}		
		        
		System.out.println("Generating data..... ");
		double data[][];	
//		int len=data.length;
		int len = numOfDataPoints;
		int perFile=len/numMapTasks;
		//data = new double [numOfDataPoints][VECTOR_SIZE];
		System.out.println("Writing "+perFile+" vectors to a file");
		
		int startPos=0;
		int endPos=0;
		
		String dataText = "";
		int point = 0;
		
		Path dataDir = vDir;
		
		// if there is already existed inputDir Directory, delete it
		fs = FileSystem.get(new URI(dataDir.toString()), configuration);
		if(fs.exists(dataDir))
			fs.delete(dataDir, true);
		// create a new data Dir for generated Properties set
		fs.mkdirs(dataDir);		
		
		File newDir =  new File(localInputDir);
		Path localInput = new Path(localInputDir);
		// create a path for HDFS input Dir 
		Path inputDir = new Path("inputDir"); 
		
		// if there is existed local and HDFS input for the same data set, 
		// then, just run with the existed data
		if (newDir.exists())
		{
			System.out.println("Reuse old data : " + newDir.toString());
		}// make new data set 
		else{
			boolean success = newDir.mkdir();
			if (success) {
					  System.out.println("Directory: " + localInputDir + " created");
			} 
		 
			// create random data points
			for (int k = 0; k < numMapTasks; k++) {
				startPos=k*perFile;
				endPos=(k+1)*perFile;
				if(k==numMapTasks-1){
					endPos=len;
				}
				
				//FileWriter fstream = new FileWriter(localInputDir + File.separator + "data_" + k);
				DataOutputStream out = new DataOutputStream(new FileOutputStream(
						localInputDir + File.separator + "data_" + k));
				//BufferedWriter out = new BufferedWriter(fstream);
				for (int i = startPos; i < endPos; i++) {
					dataText = "";
					for (int j = 0; j < VECTOR_SIZE; j++){
						point = random.nextInt()*1000;
							dataText += point + " ";
					}
						dataText+="\n";
						//out.write(dataText);
						out.writeBytes(dataText);
				}
				out.close();
				System.out.println("Wrote data to file " + k);
			}
			// copy dataset to HDFS
			fs.copyFromLocalFile(localInput, inputDir);
		}
		
		Path cDir = new Path(testDir, "centroids");
		if (fs.exists(cDir))
			fs.delete(cDir, true);	
		if (!fs.mkdirs(cDir)) {
			throw new IOException("Mkdirs failed to create " + cDir.toString());
		}

		data = new double [numCentroids][21]; 
		
		for (int i = 0 ; i < numCentroids; i++){
			for (int j = 0; j < 21; j++){
				data[i][j] = (double) random.nextInt(1000);
				}
			data[i][VECTOR_SIZE] = 0.0;
		}
		
		
		
		Path initClustersFile = new Path(cDir, "centroid_0");
		SequenceFile.Writer cWriter = SequenceFile.createWriter(fs, configuration, initClustersFile, IntWritable.class, CopyOfV2DDataWritable.class, CompressionType.NONE);
		for (int i = 0; i < numCentroids; i++) {
			cWriter.append(new IntWritable(i), new CopyOfV2DDataWritable(data[i]));
		}

		cWriter.close();
		System.out.println("Wrote centroids data to file");

		
		//============================Starting the Iterations==================================//
		System.out.println("Starting Job");
		long startTime = System.currentTimeMillis();
		long iterStartTime;
		double error = 0.0;		
		int count=0;
		do{
			iterStartTime = System.currentTimeMillis();
			Job job = new Job(configuration,"test-kmeans"+count);
			Configuration jobConfig= job.getConfiguration();
			if (jt != null) {
				jobConfig.set("mapred.job.tracker", jt);
			}
			if (dfs != null) {
				FileSystem.setDefaultUri(jobConfig, dfs);
			}
			
			Path outDir = new Path(testDir, "out-"+String.valueOf(count));
//			if (fs.exists(outDir))
//				fs.delete(outDir, false);
//			//fs.mkdirs(outDir);
			
			
			FileInputFormat.setInputPaths(job, inputDir);
			FileOutputFormat.setOutputPath(job, outDir);
			
			Path cFile = new Path(cDir, "centroid_"+count);
			URI cFileURI = new URI(cFile.toString() +
                    "#" + "centroid_"+count);
			DistributedCache.addCacheFile(cFileURI,jobConfig);
			DistributedCache.createSymlink(jobConfig);
			jobConfig.set("c-file", "centroid_"+count);
			jobConfig.set("iteration", String.valueOf(count));
			
			job.setInputFormatClass(TextInputFormat.class);
			
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(CopyOfV2DDataWritable.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);

			job.setJarByClass(IterativeMapReduce.class);
			job.setMapperClass(KmeansMapper.class);
			//job.setCombinerClass(KmeansReducer.class);
			job.setReducerClass(KmeansReducer.class);
		
			/**
			 * How to specify this or do rather do we need to specify this?
			 */
			job.setNumReduceTasks(1);
			
			try {			
				
				job.waitForCompletion(true);
				
				Path inFile = new Path(outDir, "reduce-out");
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, inFile, jobConfig);
				IntWritable key = new IntWritable();
				Text txtError = new Text();
				reader.next(key, txtError);
				reader.close();
				error = Double.valueOf(txtError.toString());
				System.out.println("Error ="+error);
				
				//<<<<<<<<<<<<<<<<<<<<<
				/*
				try {
				
					Path path = new Path("test-my-k/centroids/centroid_"+(count+1));
					SequenceFile.Reader r = new SequenceFile.Reader(fs, path,jobConfig);
					try {
						IntWritable k = new IntWritable();
						V2DDataWritable v = new V2DDataWritable();
						
						while (r.next(k, v)) {			
							System.out.println(v.getValue1()+"  ,  "+v.getValue2());

						}
					} finally {
						r.close();
					}				
					
				} catch (IOException e) {
					throw new RuntimeException(e);
				}*/
							
				
				//>>>>>>>>>>>>>>>>>>>>>>			
				
			} catch (Exception e){
				e.printStackTrace();
				//fs.delete(testDir);
			}
//			job.deleteLocalFiles();
			if (error>1)
				fs.delete(cFile, true);
			
			System.out.println("----------------------------Iteration Messages-------------------------------");
			System.out.println("-----------------------------------------------------------------------------");
				System.out.println("| Iteration #" +count + " Finished in "+ (System.currentTimeMillis() - iterStartTime) / 1000.0 + " seconds |");
			System.out.println("-----------------------------------------------------------------------------");
			System.out.println("");			
			count++;
		}while( error>1 && count<numIteration );
		//============================End all the Iterations==================================//

		System.out.println("Hadoop Kmeans Job Finished in "+ (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
		System.out.println("Number of iterations = " + (count - 1));
		return error;
	
	}

	/**
	 * Launches all the tasks in order.
	 */
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
//		System.err.println("Usage: IterativeMapReduce <data file> <clusters file> <number of map tasks> <num of Centroids> <number of iteration>");
			System.err.println("Usage: IterativeMapReduce <numOfDataPoints> <num of Centroids> <number of map tasks> <number of iteration> <localInputDir>");			
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		int numOfDataPoints = Integer.parseInt(args[0]);
		int numCentroids = Integer.parseInt(args[1]);		
		int numMapTasks = Integer.parseInt(args[2]);
		int numIteration = Integer.parseInt(args[3]);
		String localInputDir = args[4];
		
		System.out.println( "Number of Map Tasks = "	+ numMapTasks);
		System.out.println("Len : " + args.length);
		System.out.println("Args = " + args[0] + " "+ args[1] + " " + args[2] );
		System.out.println("Final Error is "	+ launch(numOfDataPoints, numCentroids, numMapTasks, numIteration, localInputDir, null, null));

		return 0;
	}

	public static void main(String[] argv) throws Exception {
		int res = ToolRunner.run(new Configuration(), new IterativeMapReduce(),
				argv);
		System.exit(res);
	}

	public static double[][] loadDataFromFile(String fileName)
			throws IOException {

		File file = new File(fileName);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		int numRecords = 0;
		String inputLine = null;
		System.out.println("load files " + fileName);
		while ((inputLine = reader.readLine()) != null) {
			numRecords++;
		}
		reader.close();
		System.out.println("Num of Record " + numRecords);
		System.out.println("close files " + fileName);
		if (numRecords > 0) {
			reader = new BufferedReader(new FileReader(file));
			inputLine = reader.readLine();
		}
		String[] vectorValues = inputLine.split(" ");
		
		System.out.println("Len of Vector " + vectorValues.length);
		
		double data[][] = new double[numRecords][vectorValues.length];
		reader.close();

		reader = new BufferedReader(new FileReader(file));

		numRecords = 0;
		while ((inputLine = reader.readLine()) != null) {
			vectorValues = inputLine.split(" ");
			for (int i = 0; i < vectorValues.length; i++) {
				data[numRecords][i] = Double.valueOf(vectorValues[i]);
			}
			numRecords++;
		}

		return data;

	}
	
	private void writeToLocalandUpLoadFile(FileSystem fs, Path dataDirPath,
			String dataFileName, String message) throws IOException {

		try {
			// Create file
			FileWriter fstream = new FileWriter(dataFileName);
			BufferedWriter out = new BufferedWriter(fstream);
			out.write(message);

			out.close();
			// Path outputDirPath = new Path(outputDir);
			//Path output = new Path(dataDirPath, dataFileName);
			//fs.copyFromLocalFile(new Path(dataFileName), output);

		} catch (Exception e) {// Catch exception if any
			System.err.println("Error: " + e.getMessage());
		}
	}	

}
