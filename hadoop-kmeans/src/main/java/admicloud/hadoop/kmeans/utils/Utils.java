package admicloud.hadoop.kmeans.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Utils {

	public static void generateDataFiles(FileSystem fs, Path dataDir, int numOfDataPoints, int vectorSize, int numFiles) throws IOException{
		Random random = new Random();
		int dataPerFile = numOfDataPoints / numFiles;
		int remainder = numOfDataPoints % numFiles;
		BufferedWriter br = null;
		Path pt = null;
		int dataThisFile = 0;
		for(int i=0; i < numFiles; i++){
			if(remainder > 0){
				dataThisFile = dataPerFile + 1;
				remainder -= 1;
			}else{
				dataThisFile = dataPerFile;
			}
			
			pt = new Path(KmeansConstants.DATA_DIR + "/data_"+i);
			br = new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
			String aData="";
			for(int k = 0; k < dataThisFile; k++){
				aData = "";
				for(int j = 0; j < vectorSize; j++){
					int aElement = random.nextInt(1000);
					if( j != vectorSize-1 ){
						aData += aElement + "\t";
					}
					else{
						aData += aElement+ "\n";
					}
				}
				br.append(aData);
			}
			br.close();
			System.out.println("wrote to "+pt.getName());
		}
	}

	public static void generateInitialCentroids(FileSystem fs, Path cDir, int numCentroids, int vectorSize) throws IOException {
		Random random = new Random();
		Path pt = new Path(cDir, KmeansConstants.CENTROID_FILE_PREFIX+0);
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
		String aCentroid="";
		for(int k = 0; k < numCentroids; k++){
			aCentroid = "";
			for(int j = 0; j < vectorSize; j++){
				int aElement = random.nextInt(1000);
				if( j != vectorSize-1 ){
					aCentroid += aElement + "\t";
				}
				else{
					aCentroid += aElement + "\n";
				}
			}
			br.append(aCentroid);
		}
		br.close();
		System.out.println("wrote to "+pt.getName());
	}
	
	public static double getEuclidean2(double[] v1, double[] v2, int vectorSize) {
		double sum = 0;
		for (int i = 0; i < vectorSize; i++) {
			sum += ((v1[i] - v2[i]) * (v1[i] - v2[i]));
		}
		return sum;
	}
}
