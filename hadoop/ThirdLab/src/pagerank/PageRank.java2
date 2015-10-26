package pagerank;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

/*
 * VERY IMPORTANT 
 * 
 * Each time you need to read/write a file, retrieve the directory path with conf.get 
 * The paths will change during the release tests, so be very carefully, never write the actual path "data/..." 
 * CORRECT:
 * String initialVector = conf.get("initialRankVectorPath");
 * BufferedWriter output = new BufferedWriter(new FileWriter(initialVector + "/vector.txt"));
 * 
 * WRONG
 * BufferedWriter output = new BufferedWriter(new FileWriter(data/initialVector/vector.txt"));
 */

public class PageRank {
	
	public static void createInitialRankVector(String directoryPath, long n) throws IOException 
	{

		File dir = new File(directoryPath);
		if(! dir.exists())
			FileUtils.forceMkdir(dir);
		
		//TO DO			
	}
	
	public static boolean checkConvergence(String initialDirPath, String iterationDirPath, double epsilon)
	{
		//TO DO
		// you need to use the L1 norm 
		return false;
	}
	
	public static void avoidSpiderTraps(String vectorDirPath, long nNodes, double beta) 
	{
		//TO DO
		
	}
	
	public static void iterativePageRank(Configuration conf) 
			throws IOException, InterruptedException, ClassNotFoundException
	{
		
		
		String initialVector = conf.get("initialVectorPath");
		String currentVector = conf.get("currentVectorPath");
		
		String finalVector = conf.get("finalVectorPath"); 
		/*here the testing system will search for the final rank vector*/
		
		Double epsilon = conf.getDouble("epsilon", 0.1);
		Double beta = conf.getDouble("beta", 0.8);

 
		//TO DO

		// to retrieve the number of nodes use long nNodes = conf.getLong("numNodes", 0); 

		

		// when you finished implementing delete this line
		throw new UnsupportedOperationException("Implementation missing");
		
	}
}
