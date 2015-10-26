package pagerank;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

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

	public static void createInitialRankVector(String directoryPath, long n) throws IOException {

		File dir = new File(directoryPath);
		if (!dir.exists())
			FileUtils.forceMkdir(dir);
		BufferedWriter output = new BufferedWriter(new FileWriter(dir + "/part-r-00000"));
		int i = 0;
		while (i < n) {
			double value = (double) 1.0 / (double) n;
			int iRow = i + 1;
			output.write(iRow + " " + value);
			output.write("\n");
			i++;
		}
		output.close();

	}

	public static boolean checkConvergence(String initialDirPath, String iterationDirPath, double epsilon)
			throws IOException {
		File initDir = new File(initialDirPath);
		File iterDir = new File(iterationDirPath);
		BufferedReader readerInit = new BufferedReader(new FileReader(initDir + "/part-r-00000"));
		BufferedReader readerIter = new BufferedReader(new FileReader(iterDir + "/part-r-00000"));
		String line1;
		Double sum = 0.0;
		while ((line1 = readerInit.readLine()) != null) {
			String[] tokens1 = line1.split("\\s");
			String line2 = readerIter.readLine();
			String[] tokens2 = line2.split("\\s");
			Double iterx = Double.parseDouble(tokens2[1]);
			Double x = Double.parseDouble(tokens1[1]);
			Double diff = iterx - x;
			sum += Math.abs(diff);
		}
		readerInit.close();
		readerIter.close();
		return (sum < epsilon);
	}

	public static void avoidSpiderTraps(String vectorDirPath, long nNodes, double beta) throws IOException {
		File dir = new File(vectorDirPath);
		BufferedReader reader = new BufferedReader(new FileReader(dir + "/part-r-00000"));
		BufferedWriter output = new BufferedWriter(new FileWriter(dir + "/vector1.txt"));
		String line1;
		while ((line1 = reader.readLine()) != null) {
			String[] tokens = line1.split("\\s");
			Double x = Double.parseDouble(tokens[1]);
			Double newX = x * beta + (double) (1 - beta) / (double) nNodes;
			output.write(tokens[0] + " " + newX + "\n");
		}
		output.close();
		reader.close();
		FileUtils.copyFile(new File(dir + "/vector1.txt"), new File(dir + "/part-r-00000"));
		FileUtils.deleteQuietly(new File(dir + "/vector1.txt"));
		FileUtils.deleteQuietly(new File(dir + "/.part-r-00000.crc")); // Pb
																		// checksum
	}

	public static void iterativePageRank(Configuration conf)
			throws IOException, InterruptedException, ClassNotFoundException {

		String initialVector = conf.get("initialVectorPath");
		String currentVector = conf.get("currentVectorPath");

		String finalVector = conf.get("finalVectorPath");
		/* here the testing system will search for the final rank vector */

		Double epsilon = conf.getDouble("epsilon", 0.1);
		Double beta = conf.getDouble("beta", 0.8);

		RemoveDeadends.job(conf);
		long nNodes = conf.getLong("numNodes", 0);
		GraphToMatrix.job(conf);
		createInitialRankVector(initialVector, nNodes);
		boolean convergence = false;
		while (!convergence) {
			MatrixVectorMult.job(conf);
			avoidSpiderTraps(currentVector, nNodes, beta);
			convergence = checkConvergence(initialVector, currentVector, epsilon);
			if (!convergence) {
				FileUtils.copyDirectory(new File(currentVector), new File(initialVector));
				FileUtils.deleteQuietly(new File(currentVector));
			}
		}
		FileUtils.copyDirectory(new File(currentVector), new File(finalVector));
		FileUtils.deleteQuietly(new File(currentVector));

	}
}
