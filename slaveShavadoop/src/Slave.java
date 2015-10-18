import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.StringTokenizer;

import data.MappedData;


public class Slave {
	private String name;
	
	public static String commonDir = "data";
	static public String getPathFile()
	{
		return System.getProperty("user.dir");

	}
	public String getOutputFilePath(String id) {
		return  getPathFile()+"/../"+commonDir+"/"+name+".txt";
	}

	
	public Slave() throws UnknownHostException {
		name = InetAddress.getLocalHost().getHostName();
	}

	private void splitBlockInFile(String block, String id) throws IOException {
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(getOutputFilePath(id))));
		StringTokenizer tokenizer = new StringTokenizer(block);
		while (tokenizer.hasMoreElements()) {
			MappedData data = new MappedData((String) tokenizer.nextElement());
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			System.out.println("Avant sleep");
			Thread.sleep(10000);
			System.out.println("Après sleep");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
