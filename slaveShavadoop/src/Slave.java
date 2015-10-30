import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import data.MappedData;
import data.MappedDataList;

public class Slave {
	private String name;

	public Slave() {
		// name = InetAddress.getLocalHost().getHostName();
	}

	private MappedDataList splitBlockFromFile(String filename) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(filename));
		String line;
		MappedDataList dataList = new MappedDataList();
		while ((line = reader.readLine()) != null) {
			String block = line.trim();
			dataList.addLine(block);
		}
		reader.close();
		return dataList;
	}

	private void sxToUmx(String id) throws IOException {
		MappedDataList dataList = splitBlockFromFile(MappedData.getSxFullNameFile(id));
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(MappedData.getUmxFullNameFile(id))));
		writer.println(dataList);
		writer.close();
		System.out.print(dataList.getKeys());

	}

	// Attention on peut avoir plusieurs process qui veulent accéder au même
	// fichier
	private synchronized void umxToSmx(String key, ArrayList<String> umxList) throws IOException {
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(MappedData.getSmxFullNameFile(key))));
		for (String umx : umxList) {
			BufferedReader reader = new BufferedReader(new FileReader(MappedData.getUmxFullNameFile(umx)));
			MappedDataList mappedDataList = MappedDataList.createAndFilterFrom(reader, key);
			if (mappedDataList != null) {
				writer.write(mappedDataList.toString());
				writer.write(System.lineSeparator());
			}
			reader.close();
		}
		writer.close();
	}

	private void testSleep() throws InterruptedException {
		System.out.println("Avant sleep");
		Thread.sleep(10000);
		System.out.println("AprÃ¨s sleep");
	}

	public static void printUsage() {
		String message = "Slave.jar [";
		String sep = "";
		for (MappedData.Task task : MappedData.Task.values()) {
			message += sep + task.toString();
			sep = "|";
		}
		message += "] <id> [<id>...]";
		System.err.println("Illegal arguments, Usage " + message);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 2) {
			printUsage();
		} else {
			Slave slave = new Slave();
			switch (args[0]) {
			case "-SX":
				try {
					String id = args[1];
					slave.sxToUmx(id);
				} catch (IOException e) {
					e.printStackTrace();
					System.err.println("SX TO UMX failed");
				}
				break;
			case "-UMX":
				try {
					ArrayList<String> umxList = new ArrayList<String>();
					String key = args[1];
					for (int i = 2; i < args.length; i++) {
						umxList.add(args[i]);
					}
					slave.umxToSmx(key, umxList);
				} catch (IOException e) {
					e.printStackTrace();
					System.err.println("UMX TO SMX failed");
				}
				break;

			default:
				printUsage();

			}
		}

	}

}
