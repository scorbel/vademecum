import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import data.MappedData;
import data.MappedDataList;

public class Slave {
	private String name;

	enum Task {
		SX, UMX, SMX
	};

	public Slave() {
		// name = InetAddress.getLocalHost().getHostName();
	}

	private MappedDataList splitBlockFromFile(String filename)
			throws IOException {
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

	private void sxToUmx(String filename) throws IOException {
		MappedDataList dataList = splitBlockFromFile(MappedData.getDataDir()
				+ "/" + filename);
		System.out.print(dataList.toString());

	}

	private void testSleep() throws InterruptedException {
		System.out.println("Avant sleep");
		Thread.sleep(10000);
		System.out.println("Après sleep");
	}

	public static void printUsage() {
		String message = "Slave.jar [";
		String sep = "";
		for (Task task : Task.values()) {
			message += sep + task.toString();
			sep = "|";
		}
		message += "<filename>";
		System.err.println("Illegal arguments, Usage " + message);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 2) {
			printUsage();
		} else {
			String filename = args[1];
			Slave slave = new Slave();
			switch (args[0]) {
			case "SX":
				try {
					slave.sxToUmx(filename);
				} catch (IOException e) {
					e.printStackTrace();
					System.err.println("SX TO UMX failed");
				}
				break;
			default:
				printUsage();

			}
		}

	}

}
