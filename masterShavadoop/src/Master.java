import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;

public class Master {

	private static String MACHINES_FILE = "machines.txt";
	private static String RUNNING_MACHINES_FILE = "runningMachines.txt";
	private static String INPUT_FILE = "input.txt";
	private static String OS = null;
	private static String DATA = "data";
	private static String SX_SUFFIXE = "SX";
	private static String TXT_EXT = ".txt";

	private HashMap<String, Process> umxMachine = new HashMap<String, Process>(); // Dictionnaire
																					// identifiant->machine
	private HashMap<String, ArrayList<String>> keyUmx = new HashMap<String, ArrayList<String>>(); // Dictionnaire
																									// mot
																									// ->
																									// {identifiant
																									// machine}

	public Master() {
		getOsName();
	}

	public static String getOsName() {
		if (OS == null) {
			OS = System.getProperty("os.name");
		}
		return OS;
	}

	public static boolean isWindows() {
		return getOsName().startsWith("Windows");
	}

	static public String getUserDir() {
		String userDir = System.getProperty("user.dir");
		if (isWindows()) {
			userDir = userDir.replaceAll("\\\\", "/");
		}
		return userDir;

	}

	static String getDataDir() {
		return System.getProperty("user.home") + "/" + DATA;
	}

	static String getTimestamp() {
		java.util.Date date = new java.util.Date();
		return new java.sql.Timestamp(date.getTime()).toString();
	}

	public String getMachinesNameFile() {
		return getUserDir() + "/" + MACHINES_FILE;
	}

	public String getRunnigMachinesNameFile() {
		return getUserDir() + "/" + RUNNING_MACHINES_FILE;
	}

	public String getInputFilename() {
		return getUserDir() + "/" + INPUT_FILE;
	}

	public String getSxNameFile() {
		return getDataDir() + "/" + SX_SUFFIXE + getTimestamp() + TXT_EXT;
	}

	public void getRunningMachines(String filename) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(filename));
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(getRunnigMachinesNameFile())));

		ShavaExec sexec = new ShavaExec();
		String line;
		while ((line = reader.readLine()) != null) {
			String ordi = line.trim();
			String[] cmd = sexec.getSshCmd("hostname", ordi);
			if (sexec.processCmd(cmd, false, true) == 1) {
				writer.println(ordi);
			}

		}
		reader.close();
		writer.close();

	}

	public void callFirstmachine(String filename) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(filename));
		String line;
		if ((line = reader.readLine()) != null) {
			String ordi = line.trim();
			ShavaExec sexec = new ShavaExec();
			if (sexec.callMachine(ordi, true) == 1) {
				System.out.println("tout est fini");
			}

		}
		reader.close();

	}

	public void callAllMachines(String filename) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(filename));
		String line;
		ShavaExec sexec = new ShavaExec();
		while ((line = reader.readLine()) != null) {
			String ordi = line.trim();
			if (sexec.callMachine(ordi, true) == 1) {
				System.out.println("tout est fini");
			}

		}
		reader.close();

	}

	public void AM() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(getInputFilename()));
		String line;
		if ((line = reader.readLine()) != null) {
			line = line.trim();
			PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(getSxNameFile())));
			writer.println(line);
			writer.close();
			ShavaExec sexec = new ShavaExec();

		}
		reader.close();

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Master master = new Master();
		try {
			master.getRunningMachines(master.getMachinesNameFile());
			master.callFirstmachine(master.getRunnigMachinesNameFile());
			// master.AM();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
