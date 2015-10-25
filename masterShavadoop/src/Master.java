import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import data.MappedData;

public class Master {

	private static String MACHINES_FILE = "machines.txt";
	private static String RUNNING_MACHINES_FILE = "runningMachines.txt";
	private static String INPUT_FILE = "input.txt";
	private static String OS = null;
	private static String SX_SUFFIXE = "SX";
	private static String UMX_SUFFIXE = "UMX";
	private static String TXT_EXT = ".txt";

	private HashMap<String, ShavaProcess> umxMachine = new HashMap<String, ShavaProcess>(); // Dictionnaire
																							// identifiant
																							// UMX
																							// ->
																							// (machine,process)
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

	static String getTimestamp() {

		// java.util.Date date = new java.util.Date();
		// return new java.sql.Timestamp(date.getTime()).toString();
		return Long.toString(new Date().getTime());
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

	public String getSxFullNameFile() {
		return MappedData.getDataDir() + "/" + getSxNameFile();
	}

	public String getSxNameFile() {
		return SX_SUFFIXE + getTimestamp() + TXT_EXT;
	}

	public String getId(String sxFilename) {
		return sxFilename.substring(SX_SUFFIXE.length(), sxFilename.length()
				- TXT_EXT.length());
	}

	public String getSxNameFile(String id) {
		return UMX_SUFFIXE + id + TXT_EXT;
	}

	public void getRunningMachines(String filename) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(filename));
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(
				getRunnigMachinesNameFile())));

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

	public boolean umxMachineContains(String ordi) {
		boolean result = false;
		if (umxMachine.values().size() > 0) {
			for (ShavaProcess process : umxMachine.values()) {
				if (process.getName().equals(ordi)) {
					result = true;
					break;
				}
			}
		}
		return result;
	}

	public String getFirstAvailableSlave() throws IOException {
		String result = null;
		BufferedReader reader = new BufferedReader(new FileReader(
				getRunnigMachinesNameFile()));
		String line;
		while ((line = reader.readLine()) != null && result == null) {
			String ordi = line.trim();
			if (!umxMachineContains(ordi)) {
				result = ordi;
			}
		}
		if (result == null) {
			// Take the first machine in umxMachine
			result = umxMachine.values().iterator().next().getName();
		}
		reader.close();
		return (result);

	}

	public void sxToUmx(String filename) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(filename));
		String line;
		if ((line = reader.readLine()) != null) {
			line = line.trim();
			PrintWriter writer = new PrintWriter(new BufferedWriter(
					new FileWriter(getSxFullNameFile())));
			writer.println(line);
			writer.close();
			String ordi = getFirstAvailableSlave();
			ShavaExec sexec = new ShavaExec();
			Process p = sexec.processSxCmd(ordi, getSxNameFile());
			String id = getId(getSxNameFile());
			umxMachine.put(id, new ShavaProcess(ordi, p));
		}
		reader.close();

	}

	public void AM() throws IOException, InterruptedException {
		sxToUmx(getInputFilename());
		for (ShavaProcess sp : umxMachine.values()) {
			sp.getProcess().waitFor();
			ShavaExec sexec = new ShavaExec();
			sexec.getInputStream(sp.getProcess());
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Master master = new Master();
		try {
			master.getRunningMachines(master.getMachinesNameFile());
			// master.callFirstmachine(master.getRunnigMachinesNameFile());
			master.AM();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
