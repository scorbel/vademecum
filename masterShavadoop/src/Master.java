import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;

import data.MappedData;

public class Master {

	private static String MACHINES_FILE = "machines.txt";
	private static String RUNNING_MACHINES_FILE = "runningMachines.txt";
	private static String INPUT_FILE = "input.txt";
	private static String OS = null;

	// Dictionnaire identifiant UMX-> (machine,process)
	private HashMap<String, ShavaProcess> umxMachine = new HashMap<String, ShavaProcess>();

	// Dictionnaire identifiant mot-> {(machine,process)}
	private HashMap<String, ArrayList<String>> keyUmx = new HashMap<String, ArrayList<String>>();

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

	public String getMachinesNameFile() {
		return MappedData.getDataDir() + "/" + MACHINES_FILE;
	}

	public String getRunnigMachinesNameFile() {
		return MappedData.getDataDir() + "/" + RUNNING_MACHINES_FILE;
	}

	public String getInputFilename() {
		return MappedData.getDataDir() + "/" + INPUT_FILE;
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
		BufferedReader reader = new BufferedReader(new FileReader(getRunnigMachinesNameFile()));
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

	private void addToKeyUmx(ShavaProcess sp, String umxResult) {
		if (umxResult != null) {
			String[] tokens = umxResult.split("\n");
			for (String token : tokens) {
				ArrayList<String> umxList = keyUmx.get(token);
				if (umxList == null) {
					umxList = new ArrayList<String>();
				}
				umxList.add(sp.getId());
			}
		}
	}

	/**
	 * Etape 2 Pour chaque bloc Pour chaque bloc du fichier d'entrée, création
	 * d'un fichier SX<id>.txt Le fichier est envoyé pour traitement sur la
	 * première machine disponible en vue de produite un fichier UMX Le process
	 * correspondant est stocké dans le dictionnaire UmxMachine l'identifiant is
	 * étant la clé filename : Fichier contenant le texte à traiter
	 */
	public void sxToUmx(String filename) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(filename));
		String line;
		while ((line = reader.readLine()) != null) {
			// introduire un splitter
			line = line.trim();
			String id = MappedData.getId();
			PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(MappedData.getSxFullNameFile(id))));
			writer.println(line);
			writer.close();
			String ordi = getFirstAvailableSlave();
			ShavaExec sexec = new ShavaExec();
			Process p = sexec.processSxCmd(ordi, id);
			umxMachine.put(id, new ShavaProcess(id, ordi, p));
		}
		reader.close();

	}

	public void AM() throws IOException, InterruptedException {
		sxToUmx(getInputFilename());
		for (ShavaProcess sp : umxMachine.values()) {
			sp.getProcess().waitFor();
			ShavaExec sexec = new ShavaExec();
			String umxResult = sexec.getInputStream(sp.getProcess());
			addToKeyUmx(sp, umxResult);
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
