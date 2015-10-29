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
	static String RUNNING_MACHINES_FILE = "runningMachines.txt";
	private static String INPUT_FILE = "input.txt";
	private static String OS = null;
	private ShavaManager manager = new ShavaManager();

	// Dictionnaire identifiant UMX-> (machine,process)
	private HashMap<String, String> umxMachine = new HashMap<String, String>();

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

	public String getInputFilename() {
		return MappedData.getDataDir() + "/" + INPUT_FILE;
	}

	private int getRunningMachines(String machinesNameFile) {
		return manager.getRunningMachines(filename);
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

	/**
	 * Pour chaque mot du texte on ajoute un identifiant umx au dictionnaire
	 * keyUmx
	 * 
	 * @param sp
	 *            Process identifiant le fichier UMX
	 * 
	 * @param umxResult
	 *            Texte contenant par ligne un mot � ajouter au dictionnaire
	 *            keyUmx
	 */
	private void addToKeyUmx(ShavaProcess sp, String umxResult) {
		if (umxResult != null) {
			String[] tokens = umxResult.split(System.lineSeparator());
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
	 * Pour chaque bloc du fichier d'entr�e, cr�ation d'un fichier SX<id>.txt Le
	 * fichier est envoy� pour traitement sur la premi�re machine disponible en
	 * vue de produite un fichier UMX. On enregistre l'id de l'UMX dans le
	 * dictionnaire umxMachine
	 * 
	 * @param filename
	 *            Fichier contenant le texte � traiter
	 */
	public void sxToUmx(String filename) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(filename));
		String line;
		while ((line = reader.readLine()) != null) {
			// TODO introduire un splitter
			line = line.trim();
			if (line.length() == 0)
				continue;
			String id = MappedData.getId();
			PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(MappedData.getSxFullNameFile(id))));
			writer.println(line);
			writer.close();
			String ordi = manager.pushJob(id, null, MappedData.Task.SX);
			this.umxMachine.put(id, ordi);

		}
		reader.close();

	}

	public void umxToSmx() {
		for (String key : this.keyUmx.keySet()) {
			String ordi = manager.pushJob(key, this.keyUmx.get(key), MappedData.Task.UMX);
		}
	}

	public void AM() throws IOException, InterruptedException {
		sxToUmx(getInputFilename());
		while (!manager.stackEmpty()) {
			ShavaProcess sp = manager.popJob();
			sp.getProcess().waitFor();
			ShavaExec sexec = new ShavaExec();
			String umxResult = sexec.getInputStream(sp.getProcess());
			if (umxResult == null) {
				System.err.println("SX TO UMX failed for " + sp.getSlaveName());
			} else {
				addToKeyUmx(sp, umxResult);
			}
		}
		umxToSmx();
		while (!manager.stackEmpty()) {
			ShavaProcess sp = manager.popJob();
			sp.getProcess().waitFor();
			ShavaExec sexec = new ShavaExec();
			String smxResult = sexec.getInputStream(sp.getProcess());
			if (smxResult == null) {
				System.err.println("UMX TO SMX failed for " + sp.getSlaveName() + " word " + sp.getId());
			}
		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Master master = new Master();
		try {
			int countMachine = master.getRunningMachines(master.getMachinesNameFile());
			if (countMachine == 00) {
				System.err.println("No slave available");
			} else {
				// master.callFirstmachine(master.getRunnigMachinesNameFile());
				master.AM();
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
