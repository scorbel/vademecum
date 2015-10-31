import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import data.MappedData;

/**
 * La classe Master de Shavadoop propose une interface pour la simulation d'un
 * fonctionnement hadoop limité à un comptage de mots
 * 
 *
 */

public class Master {

	public static Logger logger = LoggerFactory.getLogger("masterShavadoop.src.Master");
	private static String MACHINES_FILE = "machines.txt";
	static String RUNNING_MACHINES_FILE = "runningMachines.txt";
	private static String INPUT_FILE = "input.txt";
	private static String OUTPUT_FILE = "output.txt";
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

	public String getOutputFilename() {
		return MappedData.getDataDir() + "/" + OUTPUT_FILE;
	}

	/**
	 * A partir du fichier passé en paramètre donnant la liste des machines
	 * susceptibles d'être accessibles sur le réseau, après test d'activité sur
	 * le réseau le nom de la machine est enregistré dans une liste du manager
	 * 
	 * @param machinesNameFile
	 *            s Liste des noms de machines usceptibles d'être accessibles
	 *            sur le réseau un nom par ligne
	 * 
	 * @return le nombre de machines disponibles
	 * 
	 * @throws IOException
	 */
	private int getRunningMachines(String machinesNameFile) throws IOException {
		return manager.getRunningMachines(machinesNameFile);
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
	 * Alimentation du dictionnaire keyUmx associant à chaque mot un ensemble
	 * d'identifiants UMX
	 * 
	 * @param sp
	 *            Process identifiant le fichier UMX
	 * 
	 * @param umxResult
	 *            Texte contenant par ligne un mot à ajouter au dictionnaire
	 *            keyUmx
	 */
	private void addToKeyUmx(ShavaProcess sp, String umxResult) {
		if (umxResult != null) {
			String[] tokens = umxResult.split(" ");
			for (String token : tokens) {
				ArrayList<String> umxList = keyUmx.get(token);
				if (umxList == null) {
					umxList = new ArrayList<String>();
				}
				umxList.add(sp.getId());
				keyUmx.put(token, umxList);
			}
		}
	}

	private void keyUmxToString() {
		String result = "";
		for (String key : this.keyUmx.keySet()) {
			System.out.println(key);
		}
	}

	/**
	 * MAPPING
	 * 
	 * Pour chaque bloc du fichier d'entrée, création d'un fichier SX<id>.txt Le
	 * fichier est envoyé pour traitement sur la première machine disponible en
	 * vue de produite un fichier UMX. On enregistre l'id de l'UMX dans le
	 * dictionnaire umxMachine.
	 * 
	 * Le fichier SX<id>.txt comporte un bloc de données.
	 * 
	 * Le fichier UMX<id>.txt comporte sur chaque ligne un mot et le chiffre 1,
	 * ces deux informations étant séparées par un espace.
	 * 
	 * @param filename
	 *            Fichier contenant le texte à traiter
	 */
	public void sxToUmx(String filename) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(filename));
		String line;
		while ((line = reader.readLine()) != null) {
			// TODO introduire un splitter
			line = line.trim();
			if (line.length() == 0)
				continue;
			logger.info(line);
			String id = MappedData.getId();
			PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(MappedData.getSxFullNameFile(id))));
			writer.println(line);
			writer.close();
			String ordi = manager.pushJob(id, null, MappedData.Task.SX);
			this.umxMachine.put(id, ordi);

		}
		reader.close();

	}

	/**
	 * SHUFFLING
	 * 
	 * Pour chaque mot du dictionnaire keyUmx, on envoie sur la première machine
	 * disponible le mot à traiter et la liste des identifiants des fichiers UMX
	 * associés. La machine sélectionnée va produire un fichier SMX<mot>.txt,
	 * rassemblant les occurrences du mot touvées pour l'ensemble des fichiers
	 * UMX.
	 * 
	 * Le fichier UMX<id>.txt comporte sur chaque ligne un mot et le chiffre 1,
	 * ces deux informations étant séparées par un espace.
	 * 
	 * Le fichier SMX<mot>.txt comporte sur chaque ligne le mot et le chiffre 1
	 * (une ligne par occurrence trouvée)
	 */
	public void umxToSmx() {
		for (String key : this.keyUmx.keySet()) {
			manager.pushJob(key, this.keyUmx.get(key), MappedData.Task.UMX);
		}
	}

	public void smxToRmx() {
		for (String key : this.keyUmx.keySet()) {
			manager.pushJob(key, null, MappedData.Task.SMX);
		}

	}

	/**
	 * Décompte des occurrences de mots dans un document désigné par la variable
	 * globale INPUT_FILE
	 * 
	 * Procédure enchainant les phases de mapping, shuffling et reduce. A
	 * chacune de ces trois phases, la procédure dépile itérativement les
	 * process empilés et traite le résultat renvoyé.
	 * 
	 * Le résultat est stocké dans un fichier désigné par la variable globale
	 * OUTPUT_FILE Chaque ligne du fichier est constituée d'un mot et du nombre
	 * d'occurences
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */

	public void AM() throws IOException, InterruptedException {
		// MAP
		sxToUmx(getInputFilename());
		while (!manager.stackEmpty()) {
			ShavaProcess sp = manager.popJob();
			String umxResult = sp.getOutputString();
			sp.getProcess().waitFor();
			if (umxResult == null) {
				String message = MessageFormat.format("SX TO UMX failed for {0}", sp.getSlaveName());
				Master.logger.error(message);
			} else {
				addToKeyUmx(sp, umxResult);
			}
		}

		// SHUFFLE
		umxToSmx();
		while (!manager.stackEmpty()) {
			ShavaProcess sp = manager.popJob();
			String smxResult = sp.getOutputString();
			sp.getProcess().waitFor();
			if (smxResult == null) {
				String message = MessageFormat.format("UMX TO SMX failed for {0} word {1}", sp.getSlaveName(),
						sp.getId());
				Master.logger.error(message);
			}
		}
		// REDUCE
		smxToRmx();
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(getOutputFilename())));
		while (!manager.stackEmpty()) {
			ShavaProcess sp = manager.popJob();
			String rmxResult = sp.getOutputString();
			sp.getProcess().waitFor();
			if (rmxResult == null) {
				String message = MessageFormat.format("SMX TO RMX failed for {0} word {1}", sp.getSlaveName(),
						sp.getId());
				Master.logger.error(message);
			} else {
				writer.println(rmxResult);
			}
		}
		writer.close();

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Master master = new Master();
		try {
			int countMachine = master.getRunningMachines(master.getMachinesNameFile());
			if (countMachine == 0) {
				Master.logger.error("No slave available");
			} else {
				// master.callFirstmachine(master.getRunnigMachinesNameFile());
				String message = MessageFormat.format("Starting with {0} slaves", countMachine);
				Master.logger.info(message);
				master.AM();
			}

		} catch (Exception e) {
			Master.logger.error("Unable to start", e);
		}

	}

}
