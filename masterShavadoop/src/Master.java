import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import data.BlockSplitter;
import data.LineSplitter;
import data.MappedData;
import data.Splitter;

/**
 * La classe Master de Shavadoop propose une interface pour la simulation d'un
 * fonctionnement hadoop limit� � un comptage de mots
 * 
 * 
 */

public class Master {

	public static Logger logger = LoggerFactory
			.getLogger("masterShavadoop.src.Master");
	private static String MACHINES_FILE = "machines.txt";
	static String RUNNING_MACHINES_FILE = "runningMachines.txt";
	private static String INPUT_FILE = "input.txt";
	private static String OUTPUT_FILE = "output.txt";
	private static String OS = null;
	private ShavaManager manager = null;

	// Dictionnaire identifiant UMX-> (machine,process)
	private HashMap<String, String> umxMachine = new HashMap<String, String>();

	// Dictionnaire identifiant mot-> {(machine,process)}
	private HashMap<String, ArrayList<String>> keyUmx = new HashMap<String, ArrayList<String>>();
	// Index sur les mots utilis�s dans les phases de shuffle et reduce dans le
	// cas o� la limite de
	// la taille de la pile est atteinte
	private ArrayList<String> keyindex = new ArrayList<String>();

	// Mots en erreur au shuffle
	private ArrayList<String> shuffleError = new ArrayList<String>();

	// Mots en erreur au reduce
	private ArrayList<String> reduceError = new ArrayList<String>();

	private Splitter splitter = null;

	private Properties properties = new Properties();

	public Master() throws IOException {
		getOsName();
		String propFileName = "shava.properties";
		InputStream inputStream = getClass().getClassLoader()
				.getResourceAsStream(propFileName);
		if (inputStream != null) {
			properties.load(inputStream);
		} else {
			throw new FileNotFoundException("property file '" + propFileName
					+ "' not found in the classpath");
		}
		MappedData.SplitterType splitterType = MappedData.SplitterType.LINE;
		String filename = this.properties.getProperty("input", INPUT_FILE);
		String fullPathName = MappedData.getDataDir() + "/" + filename;
		if (splitterType == MappedData.SplitterType.valueOf(properties
				.getProperty("splitMode",
						MappedData.SplitterType.LINE.toString()))) {
			this.splitter = new LineSplitter(fullPathName);
		} else {
			this.splitter = new BlockSplitter(fullPathName);
		}
		String customMaxProcess = this.properties.getProperty("maxProcess");
		int maxProcess = 20;
		if (customMaxProcess != null && customMaxProcess.length() > 0) {
			maxProcess = Integer.parseInt(customMaxProcess);
		}
		manager = new ShavaManager(maxProcess);

		String message = MessageFormat.format("Input File  {0} Split type {1}",
				filename, splitterType.name());
		Master.logger.info(message);
		Master.logger.info(MessageFormat.format("Max processes  {0}",
				maxProcess));
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
	 * A partir du fichier pass� en param�tre donnant la liste des machines
	 * susceptibles d'�tre accessibles sur le r�seau, apr�s test d'activit� sur
	 * le r�seau le nom de la machine est enregistr� dans une liste du manager
	 * 
	 * @param machinesNameFile
	 *            s Liste des noms de machines usceptibles d'�tre accessibles
	 *            sur le r�seau un nom par ligne
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
			if (sexec.callMachine(ordi, true) != null) {
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
			if (sexec.callMachine(ordi, true) != null) {
				System.out.println("tout est fini");
			}

		}
		reader.close();

	}

	/**
	 * Alimentation du dictionnaire keyUmx associant � chaque mot un ensemble
	 * d'identifiants UMX
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
			String[] tokens = umxResult.split(" ");
			for (String token : tokens) {
				ArrayList<String> umxList = keyUmx.get(token);
				if (umxList == null) {
					umxList = new ArrayList<String>();
					this.keyindex.add(token);
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
	 * Pour chaque bloc du fichier d'entr�e du splitter, cr�ation d'un fichier
	 * SX<id>.txt Le fichier est envoy� pour traitement sur la premi�re machine
	 * disponible en vue de produite un fichier UMX. On enregistre l'id de l'UMX
	 * dans le dictionnaire umxMachine.
	 * 
	 * Le fichier SX<id>.txt comporte un bloc de donn�es.
	 * 
	 * Le fichier UMX<id>.txt comporte sur chaque ligne un mot et le chiffre 1,
	 * ces deux informations �tant s�par�es par un espace.
	 * 
	 * @throws InterruptedException
	 * 
	 */
	public void sxToUmx() throws IOException, InterruptedException {
		String block;
		while ((block = this.splitter.readBlock()) != null) {
			block = block.trim();
			if (block.length() == 0)
				continue;
			logger.info(block);
			String id = MappedData.getId();
			Writer writer = new OutputStreamWriter(new FileOutputStream(
					MappedData.getSxFullNameFile(id)), "UTF-8");
			BufferedWriter bufferWriter = new BufferedWriter(writer);
			bufferWriter.write(block);
			bufferWriter.close();
			String ordi = manager.pushJob(id, null, MappedData.Task.SX);
			if (ordi == null) {
				popSxToUmx();
			} else {
				this.umxMachine.put(id, ordi);
			}

		}
		this.splitter.close();
		popSxToUmx();

	}

	private void popSxToUmx() throws InterruptedException, IOException {
		while (!manager.stackEmpty()) {
			ShavaProcess sp = manager.popJob();
			boolean success = popJob(sp, MappedData.Task.SX,
					MappedData.Task.UMX);
			if (success) {
				String umxResult = sp.getOutputString();
				if (umxResult == null) {
					String message = MessageFormat.format(
							"SX TO UMX failed for {0}", sp.getSlaveName());
					Master.logger.error(message);

				} else {
					addToKeyUmx(sp, umxResult);
				}
			}
		}
	}

	/**
	 * SHUFFLING
	 * 
	 * Pour chaque mot du dictionnaire keyUmx, on envoie sur la premi�re machine
	 * disponible le mot � traiter et la liste des identifiants des fichiers UMX
	 * associ�s. La machine s�lectionn�e va produire un fichier SMX<mot>.txt,
	 * rassemblant les occurrences du mot touv�es pour l'ensemble des fichiers
	 * UMX.
	 * 
	 * Le fichier UMX<id>.txt comporte sur chaque ligne un mot et le chiffre 1,
	 * ces deux informations �tant s�par�es par un espace.
	 * 
	 * Le fichier SMX<mot>.txt comporte sur chaque ligne le mot et le chiffre 1
	 * (une ligne par occurrence trouv�e)
	 * 
	 * @param startIndex
	 *            Indice d'entr�e dans l'index keyIndex
	 * 
	 * @return l'indice du dernier �l�ment de l'index keyIndex trait� (cas o� la
	 *         pile de process est pleine) la taille de l'index sinon
	 */
	public int shuffle(int startIndex) {
		boolean fullStack = false;
		int returnIndex = startIndex;
		for (int i = startIndex; i < this.keyindex.size() && !fullStack; i++) {
			String key = this.keyindex.get(i);
			String ordi = manager.pushJob(key, this.keyUmx.get(key),
					MappedData.Task.UMX);
			if (ordi == null) {
				fullStack = true;
				returnIndex = i - 1;
			} else {
				returnIndex = i;
			}

		}
		return returnIndex;
	}

	private void umxToSmx() throws InterruptedException, IOException {
		int index = 0;
		while (index < this.keyindex.size()) {
			index = shuffle(index);
			popUmxToSmx();
			index++;
		}
	}

	private void popUmxToSmx() throws InterruptedException, IOException {
		while (!manager.stackEmpty()) {
			ShavaProcess sp = manager.popJob();
			boolean success = popJob(sp, MappedData.Task.UMX,
					MappedData.Task.SX);
			if (!success) {
				this.shuffleError.add(sp.getId());
			}
		}
	}

	private void retryUmxToSmx() throws InterruptedException, IOException {
		for (Iterator<String> iterator = this.shuffleError.iterator(); iterator
				.hasNext();) {
			String key = iterator.next();
			String ordi = manager.pushJob(key, this.keyUmx.get(key),
					MappedData.Task.UMX);
			if (ordi == null) {
				String message = MessageFormat.format(
						"RETRY UMX TO SMX failed for word {0}", key);
				Master.logger.error(message);
			} else {
				iterator.remove();
			}
		}
		popUmxToSmx();
	}

	/**
	 * REDUCE
	 * 
	 * Production d'un fichier RMX<key>.txt aggr�geant le nombre d'occurrences
	 * contenues dans le fichier SMX<key>.txt
	 * 
	 * Le fichier SMX<key>.txt comporte sur chaque ligne le mot et le chiffre 1
	 * (une ligne par occurrence)
	 * 
	 * Le fichier RMX<key>.txt comporte une seule ligne : le mot et le nombre
	 * total d'occurrence
	 * 
	 * @param startIndex
	 *            Indice d'entr�e dans l'index keyIndex
	 * 
	 * @return l'indice du dernier �l�ment de l'index keyIndex trait� (cas o� la
	 *         pile de process est pleine) la taille de l'index sinon
	 */
	public int reduce(int startIndex) {
		boolean fullStack = false;
		int returnIndex = startIndex;
		for (int i = startIndex; i < this.keyindex.size() && !fullStack; i++) {
			String key = this.keyindex.get(i);
			if (this.shuffleError.contains(key))
				continue;
			String ordi = manager.pushJob(key, null, MappedData.Task.SMX);
			if (ordi == null) {
				fullStack = true;
				returnIndex = i - 1;
			} else {
				returnIndex = i;
			}
		}
		return returnIndex;
	}

	private void smxToRmx(PrintWriter writer) throws InterruptedException,
			IOException {
		int index = 0;
		while (index < this.keyindex.size()) {
			index = reduce(index);
			popSmxToRmx(writer);
			index++;
		}
	}

	private void popSmxToRmx(PrintWriter writer) throws InterruptedException,
			IOException {
		while (!manager.stackEmpty()) {
			ShavaProcess sp = manager.popJob();
			boolean success = popJob(sp, MappedData.Task.SMX,
					MappedData.Task.RMX);
			if (success) {
				String rmxResult = sp.getOutputString();
				if (rmxResult == null) {
					String message = MessageFormat.format(
							"SMX TO RMX failed for {0} word {1]",
							sp.getSlaveName(), sp.getId());
					Master.logger.error(message);
				} else {
					writer.println(rmxResult);
				}
			} else {
				this.reduceError.add(sp.getId());
			}
		}
	}

	private void retrySmxToRmx() throws InterruptedException, IOException {
		for (Iterator<String> iterator = this.reduceError.iterator(); iterator
				.hasNext();) {
			String key = iterator.next();
			String ordi = manager.pushJob(key, null, MappedData.Task.SMX);
			if (ordi == null) {
				String message = MessageFormat.format(
						"RETRY SMX TO UMX failed for word {0}", key);
				Master.logger.error(message);
			} else {
				iterator.remove();
			}
		}
		popUmxToSmx();
	}

	/**
	 * D�compte des occurrences de mots dans un document d�sign� par la variable
	 * globale INPUT_FILE
	 * 
	 * Proc�dure enchainant les phases de mapping, shuffling et reduce. A
	 * chacune de ces trois phases, la proc�dure d�pile it�rativement les
	 * process empil�s et traite le r�sultat renvoy�.
	 * 
	 * Le r�sultat est stock� dans un fichier d�sign� par la variable globale
	 * OUTPUT_FILE Chaque ligne du fichier est constitu�e d'un mot et du nombre
	 * d'occurences
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */

	public void AM() throws IOException, InterruptedException {
		// MAP
		sxToUmx();
		// SHUFFLE
		umxToSmx();
		if (shuffleError.size() > 0) {
			retryUmxToSmx();
		}
		// REDUCE
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(
				getOutputFilename())));
		smxToRmx(writer);
		if (reduceError.size() > 0) {
			retrySmxToRmx();
		}
		String message = MessageFormat.format("{0} words in {1}",
				this.keyindex.size(), OUTPUT_FILE);
		Master.logger.info(message);
		Master.logger.info(MessageFormat.format("{0} shuffle errors",
				this.shuffleError.size()));
		Master.logger.info(MessageFormat.format("{0} reduce errors",
				this.reduceError.size()));
		writer.close();

	}

	private boolean popJob(ShavaProcess sp, MappedData.Task from,
			MappedData.Task to) throws InterruptedException, IOException {
		sp.getProcess().waitFor();
		String smxError = sp.getErrorString();
		if (smxError != null) {
			String message = MessageFormat.format(
					"{2} TO {3} failed for {0} word {1}", sp.getSlaveName(),
					sp.getId(), from.name(), to.name());
			Master.logger.error(message);
			Master.logger.error(smxError);
			return false;
		}
		String message = MessageFormat.format("job {0} {1} on {2} popped", sp
				.getTaskType().toString(), sp.getId(), sp.getSlaveName());
		Master.logger.info(message);
		return true;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			Master master = new Master();

			int countMachine = master.getRunningMachines(master
					.getMachinesNameFile());
			if (countMachine == 0) {
				Master.logger.error("No slave available");
			} else {
				// master.callFirstmachine(master.getRunnigMachinesNameFile());
				String message = MessageFormat.format(
						"Starting with {0} slaves", countMachine);
				Master.logger.info(message);
				master.AM();
			}

		} catch (Exception e) {
			Master.logger.error("Unable to start", e);
		}

	}

}
