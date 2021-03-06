import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import data.BlockSplitter;
import data.LineSplitter;
import data.MappedData;
import data.MappedData.SplitterType;
import data.MappedDataList;
import data.Splitter;

public class Slave {
	public static Logger logger = LoggerFactory.getLogger("slaveShavadoop.src.Slave");

	private String name;

	private Properties properties = new Properties();

	private SplitterType splitterType = SplitterType.LINE;

	public Slave() throws IOException {
		name = InetAddress.getLocalHost().getHostName();
		String propFileName = "shava.properties";
		InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
		if (inputStream != null) {
			properties.load(inputStream);
		} else {
			throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
		}
		this.splitterType = MappedData.SplitterType
				.valueOf(properties.getProperty("splitMode", MappedData.SplitterType.LINE.toString()));
		String message = MessageFormat.format("Split type {0}", splitterType.name());
		logger.info(message);

	}

	/**
	 * Cr�ation d'une liste de tuples (mot,1) � partir du bloc de donn�es lu
	 * dans le fichier pass� en param�tre.
	 * 
	 * @param filename
	 *            Fichier SX comprenant un bloc de donn�es
	 * 
	 * @return La liste de tuples (mot,1) par une instance d'objet
	 *         MappedDataList
	 * 
	 * @throws IOException
	 *             Exception si probl�me de lecture
	 */
	private MappedDataList splitBlockFromFile(String filename) throws IOException {
		Splitter splitter = null;
		if (splitterType == SplitterType.LINE) {
			splitter = new LineSplitter(filename);
		} else {
			splitter = new BlockSplitter(filename);
		}
		String block = splitter.readBlock();
		ArrayList<String> words = splitter.split(block);
		MappedDataList dataList = new MappedDataList();
		for (String word : words) {
			dataList.add(new MappedData(word, 1));
		}
		splitter.close();
		return dataList;
	}

	/**
	 * MAPPING
	 * 
	 * Traitement du bloc de donn�es contenu dans le fichier SX identifi� par id
	 * d'entr�e
	 * 
	 * Cr�ation d'un fichier UMX<id>.txt comportant sur chaque ligne un mot et
	 * le chiffre 1, ces deux informations �tant s�par�es par un espace.
	 * 
	 * La proc�dure affiche la liste des mots trait�s sur la sortie standard
	 * 
	 * @param id
	 *            Identifiant du fichier SX
	 *
	 * @throws IOException
	 *             Exception si probl�me de lecture et/ou d'�criture
	 */
	private void sxToUmx(String id) throws IOException {
		MappedDataList dataList = splitBlockFromFile(MappedData.getSxFullNameFile(id));
		Writer writer = new OutputStreamWriter(new FileOutputStream(MappedData.getUmxFullNameFile(id)), "UTF-8");
		BufferedWriter bufferWriter = new BufferedWriter(writer);
		bufferWriter.write(dataList.toString());
		bufferWriter.close();
		System.out.print(dataList.getKeys());
		String message = MessageFormat.format("Mapping {0}", id);
		logger.info(message);

	}

	/**
	 * SHUFFLING
	 * 
	 * Un fichier SMX<key>.txt est produit en rassemblant les occurrences du mot
	 * touv�es pour l'ensemble des fichiers UMX du param�tre umxList
	 * 
	 * Le fichier SMX<key>.txt comporte sur chaque ligne le mot et le chiffre 1
	 * (une ligne par occurrence trouv�e)
	 * 
	 * @param key
	 *            mot
	 * 
	 * @param umxList
	 *            Liste des identifiant UMX
	 * 
	 * @throws IOException
	 *             Exception si probl�me de lecture et/ou d'�criture
	 */
	// Attention on peut avoir plusieurs process qui veulent acc�der au m�me
	// fichier
	private synchronized void umxToSmx(String key, ArrayList<String> umxList) throws IOException {
		Writer writer = new OutputStreamWriter(new FileOutputStream(MappedData.getSmxFullNameFile(key)), "UTF-8");
		BufferedWriter bufferWriter = new BufferedWriter(writer);
		MappedDataList mappedDataList = null;
		for (String umx : umxList) {
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(new FileInputStream(MappedData.getUmxFullNameFile(umx)), "UTF-8"));
			mappedDataList = MappedDataList.createAndFilterFrom(reader, key);
			if (mappedDataList != null) {
				bufferWriter.write(mappedDataList.toString());
				bufferWriter.write(System.lineSeparator());
			}
			reader.close();
		}
		bufferWriter.close();
		String message = MessageFormat.format("Shuffling {0} count {1}", key,
				mappedDataList == null ? 0 : mappedDataList.size());
		logger.info(message);
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
	 * @throws IOException
	 */
	private void smxToRmx(String key) throws IOException {
		Writer writer = new OutputStreamWriter(new FileOutputStream(MappedData.getRmxFullNameFile(key)), "UTF-8");
		BufferedWriter bufferWriter = new BufferedWriter(writer);
		BufferedReader reader = new BufferedReader(
				new InputStreamReader(new FileInputStream(MappedData.getSmxFullNameFile(key)), "UTF-8"));
		MappedData mdata = MappedData.createAndSumFrom(key, reader);
		bufferWriter.write(mdata.toString());
		reader.close();
		bufferWriter.close();
		System.out.print(mdata.toString());
		String message = MessageFormat.format("Reduce {0}", key);
		logger.info(message);
	}

	private void testSleep() throws InterruptedException {
		System.out.println("Avant sleep");
		Thread.sleep(10000);
		System.out.println("Après sleep");
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
			Slave slave;
			try {
				slave = new Slave();

				switch (args[0]) {
				case "-SX": {
					String id = args[1];
					try {
						String message = MessageFormat.format("SX TO UMX starts on host {0}", slave.name);
						logger.info(message);
						slave.sxToUmx(id);
					} catch (IOException e) {
						String message = MessageFormat.format("SX TO UMX failed {0} on host {1}", id, slave.name);
						logger.error(message, e);
					}
				}
					break;
				case "-UMX": {
					ArrayList<String> umxList = new ArrayList<String>();
					String key = args[1];
					try {
						for (int i = 2; i < args.length; i++) {
							umxList.add(args[i]);
						}
						String message = MessageFormat.format("UMX TO SMX starts on host {0}", slave.name);
						logger.info(message);
						slave.umxToSmx(key, umxList);
					} catch (IOException e) {
						String message = MessageFormat.format("UMX TO SMX failed {0} on host {1}", key, slave.name);
						logger.error(message, e);
					}
				}
					break;
				case "-SMX": {
					String key = args[1];
					try {
						String message = MessageFormat.format("SMX TO RMX starts on host {0}", slave.name);
						logger.info(message);
						slave.smxToRmx(key);

					} catch (IOException e) {
						String message = MessageFormat.format("SMX TO RMX failed {0} on host {1}", key, slave.name);
						logger.error(message, e);
					}
				}
					break;

				default:
					printUsage();

				}
			} catch (Exception e1) {
				logger.error("Call to slave failed", e1);
			}
		}

	}

}
