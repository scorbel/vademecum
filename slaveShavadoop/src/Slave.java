import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.MessageFormat;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import data.MappedData;
import data.MappedDataList;

public class Slave {
	public static Logger logger = LoggerFactory.getLogger("slaveShavadoop.src.Slave");

	private String name;

	public Slave() throws UnknownHostException {
		name = InetAddress.getLocalHost().getHostName();
	}

	/**
	 * Création d'une liste de tuples (mot,1) à partir du bloc de données lu
	 * dans le fichier passé en paramètre.
	 * 
	 * @param filename
	 *            Fichier SX comprenant un bloc de données
	 * 
	 * @return La liste de tuples (mot,1) par une instance d'objet
	 *         MappedDataList
	 * 
	 * @throws IOException
	 *             Exception si problème de lecture
	 */
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

	/**
	 * MAPPING
	 * 
	 * Traitement du bloc de données contenu dans le fichier SX identifié par id
	 * d'entrée
	 * 
	 * Création d'un fichier UMX<id>.txt comportant sur chaque ligne un mot et
	 * le chiffre 1, ces deux informations étant séparées par un espace.
	 * 
	 * La procédure affiche la liste des mots traités sur la sortie standard
	 * 
	 * @param id
	 *            Identifiant du fichier SX
	 *
	 * @throws IOException
	 *             Exception si problème de lecture et/ou d'écriture
	 */
	private void sxToUmx(String id) throws IOException {
		MappedDataList dataList = splitBlockFromFile(MappedData.getSxFullNameFile(id));
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(MappedData.getUmxFullNameFile(id))));
		writer.println(dataList);
		writer.close();
		System.out.print(dataList.getKeys());
		String message = MessageFormat.format("Mapping {0}", id);
		logger.info(message);

	}

	/**
	 * SHUFFLING
	 * 
	 * Un fichier SMX<key>.txt est produit en rassemblant les occurrences du mot
	 * touvées pour l'ensemble des fichiers UMX du paramètre umxList
	 * 
	 * Le fichier SMX<key>.txt comporte sur chaque ligne le mot et le chiffre 1
	 * (une ligne par occurrence trouvée)
	 * 
	 * @param key
	 *            mot
	 * 
	 * @param umxList
	 *            Liste des identifiant UMX
	 * 
	 * @throws IOException
	 *             Exception si problème de lecture et/ou d'écriture
	 */
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
		String message = MessageFormat.format("Shuffling {0}", key);
		logger.info(message);
	}

	/**
	 * REDUCE
	 * 
	 * Production d'un fichier RMX<key>.txt aggrégeant le nombre d'occurrences
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
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(MappedData.getRmxFullNameFile(key))));
		BufferedReader reader = new BufferedReader(new FileReader(MappedData.getSmxFullNameFile(key)));
		MappedData mdata = MappedData.createAndSumFrom(key, reader);
		writer.write(mdata.toString());
		reader.close();
		writer.close();
		System.out.print(mdata.toString());
		String message = MessageFormat.format("Reduce {0}", key);
		logger.info(message);
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
			} catch (UnknownHostException e1) {
				logger.error("Call to slave failed", e1);
			}
		}

	}

}
