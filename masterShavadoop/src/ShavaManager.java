import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Stack;

import data.MappedData;

public class ShavaManager {
	private int maxProcess = 20;

	public String getRunningMachinesNameFile() {
		return MappedData.getDataDir() + "/" + Master.RUNNING_MACHINES_FILE;
	}

	HashMap<String, Stack<ShavaProcess>> slaves = new HashMap<String, Stack<ShavaProcess>>();
	Stack<ShavaProcess> shavaStack = new Stack<ShavaProcess>();

	public ShavaManager(int maxProcess) {
		this.maxProcess = maxProcess;
	}

	public String getFirstAvailableSlave() {
		String result = null;
		for (String slave : slaves.keySet()) {
			if (slaves.get(slave).isEmpty()) {
				result = slave;
				break;
			}
		}
		if (result == null) {
			Random random = new Random();
			List<String> keys = new ArrayList<String>(slaves.keySet());
			String randomKey = keys.get(random.nextInt(keys.size()));
			return randomKey;
		}
		return (result);

	}

	public int getRunningMachines(String filename) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(filename));
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(getRunningMachinesNameFile())));

		ShavaExec sexec = new ShavaExec();
		String line;
		int result = 0;
		while ((line = reader.readLine()) != null) {
			String ordi = line.trim();
			String[] cmd = sexec.getSshCmd("hostname", ordi);
			if (sexec.processCmd(cmd, false, true) != null) {
				slaves.put(ordi, new Stack<ShavaProcess>());
				writer.println(ordi);
				result++;
			}

		}
		reader.close();
		writer.close();
		return result;

	}

	public String pushJob(String id, ArrayList<String> argList, MappedData.Task taskType) {
		if (this.shavaStack.size() > this.maxProcess) {
			return null;
		}
		String ordi = getFirstAvailableSlave();
		ShavaExec sexec = new ShavaExec();
		Process p = null;
		try {
			switch (taskType) {
			case SX:
				p = sexec.processSxCmd(ordi, id);
				break;
			case UMX:
				p = sexec.processUmxCmd(ordi, id, argList);
				break;
			case SMX:
				p = sexec.processSmxCmd(ordi, id);
				break;
			default:
				break;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (p == null) {
			// TODO sélectionner une autre machine
			return null;
		}
		ShavaProcess sp = new ShavaProcess(id, ordi, sexec, taskType);
		Stack<ShavaProcess> pStack = slaves.get(ordi);
		pStack.push(sp);
		slaves.put(ordi, pStack);
		shavaStack.push(sp);
		String message = MessageFormat.format("job {0} {1} on {2} pushed", taskType.toString(), id, ordi);
		Master.logger.info(message);
		return ordi;
	}

	public ShavaProcess popJob() {
		ShavaProcess sp = shavaStack.pop();
		Stack<ShavaProcess> pStack = slaves.get(sp.getSlaveName());
		pStack.pop();
		return sp;
	}

	public boolean stackEmpty() {
		return this.shavaStack.empty();
	}

}
