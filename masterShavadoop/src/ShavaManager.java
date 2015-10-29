import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Stack;

import data.MappedData;

public class ShavaManager {
	public String getRunningMachinesNameFile() {
		return MappedData.getDataDir() + "/" + Master.RUNNING_MACHINES_FILE;
	}

	HashMap<String, Stack<ShavaProcess>> slaves = new HashMap<String, Stack<ShavaProcess>>();
	Stack<ShavaProcess> shavaStack = new Stack<ShavaProcess>();

	public ShavaManager() {

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
			return slaves.keySet().iterator().next();
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
			if (sexec.processCmd(cmd, false, true) == 1) {
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
		ShavaProcess sp = new ShavaProcess(id, ordi, p, taskType);
		Stack<ShavaProcess> pStack = slaves.get(ordi);
		pStack.push(sp);
		slaves.put(ordi, pStack);
		shavaStack.push(sp);
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
