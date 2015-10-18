import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ShavaExec {
	private static String SLAVE_PROJECT_NAME = "slaveShavadoop/bin";
	private static String LOGIN = "scorbel";

	private static String getSlaveJar() {
		return Master.getUserDir() + "/../" + SLAVE_PROJECT_NAME + "/slave.jar";
	}

	private static String javaJar() {
		return "java -jar ";
	}

	public ShavaExec() {
	}

	String[] getSshCmd(String cmd, String slaveName) {
		String[] result = new String[3];
		result[0] = "ssh";
		result[2] = cmd;

		if (Master.isWindows()) {
			result[1] = slaveName;
		} else {
			result[1] = LOGIN + "@" + slaveName;
		}
		return result;
	}

	public Process processCmd(String[] cmd) {
		Process p = null;
		try {
			ProcessBuilder pb = new ProcessBuilder(cmd);
			p = pb.start();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return p;

	}

	private int getInputStream(boolean echo, Process p) throws IOException {
		int error = 1;
		String s = null;
		BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
		BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
		while ((s = stdInput.readLine()) != null) {
			if (echo)
				System.out.println(s);
		}

		while ((s = stdError.readLine()) != null) {
			if (echo)
				System.out.println(s);
			error = 0;
		}
		return error;
	}

	public int processCmd(String[] cmd, boolean echo, boolean wait) {
		try {
			Process p = processCmd(cmd);
			if (wait)
				p.waitFor();
			return getInputStream(echo, p);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return 0;

	}

	public int execCmd(String[] cmd, boolean echo) {
		try {

			Process p = Runtime.getRuntime().exec(cmd);
			p.waitFor();
			return getInputStream(echo, p);

		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		return 0;
	}

	public int callMachine(String ordi, boolean wait) {
		String javaCmd = javaJar() + getSlaveJar();
		String[] cmd = getSshCmd(javaCmd, ordi);
		return processCmd(cmd, true, wait);

	}

	public Process processSxCmd(String ordi, String filename) {
		String javaCmd = javaJar() + getSlaveJar() + " -SX " + filename;
		String[] cmd = getSshCmd(javaCmd, ordi);
		return processCmd(cmd);
	}

}
