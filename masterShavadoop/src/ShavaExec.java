import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class ShavaExec {
	private static String SLAVE_PROJECT_NAME = "slaveShavadoop/bin";
	private static String LOGIN = "scorbel";
	StreamGobbler errorGobbler;
	StreamGobbler outputGobbler;
	private Process p = null;

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

	public Process processCmd(String[] cmd) throws IOException {
		ProcessBuilder pb = new ProcessBuilder(cmd);
		p = pb.start();
		errorGobbler = new StreamGobbler(p.getErrorStream());
		outputGobbler = new StreamGobbler(p.getInputStream());
		errorGobbler.start();
		outputGobbler.start();
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

	public String getInputStream(Process p) throws IOException {
		StringBuffer result = new StringBuffer();
		String s = null;
		BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
		BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
		while ((s = stdInput.readLine()) != null) {
			result.append(s);
		}
		while ((s = stdError.readLine()) != null) {
			System.err.println(s);
			result = null;
		}
		return result.toString();

	}

	public String getInputStream() throws InterruptedException {
		String result = null;
		if (this.p != null) {
			// outputGobbler.interrupt();
			// errorGobbler.interrupt();
			outputGobbler.join();
			StringBuilder outputBuilder = outputGobbler.getOutputBuffer();
			if (outputBuilder.length() > 0)
				result = outputBuilder.toString();
		}
		return result;

	}

	public String getErrorStream() throws InterruptedException {
		String result = null;
		if (this.p != null) {
			// outputGobbler.interrupt();
			// errorGobbler.interrupt();
			errorGobbler.join();
			StringBuilder errorBuilder = errorGobbler.getOutputBuffer();
			if (errorBuilder.length() > 0) {
				result = errorBuilder.toString();
			}
		}
		return result;

	}

	public String processCmd(String[] cmd, boolean echo, boolean wait) {
		try {
			Process p = processCmd(cmd);
			if (wait)
				p.waitFor();
			return getInputStream();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;

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

	public String callMachine(String ordi, boolean wait) {
		String javaCmd = javaJar() + getSlaveJar();
		String[] cmd = getSshCmd(javaCmd, ordi);
		return processCmd(cmd, true, wait);

	}

	public Process processSxCmd(String ordi, String id) throws IOException {
		String javaCmd = javaJar() + getSlaveJar() + " -SX " + id;
		String[] cmd = getSshCmd(javaCmd, ordi);
		return processCmd(cmd);
	}

	public Process processUmxCmd(String ordi, String key, ArrayList<String> idList) throws IOException {
		String javaCmd = javaJar() + getSlaveJar() + " -UMX";
		javaCmd += " " + key;
		for (String id : idList) {
			javaCmd += " " + id;
		}
		String[] cmd = getSshCmd(javaCmd, ordi);
		// Master.logger.info(javaCmd);
		return processCmd(cmd);
	}

	public Process processSmxCmd(String ordi, String key) throws IOException {
		String javaCmd = javaJar() + getSlaveJar() + " -SMX " + key;
		String[] cmd = getSshCmd(javaCmd, ordi);
		return processCmd(cmd);
	}

	public int waitFor() throws InterruptedException {
		if (this.p == null)
			return 0;
		return p.waitFor();
	}

	public Process getProcess() {
		return this.p;
	}

}
