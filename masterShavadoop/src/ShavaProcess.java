public class ShavaProcess {
	private String name;
	private Process process;

	ShavaProcess(String name, Process process) {
		this.setName(name);
		this.process = process;
	}

	public int waitFor() throws InterruptedException {
		return (this.process.waitFor());
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Process getProcess() {
		return this.process;
	}

}
