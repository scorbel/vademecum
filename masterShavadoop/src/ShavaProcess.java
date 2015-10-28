public class ShavaProcess {
	private String name;
	private Process process;
	private String id;

	ShavaProcess(String id, String name, Process process) {
		this.setName(name);
		this.setId(id);
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

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

}
