import data.MappedData;

public class ShavaProcess {
	enum Status {
		RUNNING, FAILED
	};

	private String slaveName;
	private Process process;
	private String id;
	private Status status;
	private MappedData.Task taskType;

	ShavaProcess(String id, String name, Process process, MappedData.Task taskType) {
		this.setSlaveName(name);
		this.setId(id);
		this.process = process;
		this.status = Status.RUNNING;
		this.taskType = taskType;
	}

	public int waitFor() throws InterruptedException {
		return (this.process.waitFor());
	}

	public String getSlaveName() {
		return slaveName;
	}

	public void setSlaveName(String name) {
		this.slaveName = name;
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

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public MappedData.Task getTaskType() {
		return taskType;
	}

	public void setTaskType(MappedData.Task taskType) {
		this.taskType = taskType;
	}

}
