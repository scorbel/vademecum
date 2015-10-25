package data;

public class MappedData implements Comparable<MappedData> {

	private static String DATA = "data";

	public static String getDataDir() {
		return System.getProperty("user.home") + "/" + DATA;
	}

	private String mot = new String();
	private Integer counter = 1;

	public MappedData(String mot, Integer counter) {
		this.setMot(mot);
		this.setCounter(counter);
	}

	public MappedData(String mot) {
		this.setMot(mot);
	}

	public String getMot() {
		return mot;
	}

	public void setMot(String mot) {
		this.mot = mot;
	}

	public Integer getCounter() {
		return counter;
	}

	public void setCounter(Integer counter) {
		this.counter = counter;
	}

	@Override
	public int compareTo(MappedData other) {
		return this.mot.compareTo(other.mot);
	}

	public String toString() {
		return mot + "," + counter;
	}

}
