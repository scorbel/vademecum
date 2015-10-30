package data;

public class MappedData implements Comparable<MappedData> {

	private static String DATA = "data";
	private static String TXT_EXT = ".txt";

	public enum Task {
		SX, UMX, SMX, RMX
	};

	public static String getDataDir() {
		return System.getProperty("user.home") + "/" + DATA;
	}

	public static String getSxNameFile(String sId) {
		return getShaNameFile(sId, Task.SX.toString(), null);
	}

	public static String getUmxNameFile(String sId) {
		return getShaNameFile(sId, Task.UMX.toString(), null);
	}

	public static String getRmxNameFile(String sId) {
		return getShaNameFile(sId, Task.RMX.toString(), null);
	}

	public static String getSmxNameFile(String sId) {
		return getShaNameFile(sId, Task.SMX.toString(), null);
	}

	public static String getShaNameFile(String sId, String suffixe, String prefixe) {
		String str = suffixe + sId;
		if (prefixe != null && prefixe.length() > 0) {
			str += prefixe;
		}
		return str + TXT_EXT;
	}

	public static String getSxFullNameFile(String id) {
		return getDataDir() + "/" + getSxNameFile(id);
	}

	public static String getUmxFullNameFile(String id) {
		return getDataDir() + "/" + getUmxNameFile(id);
	}

	public static String getSmxFullNameFile(String id) {
		return getDataDir() + "/" + getSmxNameFile(id);
	}

	public static String getRmxFullNameFile(String id) {
		return getDataDir() + "/" + getRmxNameFile(id);
	}

	public static String getTimestamp() {
		return Long.toString(System.nanoTime());
	}

	public static String getId() {
		return getTimestamp();
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
		return mot + " " + counter;
	}

	public static MappedData createFrom(String str) {
		String[] tokens = str.split(" ");
		MappedData mData = new MappedData(tokens[0]);
		if (tokens.length >= 2) {
			mData.setCounter(Integer.parseInt(tokens[1]));
		}
		return mData;
	}

}
