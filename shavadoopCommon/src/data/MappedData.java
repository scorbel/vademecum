package data;

import java.util.Date;

public class MappedData implements Comparable<MappedData> {

	private static String DATA = "data";
	private static String SX_SUFFIXE = "SX";
	private static String UMX_SUFFIXE = "UMX";
	private static String TXT_EXT = ".txt";

	public static String getDataDir() {
		return System.getProperty("user.home") + "/" + DATA;
	}

	public static String getSxNameFile(String sId) {
		return getShaNameFile(sId, SX_SUFFIXE, null);
	}

	public static String getUmxNameFile(String sId) {
		return getShaNameFile(sId, UMX_SUFFIXE, null);
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

	public static String getTimestamp() {
		return Long.toString(new Date().getTime());
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

}
