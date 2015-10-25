package data;

import java.util.ArrayList;
import java.util.Collections;

public class MappedDataList {
	public static String Separator = " ";
	private ArrayList<MappedData> data = new ArrayList<MappedData>();

	public MappedDataList() {
	}

	public void addLine(String line) {
		String tokens[] = line.split(Separator);
		for (String token : tokens) {
			data.add(new MappedData(token));
		}
	}

	public void sort() {
		Collections.sort(data);
	}

	public String toString() {
		String result = "";
		String sep = "";
		for (MappedData mdata : data) {
			result += sep + mdata.toString();
			sep = "\n";
		}
		return result;
	}

}
