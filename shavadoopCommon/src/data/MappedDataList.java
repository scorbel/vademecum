package data;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

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

	private void add(MappedData mData) {
		data.add(mData);
	}

	public void sort() {
		Collections.sort(data);
	}

	public String toString() {
		String result = "";
		String sep = "";
		for (MappedData mdata : data) {
			result += sep + mdata.toString();
			sep = System.lineSeparator();
		}
		return result;
	}

	public String getKeys() {
		String result = "";
		HashMap<String, String> wordMap = new HashMap<String, String>();
		for (MappedData mappedData : data) {
			wordMap.put(mappedData.getMot(), mappedData.getMot());
		}
		String sep = "";
		for (String word : wordMap.keySet()) {
			result += sep + word;
			sep = System.lineSeparator();
		}
		return result;

	}

	public static MappedDataList createAndFilterFrom(BufferedReader reader, String key) throws IOException {
		MappedDataList result = new MappedDataList();
		String line = null;
		while ((line = reader.readLine()) != null) {
			line = line.trim();
			if (line.length() > 0) {
				MappedData mData = MappedData.createFrom(line);
				if (mData.getMot().equals(key)) {
					result.add(mData);
				}
			}
		}

		return result;
	}

}
