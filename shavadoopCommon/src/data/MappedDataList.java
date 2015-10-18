package data;
import java.util.ArrayList;
import java.util.Collections;



public class MappedDataList {
	public static String Separator = " ";
	private ArrayList<MappedData> data = new ArrayList<MappedData>();
	public MappedDataList(String line) {
		String tokens[] = line.split(Separator);
		for ( String token : tokens) {
			data.add(new MappedData(token));
		}
	}
	
	public void sort() {
		Collections.sort(data);
	}
	

}
