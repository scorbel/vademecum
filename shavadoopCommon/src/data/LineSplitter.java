package data;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class LineSplitter extends Splitter {

	public LineSplitter(String filename) throws FileNotFoundException {
		super(filename);
	}

	@Override
	public String readBlock() throws IOException {
		return reader.readLine();
	}

	@Override
	public ArrayList<String> split(String data) {
		String noNewLine = data.replaceAll("\\r?\\n", "");
		String[] tokens = noNewLine.split("\\W+");
		return new ArrayList<String>(Arrays.asList(tokens));
	}

}
