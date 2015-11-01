package data;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public abstract class Splitter {
	protected BufferedReader reader = null;

	public Splitter(String filename) throws FileNotFoundException {
		reader = new BufferedReader(new FileReader(filename));
	}

	public void close() throws IOException {
		reader.close();
	}

	public abstract String readBlock() throws IOException;

	public abstract ArrayList<String> split(String data);
}
