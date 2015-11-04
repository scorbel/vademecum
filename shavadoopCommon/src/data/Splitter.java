package data;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

public abstract class Splitter {
	protected BufferedReader reader = null;

	public Splitter(String filename) throws FileNotFoundException, UnsupportedEncodingException {
		reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "UTF-8"));
	}

	public void close() throws IOException {
		reader.close();
	}

	public abstract String readBlock() throws IOException;

	public abstract ArrayList<String> split(String data);
}
