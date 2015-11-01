import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class StreamGobbler extends Thread {
	InputStream is;
	StringBuilder outputBuffer = new StringBuilder();

	public StreamGobbler(InputStream is) {
		this.is = is;
	}

	public StringBuilder getOutputBuffer() {
		return outputBuffer;
	}

	@Override
	public void run() {
		try {
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			String line = null;
			while ((line = br.readLine()) != null)
				outputBuffer.append(line);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
