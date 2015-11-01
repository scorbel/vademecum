package data;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;

public class BlockSplitter extends Splitter {

	public static ArrayList<String> Stopwords = new ArrayList(Arrays.asList("a", "about", "above", "above", "across",
			"after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also", "although",
			"always", "am", "among", "amongst", "amoungst", "amount", "an", "and", "another", "any", "anyhow", "anyone",
			"anything", "anyway", "anywhere", "are", "around", "as", "at", "back", "be", "became", "because", "become",
			"becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides",
			"between", "beyond", "bill", "both", "bottom", "but", "by", "call", "can", "cannot", "cant", "co", "con",
			"could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg",
			"eight", "either", "eleven", "else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every",
			"everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first",
			"five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get",
			"give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein",
			"hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "i", "ie", "if", "in",
			"inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly",
			"least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more",
			"moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never",
			"nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere",
			"of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our",
			"ours", "ourselves", "out", "over", "own", "part", "per", "perhaps", "please", "put", "rather", "re",
			"same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side",
			"since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime",
			"sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them",
			"themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon",
			"these", "they", "thickv", "thin", "third", "this", "those", "though", "three", "through", "throughout",
			"thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un",
			"under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever",
			"when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon",
			"wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why",
			"will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves",
			"the"));

	private static int size = 1024;

	public static boolean isStopWord(String word) {
		return Stopwords.contains(word.toLowerCase());
	}

	private String inWords = null;

	public BlockSplitter(String filename) throws FileNotFoundException {
		super(filename);
	}

	@Override
	public String readBlock() throws IOException {
		char[] buff = new char[size];
		int readLen = reader.read(buff);
		String result = "";
		if (readLen == -1)
			return null;
		if (inWords != null) {
			result = new String(inWords);
			inWords = null;
		}

		if (readLen < size) {
			// offset += readLen;
			String str = new String(buff);
			result += str.substring(0, readLen);
		} else {

			BreakIterator bi = BreakIterator.getWordInstance();
			String text = new String(buff);
			bi.setText(text);
			if (bi.isBoundary(size - 1)) {
				result += text.substring(0, size - 1);
			} else {
				int preceding = bi.preceding(size - 1);
				inWords = text.substring(preceding, size);
				result += text.substring(0, preceding - 1);
			}
		}
		return result;
	}

	public ArrayList<String> split(String data) {
		ArrayList<String> result = new ArrayList<String>();
		BreakIterator bi = BreakIterator.getWordInstance(Locale.US);
		bi.setText(data);
		int lastIndex = bi.first();
		while (lastIndex != BreakIterator.DONE) {
			int firstIndex = lastIndex;
			lastIndex = bi.next();
			if (lastIndex != BreakIterator.DONE && Character.isLetterOrDigit(data.charAt(firstIndex))) {
				String word = data.substring(firstIndex, lastIndex);
				if (!isStopWord(word)) {
					result.add(word);
				}
			}
		}
		return result;
	}

	public static void main(String[] args) {
		try {
			Splitter splitter = new BlockSplitter(args[0]);
			String b = null;
			while ((b = splitter.readBlock()) != null) {
				ArrayList<String> words = splitter.split(b);
				for (String word : words) {
					System.out.println(word);
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
