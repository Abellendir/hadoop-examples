package tfidf.reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer5 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		ArrayList<String> sentences = new ArrayList<>();
		ArrayList<String> words = new ArrayList<>();
		for (Text t : values) {
			String[] s = t.toString().split("<====>");
			if (s[0].equals("word")) {
				String word = s[1];
				String tf = s[2];
				words.add(word + "\t" + tf);
			} else {
				String s1 = new String(s[1]);
				if (!s1.equals(" ")) {
					sentences.add(s1);
				}
			}
		}

		context.write(new Text(key), new Text("sentences " + sentences.size() + "\t" + "words " + words.size()));

		double[] sentenceTFIDF = new double[sentences.size()];
		String[] masterString = new String[sentences.size()];

		for (int i = 0; i < sentences.size(); i++) {
			masterString[i] = sentences.get(i);
		}

		for (int i = 0; i < masterString.length; i++) {
			String s = masterString[i];
			StringTokenizer tokens = new StringTokenizer(s);
			ArrayList<String> modified = new ArrayList<>();
			ArrayList<Double> TFIDFSvalues = new ArrayList<>();
			while (tokens.hasMoreTokens()) {
				String out = tokens.nextToken().replaceAll("[^A-Za-z0-9]", "").toLowerCase();
				if (!out.equals("")) {
					if (!modified.contains(out)) {
						double d = 0.0;
						for (String prime : words) {
							if (prime.split("\t")[0].equals(out)) {
								d = Double.parseDouble(prime.split("\t")[1]);
								break;
							}
						}
						modified.add(out);
						TFIDFSvalues.add(d);
						// context.write(key, new Text(out + "\t" + d));
					}
				}
			}
			if (TFIDFSvalues.size() < 5) {
				double sum = 0.0;
				for (double d : TFIDFSvalues) {
					sum += d;
				}
				sentenceTFIDF[i] = sum;
			} else {
				sentenceTFIDF[i] = getTFIDF(modified, TFIDFSvalues);
			}
		}
		if (sentenceTFIDF.length < 4) {
			String s = "";
			for (int i = 0; i < sentenceTFIDF.length; i++) {
				s += ("<====>" + masterString[i]);
			}
			context.write(new Text(key), new Text(s));
		} else {
			int[] indexes = new int[3];
			indexes = getSentences(sentenceTFIDF);
			String s = "";
			for (int j = 0; j < indexes.length; j++) {
				s += ("<====>" + masterString[indexes[j]]);
			}
			context.write(new Text(key), new Text(s));

		}
	}

	private int[] getSentences(double[] sentenceTFIDF) {
		int[] indexes = new int[3];
		double d1 = 0.0;
		double d2 = 0.0;
		double d3 = 0.0;
		for (int i = 0; i < sentenceTFIDF.length; i++) {
			int d = i;
			int temp = 0;
			if (sentenceTFIDF[d] > sentenceTFIDF[indexes[0]]) {
				temp = indexes[0];
				indexes[0] = d;
				d = temp;
			}
			if (sentenceTFIDF[d] > sentenceTFIDF[indexes[1]]) {
				temp = indexes[1];
				indexes[1] = d;
				d = temp;
			}
			if (sentenceTFIDF[d] > sentenceTFIDF[indexes[2]]) {
				temp = indexes[2];
				indexes[2] = d;
				d = temp;
			}
		}
		return indexes;
	}

	public double getTFIDF(ArrayList<String> modified, ArrayList<Double> TFIDFSvalues) {
		double d1 = 0.0;
		double d2 = 0.0;
		double d3 = 0.0;
		double d4 = 0.0;
		double d5 = 0.0;
		for (double d : TFIDFSvalues) {
			double temp;
			if (d > d1) {
				temp = d1;
				d1 = d;
				d = temp;
			}
			if (d > d2) {
				temp = d2;
				d2 = d;
				d = temp;
			}
			if (d > d3) {
				temp = d2;
				d3 = d;
				d = temp;
			}
			if (d > d4) {
				temp = d2;
				d4 = d;
				d = temp;
			}
			if (d > d5) {
				d5 = d;
			}
		}
		return d1 + d2 + d3 + d4 + d5;
	}
}
