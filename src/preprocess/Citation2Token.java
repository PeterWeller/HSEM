package preprocess;

// extract tokens from citations
//programmed by Hung-sik Kim
import java.io.*;
import java.util.*;

public class Citation2Token {

	// change : Ncitations, kindTokens
	// generate a ground vector and tokens for each record

	// 1. Generation a ground vector according to both input data and selection
	// of tokens
	public static void main(String[] args) throws Exception {
		// Read an original citation text file
		int kindTokens = 3;// 1:firstTwo, 2:charBiGram, 3:charTriGram,
							// 4:wordUniGram
		String Ncitations = "20000000";
		String ReadFileLocation = "/home/hellisk/data/citeseer/";
		BufferedReader citationRead = new BufferedReader(new FileReader(
				ReadFileLocation + "kim_" + Ncitations + ".txt"));
		// select the location of output files
		String WriteFileLocation = "/home/hellisk/data/citeseer/";
		Citation2Token kim = new Citation2Token(); // avoid static method.
		kim.vectorsAndTokens(Ncitations, kindTokens, ReadFileLocation,
				WriteFileLocation, citationRead);
	}// end of main()

	void vectorsAndTokens(String Ncitations, int kindTokens,
			String ReadFileLocation, String WriteFileLocation,
			BufferedReader citationRead) throws Exception {
		double sTime = System.currentTimeMillis();
		double lap1 = 0.0;
		double lap2 = 0.0;
		double dummy = 0.0;
		int Ttokens = 0;
		String TokenName = null;
		String fileNameTag = null;

		switch (kindTokens) {
		case 1:
			fileNameTag = "firstTwo";
			break;
		case 2:
			fileNameTag = "charBiGram";
			break;
		case 3:
			fileNameTag = "charTriGram";
			break;
		case 4:
			fileNameTag = "wordUniGram";
			break;
		default:
			fileNameTag = "charTriGram";
			break;
		}

		switch (kindTokens) {
		case 1:
			TokenName = WriteFileLocation + "tokens_" + Ncitations + "_"
					+ fileNameTag + ".txt";
			break;
		case 2:
			TokenName = WriteFileLocation + "tokens_" + Ncitations + "_"
					+ fileNameTag + ".txt";
			break;
		case 3:
			TokenName = WriteFileLocation + "tokens_" + Ncitations + "_"
					+ fileNameTag + ".txt";
			break;
		case 4:
			TokenName = WriteFileLocation + "tokens_" + Ncitations + "_"
					+ fileNameTag + ".txt";
			break;
		default:
			TokenName = WriteFileLocation + "tokens_" + Ncitations + "_"
					+ fileNameTag + ".txt";
			break;
		}
		// Write informatic words (bi- tri- uni grams)
		BufferedWriter bwbi = new BufferedWriter(new FileWriter(TokenName));

		// Write word Vector Hash to a data file
		FileOutputStream fos = new FileOutputStream(WriteFileLocation
				+ "GroundVector_" + Ncitations + "_" + fileNameTag + ".dat");
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		FileOutputStream foVectorDimension = new FileOutputStream(
				WriteFileLocation + "GV_Dimensions_" + Ncitations + "_"
						+ fileNameTag + ".dat");
		ObjectOutputStream ooVectorDimension = new ObjectOutputStream(
				foVectorDimension);

		// Make a hash table for stop words (stop list)
		HashMap<String, String> stopWordHash = new HashMap<String, String>();
		BufferedReader stopListRead = new BufferedReader(new FileReader(
				"file/stoplist_citation.txt"));
		String stopWord;
		while ((stopWord = stopListRead.readLine()) != null) {
			stopWordHash.put(stopWord, stopWord);
		}

		// Initialize word hash table for a ground vector
		HashMap<String, Integer> wordVectorHash = new HashMap<String, Integer>();
		int hashValue = 0; // index of a vector

		String line;
		final int N_column = 15;
		int documentID = 0;
		int currentID = 0;
		StringTokenizer st;

		// variables for test or evaluation of this program
		int kimI = 0, kim2 = 0;

		// contains Vector dimension (different tokens), put xx instead of
		// "space".
		// we will use this to combine buckets in unsupervised LSH
		String vectorDimension = null;
		int indicator = 1;
		while ((line = citationRead.readLine()) != null) {
			// System.out.println(indicator++);
			String[] lines = new String[N_column];
			lines = line.split("\t");
			if (lines[1] != null)
				documentID = Integer.parseInt(lines[1]);// save document ID
			if (lines[0] != null)
				currentID = Integer.parseInt(lines[0]);// save document ID

			// Remove special characters and numbers such as . , " [ -. 1 2 3 4
			// ...
			String lineRSC = null;
			if (lines[14] != null)
				lineRSC = lines[14].replaceAll("[\\W&&\\S]", " "); // remove
																	// special
																	// characters
			if (lines[14] != null)
				lineRSC = lineRSC.replaceAll("[\\d]", " "); // remove numbers

			// Make alphabets lower case
			String lineRSClower = null;
			if (lines[14] != null)
				lineRSClower = lineRSC.toLowerCase();

			// Make tokens of citation
			// if(lineRSClower!=null) st = new StringTokenizer(lineRSClower);
			st = new StringTokenizer(lineRSClower);
			String[] tokenWord = new String[st.countTokens()];
			int j = 0; // j is index of tokens in one line
			while (st.hasMoreTokens()) {
				tokenWord[j] = st.nextToken();
				j++;
			}

			// Remove simple words
			String[] tempWord = new String[tokenWord.length]; // temporary store
																// important
																// words
			int numberOfInfoWord = 0;
			for (j = 0; j < tokenWord.length; j++) {
				if (stopWordHash.containsKey(tokenWord[j]) == false) {
					tempWord[numberOfInfoWord] = tokenWord[j];
					numberOfInfoWord++;
				}
			}
			// Save important words in infoWord variable
			String[] infoWord = new String[numberOfInfoWord];
			String infoLine = null;
			for (j = 0; j < numberOfInfoWord; j++) {
				infoWord[j] = tempWord[j];
				// Make lines which contain only informatic words to generate
				// char-level bi- tri- gram
				if (infoLine == null)
					infoLine = infoWord[j];
				else
					infoLine = infoLine + " " + infoWord[j];
			}

			// extract tokens (first two, char-bi, char-tri, word-uni gram) from
			// words.
			// write tokens to a "txt" file
			if (infoLine != null && infoLine.length() > 1) {
				bwbi.write(currentID + "\t" + documentID + "\t");
				String[] tokens = null;
				if (kindTokens == 2 || kindTokens == 3) {
					if (kindTokens == 2) {
						tokens = new String[infoLine.length() - 1];
						for (int i = 0; i < infoLine.length() - 1; i++) {
							tokens[i] = infoLine.substring(i, i + 2);
							tokens[i] = tokens[i].replaceAll(" ", "XX");
							bwbi.write(tokens[i] + "\t");
						}
					} else {
						tokens = new String[infoLine.length() - 2];
						for (int i = 0; i < infoLine.length() - 2; i++) {
							tokens[i] = infoLine.substring(i, i + 3);
							tokens[i] = tokens[i].replaceAll(" ", "XX");
							// System.out.println(tokens[i]);
							bwbi.write(tokens[i] + "\t");
						}
					}
				} else {
					if (kindTokens == 1) {
						tokens = new String[infoWord.length];
						for (int k = 0; k < tokens.length; k++) {
							tokens[k] = infoWord[k].substring(0, 2);
							bwbi.write(tokens[k] + "\t");
						}
					} else {
						tokens = infoWord;
						for (int k = 0; k < tokens.length; k++) {
							bwbi.write(tokens[k] + "\t");
						}
					}
				}
				bwbi.write("\n");

				kimI = kimI + numberOfInfoWord;

				// make a word vector using a hash table
				// <key,value>=<word,index>
				double vectorGenerationTime = System.currentTimeMillis();
				for (int i = 0; i < tokens.length; i++) {
					if (wordVectorHash.containsKey(tokens[i]) == false) {
						wordVectorHash.put(tokens[i], hashValue);
						hashValue++;
						double dummystart = System.currentTimeMillis();
						String newToken = "";
						for (int k = 0; k < tokens[i].length(); k++) {
							if (tokens[i].substring(k, k + 1).equals(" ")) {
								newToken = newToken + "XX";
							} else {
								newToken = newToken
										+ tokens[i].substring(k, k + 1);
							}
						}

						if (vectorDimension == null) {
							vectorDimension = newToken;
						} else {
							vectorDimension = vectorDimension + "\t" + newToken;
						}
						dummy += System.currentTimeMillis() - dummystart;
					}
				}
				Ttokens += tokens.length;
				lap1 += System.currentTimeMillis() - vectorGenerationTime;
			} else {
				System.out.println("No informatic tokens in this line :\t"
						+ indicator);
			}
			indicator++;
		}// end of while(read line from a text file)
		bwbi.close();
		lap2 = (System.currentTimeMillis() - sTime - lap1 - dummy) / 1000.0;
		lap1 = (lap1 - dummy) / 1000.0;
		System.out.println("Vector Generation Time:\t" + lap1);
		System.out.println("Tokens Generation Time:\t" + lap2);

		BufferedWriter bwlog = new BufferedWriter(new FileWriter(
				WriteFileLocation + "log_" + Ncitations + "_" + fileNameTag
						+ ".txt"));
		bwlog.write("Totle number of words =\t" + kimI + "\n");
		bwlog.write("Total vector dimension :\t" + hashValue);
		bwlog.close();

		System.out.println("Totle number of words =\t" + kimI); // show the
																// total number
																// of words
		System.out.println("Totle number of tokens =\t" + Ttokens); // show the
																	// total
																	// number of
																	// words
		System.out.println("Total vector dimension :\t" + hashValue); // show
																		// the
																		// dimension
																		// of a
																		// vector

		// Write word Vector Hash to a data file
		oos.writeObject(wordVectorHash);
		oos.close();
		ooVectorDimension.writeObject(vectorDimension);
		ooVectorDimension.close();
	}
}
