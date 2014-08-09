package preprocess;

import java.io.*;

/**
 * Created by roy_gaoz on 2014/7/4.
 */

public class JustTry {
    static String[] keys = {"dc:title", "dc:creator", "dc:subject", "dc:contributor", "dc:description"};

    public static void main(String[] args) throws Exception {
        double startTime = System.currentTimeMillis();
        translateXML();
        System.out.println("Processing Time:\t"
                + (System.currentTimeMillis() - startTime) / 1000.0 + " s");
    }

    static void translateXML() throws Exception {
        BufferedReader jin = new BufferedReader(new FileReader("37.xml"));
        FileWriter citeSeer = new FileWriter("citeSeer.txt");
        FileWriter assistant = new FileWriter("assistant.txt");
        String line, lineMark, lineInfo, lines = "";
        int count = 0, position1, position2, position3;

        while ((line = jin.readLine()) != null) {
            position1 = line.indexOf('<');
            position2 = line.indexOf('>');
            lineMark=line.substring(position1 + 1, position2);
            //System.out.println(lineMark);

            if (isAKeyWord(lineMark)) {
                lineInfo = line.substring(position2 + 1, line.length());
                position3 = lineInfo.indexOf('<');
                while (position3 < 0) {
                    lineInfo += jin.readLine();
                    position3 = lineInfo.indexOf('<');
                }
                lineInfo = lineInfo.substring(0, position3);
                lines += lineInfo+" ";
            } else if (isASopWord(lineMark)) {
                if (!lines.equals("")) toToken(++count, lines, citeSeer, assistant);
                lines = "";
            }
        }

        System.out.println("\nTotal books processed:\t" + count);
        jin.close();
    }

    static void toToken(int number, String temp, FileWriter outputFile, FileWriter assistFile) throws Exception {
        //System.out.print(number + "\t" + temp + "\n");
        assistFile.write(number + "\t" + temp + "\n");

        String lineRSC = temp.replaceAll("[\\W&&\\S]", " ");
        lineRSC = lineRSC.replaceAll("[\\d]", " ");
        String lineRSClower = lineRSC.toLowerCase();

        outputFile.write(number + "\t");

        String[] tokens = new String[lineRSClower.length() - 2];
        for (int i = 0; i < lineRSClower.length() - 2; i++) {
            tokens[i] = lineRSClower.substring(i, i + 3);
            tokens[i] = tokens[i].replaceAll(" ", "XX");
            outputFile.write(tokens[i] + "\t");
        }

        outputFile.write("\n");
    }

    static boolean isAKeyWord(String str) {
        for (String eachS : keys)
            if (str.equals(eachS)) return true;
        return false;
    }

    static boolean isASopWord(String str) {
        return str.equals("/record");
    }
}

