package evaluation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class AccuracyTest {
    
    public static void main(String args[]) throws Exception
    {
        final int TOP = 200;
        String outputPath = "C:/z-ling/task/HEY/ResultTest/AccuracyResult_"+TOP+".txt";
        FileOutputStream fos=new FileOutputStream(outputPath);
        OutputStreamWriter osw=new OutputStreamWriter(fos);
        BufferedWriter bw=new BufferedWriter(osw);
        
        //Cosine Similarity
        String  fileName1 = "C:/z-ling/task/HEY/ResultTest/TopSim_"+TOP+".txt";
        File file1 = new File(fileName1);
        BufferedReader reader1 = null;
        reader1 = new BufferedReader(new FileReader(file1));
        String tempString1 = null;
       HashMap<String,Float> map1 = new HashMap<String,Float>();
        
       final int consineSimTop = 200;
       int i =  0;
        while ((tempString1 = reader1.readLine()) != null && i<consineSimTop) {
            i++;
            String[] s = tempString1.split("\t",3);
            
            String key = null;
            if(Long.valueOf(s[0])<Long.valueOf(s[1]))
                     key = s[0]+"\t"+s[1];
            else
                    key = s[1]+"\t"+s[0];
            
            float value = Float.valueOf(s[2]);
            map1.put(key, value);
           // System.out.println(s[0]+"\t"+s[1]+value);
        }
        System.out.println("map1 size: "+map1.size());
        bw.write("using cosine similarity: "+map1.size());
        bw.newLine();
        reader1.close();
        
        //HSEM
        String  fileName2 = "C:/z-ling/task/HEY/ResultTest/HamTopSim_"+TOP+"2.txt";
        File file2 = new File(fileName2);
        BufferedReader reader2 = null;
        reader2 = new BufferedReader(new FileReader(file2));
        String tempString2 = null;
        HashMap<String,Float> map2 = new HashMap<String,Float>();
       
       final int hammingTop = 200;
       int j = 0;
        while ((tempString2 = reader2.readLine()) != null&& j<hammingTop) {
            j++;
            String[] s = tempString2.split("\t",3);
            
            String key = null;
            if(Long.valueOf(s[0])<Long.valueOf(s[1]))
                     key = s[0]+"\t"+s[1];
            else
                    key = s[1]+"\t"+s[0];
            
            float value = Float.valueOf(s[2]);
            map2.put(key, value);
           // System.out.println(s[0]+"\t"+s[1]+value);
        }
        System.out.println("map2 size: "+map2.size());
        bw.write("using hamming distance: "+map2.size());
        bw.newLine();
        reader2.close();
        
        int match = 0;
        Iterator iter = map2.entrySet().iterator();
        while (iter.hasNext()) {
        Map.Entry entry = (Map.Entry) iter.next();
        Object key = entry.getKey();
       // Object val = entry.getValue();
        if(map1.containsKey(key))
            match++;
        }
        
        double rate = ((double)match)/((double)map2.size());
        System.out.println("Match Rate: "+rate);
        bw.write("Match Rate: "+rate);
        bw.newLine();
        bw.close();
 
    }
}
