package evaluation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;

public class TestRawData {
    public static void main(String[] args) throws Exception{
       
        final int TOTAL_COUNT = 200;
        String outputPath = "C:/z-ling/task/HEY/ResultTest/dataset.txt";
        FileOutputStream fos=new FileOutputStream(outputPath);
        OutputStreamWriter osw=new OutputStreamWriter(fos);
        BufferedWriter bw=new BufferedWriter(osw);
        
    String  fileName = "C:/z-ling/task/HEY/demo/Harra.Codes.and.Sample.Data/dataset/dongwon.2.txt";
    File file = new File(fileName);
   BufferedReader reader = null;
   
    try {

        reader = new BufferedReader(new FileReader(file));
        String tempString = null;
        int line = 1;
        int entityCount = 0;
       HashMap<String,Integer> map = new HashMap<String,Integer>();
        
        while ((tempString = reader.readLine()) != null && entityCount < TOTAL_COUNT) {
            
             line++;
         //    System.out.println(tempString);
            String tmp[] = tempString.split("\t");
            System.out.println(tmp[1]);
            if(!map.containsKey(tmp[1])){
                
                map.put(tmp[1], 1);
                
                bw.write(tempString);
                bw.newLine();
                
                entityCount++;
            }
            else if(map.get(tmp[1]) < 5 ){
  
                    bw.write(tempString);
                    bw.newLine();
                    
                    Integer count = map.get(tmp[1]);
                    count++;
                    map.put(tmp[1], count);
                
            }
            
           
            
        }
        reader.close();
        bw.close();
    } catch (IOException e) {
        e.printStackTrace();
    } finally {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e1) {
            }
        }
    }
    }
}
