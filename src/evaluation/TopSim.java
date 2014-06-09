package evaluation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TopSim {
    
    public static void main(String args[]) throws Exception
    {
        final int TOP = 200;
        
        String outputPath = "C:/z-ling/task/HEY/ResultTest/HamTopSim_"+TOP+".txt";
        FileOutputStream fos=new FileOutputStream(outputPath);
        OutputStreamWriter osw=new OutputStreamWriter(fos);
        BufferedWriter bw=new BufferedWriter(osw);
        
        String  fileDir = "C:/z-ling/task/HEY/ResultTest/testDataset2";
        File f=new File( fileDir);
        File[] fs=f.listFiles();
        
        HashMap<String,Float> map = new HashMap<String,Float>();
        
        for(int i=0;i<fs.length;i++){
            
          if(fs[i].isFile()){
          File file = new File(fs[i].toString());
          BufferedReader reader = null;
          reader = new BufferedReader(new FileReader(file));
          String tempString = null;
          while ((tempString = reader.readLine()) != null) {
              
              String tmp[] = tempString.split("\t" , 3);
              String key = tmp[0]+"\t"+tmp[1];
              String value = tmp[2];
              map.put(key, Float.parseFloat(value));
              
           }
           reader.close();
          }

          
        }  

        
        
        //sort
        List<Map.Entry<String, Float>> infoIds =
                new ArrayList<Map.Entry<String, Float>>(map.entrySet());

//            //����ǰ
//            for (int i = 0; i < infoIds.size(); i++) {
//                String id = infoIds.get(i).toString();
//                System.out.println(id);
//            }

            //����
            Collections.sort(infoIds, new Comparator<Map.Entry<String, Float>>() {   
                public int compare(Map.Entry<String, Float> o1, Map.Entry<String, Float> o2) {   
                    //sorted by value
                    if(o2.getValue() - o1.getValue()>= 0)
                        return 1;
                    else
                        return -1;
                    
                    //sorted by key
                  //  return (o1.getKey()).toString().compareTo(o2.getKey());
                }
            }); 

            //�����
            for (int i = 0; i < infoIds.size() && i< TOP; i++) {
                String id = infoIds.get(i).toString();
                String[] s = id.split("="); 
                bw.write(s[0]+"\t"+s[1]);
                bw.newLine();
            }
            
            bw.close();
            System.out.println("Compeleted");
    }
}
