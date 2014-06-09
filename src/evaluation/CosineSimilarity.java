package evaluation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CosineSimilarity
{
	/*
	 * ���������ַ�(Ӣ���ַ�)�����ƶȣ��򵥵����Ҽ��㣬δ��Ȩ��
	 */
     public static double getSimilarDegree(String str1, String str2)
     {
    	//���������ռ�ģ�ͣ�ʹ��mapʵ�֣�����Ϊ���ֵΪ����Ϊ2�����飬����Ŷ�Ӧ�������ַ��еĳ��ִ���
    	 Map<String, int[]> vectorSpace = new HashMap<String, int[]>();
    	 int[] itemCountArray = null;//Ϊ�˱���Ƶ������ֲ����������Խ�itemCountArray�����ڴ�
    	 
    	 //�Կո�Ϊ�ָ���ֽ��ַ�
    	 String strArray[] = str1.split(" ");
    	 for(int i=0; i<strArray.length; ++i)
    	 {
    		 if(vectorSpace.containsKey(strArray[i]))
    			 ++(vectorSpace.get(strArray[i])[0]);
    		 else
    		 {
    			 itemCountArray = new int[2];
    	         itemCountArray[0] = 1;
    	         itemCountArray[1] = 0;
    	         vectorSpace.put(strArray[i], itemCountArray);
    		 }
    	 }
    	 
    	 strArray = str2.split(" ");
    	 for(int i=0; i<strArray.length; ++i)
    	 {
    		 if(vectorSpace.containsKey(strArray[i]))
    			 ++(vectorSpace.get(strArray[i])[1]);
    		 else
    		 {
    			 itemCountArray = new int[2];
    	         itemCountArray[0] = 0;
    	         itemCountArray[1] = 1;
    	         vectorSpace.put(strArray[i], itemCountArray);
    		 }
    	 }
    	 
    	 //�������ƶ�
    	 double vector1Modulo = 0.00;//����1��ģ
    	 double vector2Modulo = 0.00;//����2��ģ
    	 double vectorProduct = 0.00; //������
    	 Iterator iter = vectorSpace.entrySet().iterator();
    	 
    	 while(iter.hasNext())
    	 {
    		 Map.Entry entry = (Map.Entry)iter.next();
    		 itemCountArray = (int[])entry.getValue();
    		 
    		 vector1Modulo += itemCountArray[0]*itemCountArray[0];
    		 vector2Modulo += itemCountArray[1]*itemCountArray[1];
    		 
    		 vectorProduct += itemCountArray[0]*itemCountArray[1];
    	 }
    	 
    	 vector1Modulo = Math.sqrt(vector1Modulo);
    	 vector2Modulo = Math.sqrt(vector2Modulo);
    	 
    	 //�������ƶ�
		return (vectorProduct/(vector1Modulo*vector2Modulo));
     }
     
     /*
      * 
      */
     public static void main(String args[]) throws Exception
     {
         String outputPath = "C:/z-ling/task/HEY/ResultTest/consineSim.txt";
         FileOutputStream fos=new FileOutputStream(outputPath);
         OutputStreamWriter osw=new OutputStreamWriter(fos);
         BufferedWriter bw=new BufferedWriter(osw);
         
         String  fileName = "C:/z-ling/task/HEY/ResultTest/dataset.txt";
         File file = new File(fileName);
         BufferedReader reader = null;
         reader = new BufferedReader(new FileReader(file));
         String tempString = null;
        ArrayList<String> list = new ArrayList<String>();
         
         while ((tempString = reader.readLine()) != null) {
             list.add(tempString);
         }
         reader.close();
         
         final int threshold = 3;
         for(int i = 0 ; i< list.size(); i++){
             
             String [] tmp = list.get(i).split("\t",2);
             String num = tmp[0];
             String str1 = tmp[1];

            for(int j = i+1; j<list.size(); j++){
                
                String [] tmp2 = list.get(j).split("\t", 2);
                String num2 = tmp2[0];
                String str2 = tmp2[1];
                
                String result = null;
                result = num + "\t"+num2+"\t"+CosineSimilarity.getSimilarDegree(str1, str2);
                bw.write(result);
                bw.newLine();
            }
            
         }
        
         bw.close();

     }
}
