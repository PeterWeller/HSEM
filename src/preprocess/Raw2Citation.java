package preprocess;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.StringTokenizer;

public class Raw2Citation {


    public static void main(String[] args) throws IOException{
        int Ncitations=20000000;
        BufferedReader b1Read  = new BufferedReader(new FileReader("/home/hellisk/data/citeseer/dongwon.1.txt"));
        BufferedReader b2Read  = new BufferedReader(new FileReader("/home/hellisk/data/citeseer/dongwon.2.txt"));      
  
        //BufferedReader b1Read  = new BufferedReader(new FileReader("C:\\z-ling\\task\\HEY\\demo\\Harra.Codes.and.Sample.Data\\dataset\\sample1.txt"));
       // BufferedReader b2Read  = new BufferedReader(new FileReader("C:\\z-ling\\task\\HEY\\demo\\Harra.Codes.and.Sample.Data\\dataset\\sample2.txt"));        
  
           File ofile = new File("/home/hellisk/data/citeseer/kim_20000000.txt");
            PrintWriter bWrite  = new PrintWriter(new FileOutputStream(ofile, true));
        //BufferedWriter bWrite  = new BufferedWriter(new FileWriter("C:\\z-ling\\task\\HEY\\demo\\Harra.Codes.and.Sample.Data\\dataset\\kim_20000000.txt"));
        String line;
        final int N_column=15;
        StringTokenizer st = new StringTokenizer("");
        int stopWhile=1;        
        boolean checkLine=true; // if line is correct
        boolean preLine=true;// if previous line is correct
        String tempLine=null;
        String[] lines = new String[N_column];
        while ((line = b1Read.readLine()) != null) {
            if(line.substring(line.length()-1,line.length()).equals("\\")){
                if(tempLine==null){
                    tempLine=line.substring(0,line.length()-1);
                }
                else{
                    tempLine+=line.substring(0,line.length()-1);
                }
                
                checkLine=false;// A line contains error
            }
            else{
                if (checkLine){
                    bWrite.write(line+"\n");                    
                }
                else{
                    line=tempLine+"fl"+line;
                    checkLine=true;
                    bWrite.write(line+"\n");
                    tempLine=null;
                }
                stopWhile++;
                lines = line.split("\t");
            }
            if(lines.length<2){
                System.out.println("Line error");
                System.out.println(line);
            }
            
            if(stopWhile%10000==0){
                System.out.print("dongwon1: Line: "+(stopWhile-1)+ "\tDocument ID: "+lines[1]+" ");
                System.out.println();
            }

            st = new StringTokenizer(lines[14]);
            if (stopWhile > Ncitations){
                System.out.println("\nThis is the end of Lines.\n"+Ncitations+" of Lines are generated\n\n\n");
                break;
            }

        }
        stopWhile=1;
        while ((line = b2Read.readLine()) != null) {
            if(line.substring(line.length()-1,line.length()).equals("\\")){
                if(tempLine==null){
                    tempLine=line.substring(0,line.length()-1);
                }
                else{
                    tempLine+=line.substring(0,line.length()-1);
                }
                checkLine=false;// A line contains error
            }
            else{
                if (checkLine){
                    bWrite.write(line+"\n");                    
                }
                else{
                    line=tempLine+"fl"+line;
                    checkLine=true;
                    bWrite.write(line+"\n");
                    tempLine=null;
                }
                stopWhile++;
                lines = line.split("\t");
            }
            if(lines.length<2){
                System.out.println("Line error");
                System.out.println(line);
            }
            if(stopWhile%10000==0){
                System.out.print("dongwon2: Line: "+(stopWhile-1)+ "\tDocument ID: "+lines[1]+" ");
                System.out.println();
            }

            st = new StringTokenizer(lines[14]);
            if (stopWhile > Ncitations){
                System.out.println("\nThis is the end of Lines.\n"+Ncitations+" of Lines are generated\n\n\n");
                break;
            }
        }
        
        bWrite.close();
    }
}
