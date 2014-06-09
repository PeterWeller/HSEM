package preprocess;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

public class VectorGeneration {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text writeKey = new Text("K");
        private Text writeValue = new Text();

        
        public void map(LongWritable key, Text value,OutputCollector<Text, Text> output, Reporter reporter)throws IOException {

             //字典输入
            String inputPath = "hdfs://linux-rq7e.site:9000/AscSortedDictionary/part-r-00000";

            Configuration conf = new Configuration();
            Path inPath = new Path(inputPath);
            FileSystem hdfs = inPath.getFileSystem(conf);
             
            FSDataInputStream dis = hdfs.open(inPath);
            LineReader in = new LineReader(dis,conf);  
            
            ArrayList<String> tempList = new ArrayList<String>();
            
            Text line = new Text();
          
            while(in.readLine(line) > 0){
                String[] t = line.toString().split("\t");
                tempList.add(t[1]);
            }
            dis.close();
            in.close();

            //token
            String s = value.toString();
            String[] segment = s.split("\t");
            
            writeKey = new Text(segment[0]);
            
            String token = "";
           
            ArrayList<String> recordList = new ArrayList<String>();
            for(int i = 2; i < segment.length; i++ )
                recordList.add(segment[i]);
            
           String tmp = null;
           
           for(int i=0;i<tempList.size();i++){ 

               tmp = tempList.get(i); 
               
               if(recordList.contains(tmp))
                   token += "1";
               else
                   token += "0"; 

           } 
           
            writeValue = new Text(token);
            output.collect(writeKey, writeValue);
        }
    }


    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key,Iterator<Text> values,OutputCollector<Text, Text> output, Reporter reporter)throws IOException {

            while (values.hasNext()) {
                output.collect(key, values.next());
            }
    }
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = new Configuration();
        Job job = new Job(conf,"Token");
        job.setJarByClass(VectorGeneration.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        //job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

//        job.setInputFormat(TextInputFormat.class);
//        job.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        Path output = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, output);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
