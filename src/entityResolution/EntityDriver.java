package entityResolution;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class EntityDriver {
	private final static String inputPath = "hdfs://10.11.1.51:9000/home/input/";
	private final static String outputPath = "hdfs://10.11.1.51:9000/home/output/";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		// 分布式缓存文件：随机向量列表
		Path randomVectorFile = new Path(
				"hdfs://10.11.1.51:9000/home/input/randomVector.txt");
		DistributedCache.addCacheFile(randomVectorFile.toUri(), conf);

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 0) {
			System.err.println("Usage: No arguments required");
			System.exit(2);
		}

		Job job = new Job(conf, "Distributed ER");
		job.setJarByClass(EntityDriver.class);

		job.setMapperClass(EntityMap.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(EntityReduce.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}