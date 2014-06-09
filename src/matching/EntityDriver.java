package matching;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class EntityDriver {

	public final static int DIMENSION = 3871; // 随机向量维度
	public final static int RANDOM_VECTORS = 101; // 随机向量个数，签名长度
	public final static int PERMUTATION = 11; // 随机变换个数
	public final static int WINDOW = 10; // 滑动窗口

	private final static Path inputFolder = new Path(
			"hdfs://linux-rq7e.site:9000/token4Test/");
	private final static Path outputPath = new Path(
			"hdfs://linux-rq7e.site:9000/entityOutput/");
	private final static Path inputFolder2 = new Path(
			"hdfs://linux-rq7e.site:9000/entityOutput/");
	private final static Path outputPath2 = new Path(
			"hdfs://linux-rq7e.site:9000/entityResult" + "_"
					+ String.valueOf(RANDOM_VECTORS) + "_"
					+ String.valueOf(PERMUTATION) + "_"
					+ String.valueOf(WINDOW) + "/");
	public final static Path randomVectorPath = new Path(
			"hdfs://linux-rq7e.site:9000/entityInput/randomVector.txt");

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		RandomGeneration.randomVector(conf);

		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(outputPath, true);
		hdfs.delete(outputPath2, true);

		// 分布式缓存文件：随机向量列表
		DistributedCache.addCacheFile(randomVectorPath.toUri(), conf);

		FileStatus stats[] = hdfs.listStatus(inputFolder);

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 0) {
			System.err.println("Usage: 神马都没有！");
			System.exit(2);
		}
		Job job = new Job(conf, "Distributed ER");
		job.setJarByClass(EntityDriver.class);

		job.setMapperClass(EntityMap.class);
		job.setReducerClass(EntityReduce.class);

		if (PERMUTATION >= 11)
			job.setNumReduceTasks(11);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(EntityData.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		for (int i = 0; i < stats.length; i++) {
			if (!stats[i].isDir())
				FileInputFormat.addInputPath(job, stats[i].getPath());
		}

		FileOutputFormat.setOutputPath(job, outputPath);

		if (job.waitForCompletion(true)) {
			Job secondJob = new Job(conf, "Deduplication");
			secondJob.setJarByClass(EntityDriver.class);

			secondJob.setMapperClass(DeduplicationMap.class);
			secondJob.setReducerClass(DeduplicationReduce.class);

			secondJob.setMapOutputKeyClass(Text.class);
			secondJob.setMapOutputValueClass(Text.class);

			secondJob.setOutputKeyClass(Text.class);
			secondJob.setOutputValueClass(FloatWritable.class);

			FileStatus stats2[] = hdfs.listStatus(inputFolder2);
			for (int i = 0; i < stats2.length; i++) {
				if (!stats2[i].isDir())
					FileInputFormat
							.addInputPath(secondJob, stats2[i].getPath());
			}

			FileOutputFormat.setOutputPath(secondJob, outputPath2);
			System.exit(secondJob.waitForCompletion(true) ? 0 : 1);
		}
	}
}