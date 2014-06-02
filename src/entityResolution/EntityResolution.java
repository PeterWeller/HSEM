package entityResolution;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.file.tfile.ByteArray;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class EntityResolution {
	public static class SignatureMapper extends
			Mapper<Object, Text, Text, Text> {

		private Text word = new Text();
		private final static int DIMENSION = 1000; // 随机向量维度
		private final static int RANDOM_VECTORS = 100; // 随机向量个数，签名长度
		private final static int PERMUTATION = 10; // 随机变换个数

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] segment = line.split("\t");
			
			char[] entityFeature = segment[1].toCharArray();
			if (entityFeature.length != DIMENSION)
				System.out.println("卧槽这数据是个坑");
			boolean[] boolFeature = new boolean[DIMENSION];
			for (int i = 0; i < DIMENSION; i++) {
				boolFeature[i] = entityFeature[i] == '1';
			}
//			boolean[] originSignature = sigGen(boolFeature, randomVector);

			// for (int i = 0; i < segment.length; i++) {
			// word.set(segment[i]);
			// context.write(word, one);
			// }
		}

		private boolean[] sigGen(boolean[] feature, boolean[][] randomVector) {
			boolean[] result = new boolean[10];
			return result;
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	// �������
	private static class IntWritableDecreasingComparator extends
			IntWritable.Comparator {
		public int compare(WritableComparable a, WritableComparable b) {
			return -super.compare(a, b);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}
	}

	// �������
	private static class IntWritableIncreasingComparator extends
			IntWritable.Comparator {
		public int compare(WritableComparable a, WritableComparable b) {
			return super.compare(a, b);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return super.compare(b1, s1, l1, b2, s2, l2);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Path tempDir = new Path("wordcount-temp-"
				+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE))); // ����һ����ʱĿ¼

		Job job = new Job(conf, "word count");
		job.setJarByClass(EntityResolution.class);
		try {
			job.setMapperClass(SignatureMapper.class);
			job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(IntSumReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, tempDir);// �Ƚ���Ƶͳ�������������д����ʱĿ
															// ¼��,
															// ��һ��������������ʱĿ¼Ϊ����Ŀ¼��
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			if (job.waitForCompletion(true)) {
				Job sortJob = new Job(conf, "sort");
				sortJob.setJarByClass(EntityResolution.class);

				FileInputFormat.addInputPath(sortJob, tempDir);
				sortJob.setInputFormatClass(SequenceFileInputFormat.class);

				/*
				 * InverseMapper��hadoop���ṩ��������ʵ��map()֮�����ݶԵ�key��value��
				 * ��
				 */
				sortJob.setMapperClass(InverseMapper.class);
				/* �� Reducer �ĸ����޶�Ϊ1, ��������Ľ���ļ�����һ���� */
				sortJob.setNumReduceTasks(1);
				FileOutputFormat.setOutputPath(sortJob, new Path(otherArgs[1]));

				sortJob.setOutputKeyClass(IntWritable.class);
				sortJob.setOutputValueClass(Text.class);
				/*
				 * Hadoop Ĭ�϶� IntWritable ���������򣬶�������Ҫ���ǰ��������С�
				 * �������ʵ����һ�� IntWritableDecreasingComparator ��,��
				 * ��ָ��ʹ������Զ���� Comparator ����������е� key (��Ƶ)��������
				 */
				sortJob.setSortComparatorClass(IntWritableIncreasingComparator.class);

				System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
			}
		} finally {
			FileSystem.get(conf).deleteOnExit(tempDir);
		}
	}
}