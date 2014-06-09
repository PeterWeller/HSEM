package matching;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EntityMap extends Mapper<Object, Text, IntWritable, EntityData> {

	private static boolean[][] randomVector = new boolean[EntityDriver.RANDOM_VECTORS][EntityDriver.DIMENSION];

	public void setup(Context context) throws IOException {

		// 读取随机向量信息
		Path[] caches = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());
		if (caches == null || caches.length <= 0) {
			System.err.println("Random vector is missing!");
			System.exit(1);
		}
		BufferedReader br = null;
		br = new BufferedReader(new InputStreamReader(new FileInputStream(
				caches[0].toString()), "UTF-8"));
		String bufferedReadLine;
		for (int i = 0; i < EntityDriver.RANDOM_VECTORS; i++) {
			bufferedReadLine = br.readLine();
			if (bufferedReadLine.length() == EntityDriver.DIMENSION) {
				for (int j = 0; j < EntityDriver.DIMENSION; j++) {
					randomVector[i][j] = Integer.parseInt(bufferedReadLine
							.charAt(j) + "") == 1 ? true : false;
				}
			}
		}
		try {
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] segment = line.split("\t");// 原始数据为（实体编号+实体属性向量）

		// 读取实体属性向量
		char[] entityFeature = segment[1].toCharArray();
		if (entityFeature.length != EntityDriver.DIMENSION) {
			System.err.println("卧槽这数据是个坑");
		}
		boolean[] boolFeature = new boolean[EntityDriver.DIMENSION];
		for (int i = 0; i < EntityDriver.DIMENSION; i++) {
			boolFeature[i] = entityFeature[i] == '1';
		}

		// 生成签名
		boolean[] signature = new boolean[EntityDriver.RANDOM_VECTORS];
		int count = 0;
		for (int i = 0; i < EntityDriver.RANDOM_VECTORS; i++) {
			count = 0;
			for (int j = 0; j < EntityDriver.DIMENSION; j++) {
				count += boolFeature[j] == randomVector[i][j] ? 1 : -1;
			}
			signature[i] = count >= 0 ? true : false;
		}

		// 随机变换

		Random random = new Random(20);
		int a, b, newPosition;
		for (int i = 0; i < EntityDriver.PERMUTATION; i++) {

			EntityData outputValue = new EntityData();
			boolean[] permutedSignature = new boolean[EntityDriver.DIMENSION];
			a = random.nextInt(EntityDriver.RANDOM_VECTORS - 2) + 1;
			b = random.nextInt(EntityDriver.RANDOM_VECTORS - 2) + 1;
			for (int j = 0; j < EntityDriver.RANDOM_VECTORS; j++) {
				newPosition = (a * j + b) % EntityDriver.RANDOM_VECTORS;
				permutedSignature[newPosition] = signature[j];
			}
			outputValue.set(new IntWritable(Integer.parseInt(segment[0])),
					permutedSignature);
			context.write(new IntWritable(i), outputValue);
		}
	}
}