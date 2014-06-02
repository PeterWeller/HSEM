package entityResolution;

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

public class EntityMap extends Mapper<Object, Text, IntWritable, Text> {

	private Text word = new Text();
	private final static int DIMENSION = 1000; // 随机向量维度
	private final static int RANDOM_VECTORS = 37; // 随机向量个数，签名长度
	private final static int PERMUTATION = 10; // 随机变换个数
	boolean[][] randomVector = new boolean[RANDOM_VECTORS][DIMENSION];

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
		for (int i = 0; i < RANDOM_VECTORS; i++) {
			bufferedReadLine = br.readLine();
			if (bufferedReadLine.length() == DIMENSION) {
				for (int j = 0; j < DIMENSION; j++) {
					randomVector[i][j] = Integer.parseInt(bufferedReadLine
							.charAt(j) + "") == 1 ? true : false;
				}
			}
		}
		try {
			br.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] segment = line.split("\t");// 原始数据为（实体编号+实体属性向量）

		// 读取实体属性向量
		char[] entityFeature = segment[1].toCharArray();
		if (entityFeature.length != DIMENSION)
			System.out.println("卧槽这数据是个坑");
		boolean[] boolFeature = new boolean[DIMENSION];
		for (int i = 0; i < DIMENSION; i++) {
			boolFeature[i] = entityFeature[i] == '1';
		}

		// 生成签名
		boolean[] signature = new boolean[DIMENSION];
		int count = 0;
		for (int i = 0; i < RANDOM_VECTORS; i++) {
			count = 0;
			for (int j = 0; j < DIMENSION; j++) {
				count += boolFeature[j] == randomVector[i][j] ? 1 : -1;
			}
			signature[i] = count >= 0 ? true : false;
		}

		// 随机变换

		Random random = new Random(20);
		int a, b, newPosition;
		IntWritable k = new IntWritable();
		boolean[] permutedSignature = new boolean[DIMENSION];
		String perSig = new String();
		for (int i = 0; i < PERMUTATION; i++) {
			k.set(i);
			a = random.nextInt(RANDOM_VECTORS - 2)+1;
			b = random.nextInt(RANDOM_VECTORS - 2)+1;
			for (int j = 0; j < RANDOM_VECTORS; j++) {
				newPosition = (a * j + b) % RANDOM_VECTORS;
				permutedSignature[newPosition] = signature[j];
			}
			perSig = "";
			for(int j=0;j < RANDOM_VECTORS; j++){
				perSig+=permutedSignature[j]==true?"1":"0";
			}
			word.set(segment[0] + "\t"+perSig);
			context.write(k, word);
		}
	}
}