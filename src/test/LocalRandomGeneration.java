package test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;

public class LocalRandomGeneration {
	private final static int DIMENSION = 3871; // 随机向量维度
	private final static int RANDOM_VECTORS = 101; // 随机向量个数，签名长度

	public static void main(String[] args) {
		randomVector();
	}

	public static void randomVector() {
		BufferedWriter output = null;
		try {

			File f = new File("file/randomVector.txt");
			output = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(f, false), "UTF-8"));

			Random random = new Random();
			boolean[][] randomVector = new boolean[RANDOM_VECTORS][DIMENSION];
			for (int i = 0; i < RANDOM_VECTORS; i++) {

				for (int j = 0; j < DIMENSION; j++) {
					randomVector[i][j] = random.nextBoolean();
					if (randomVector[i][j] == true) {

						output.write("1");
					} else {
						output.write("0");
					}
				}
				output.write("\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("shit");
		} finally {
			try {
				output.flush();
				output.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}