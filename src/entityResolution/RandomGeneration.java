package entityResolution;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Random;

public class RandomGeneration {
	private final static int DIMENSION = 1000; // 随机向量维度
	private final static int RANDOM_VECTORS = 100; // 随机向量个数，签名长度
	private final static int PERMUTATION = 10; // 随机变换个数

	public void main(String[] args) {
		// TODO Auto-generated method stub
		randomVector();
//		randomFactor();
		readTest();
	}

	public void randomVector() {
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
						System.out.print(1);
					} else {
						output.write("0");
						System.out.print(0);
					}
				}
				output.write("\n");
				System.out.println();
			}
			System.out.println();
			System.out.println();
			System.out.println();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("shit");
		} finally {
			try {
				output.flush();
				output.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void randomFactor() {
		BufferedWriter output = null;
		try {

			File f = new File("file/randomFactor.txt");
			output = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(f, false), "UTF-8"));

			Random randoma = new Random(RANDOM_VECTORS);
			boolean[][] randomVector = new boolean[RANDOM_VECTORS][DIMENSION];
			for (int i = 0; i < RANDOM_VECTORS; i++) {

				for (int j = 0; j < DIMENSION; j++) {
					randomVector[i][j] = randoma.nextBoolean();
					if (randomVector[i][j] == true) {

						output.write("1");
						System.out.print(1);
					} else {
						output.write("0");
						System.out.print(0);
					}
				}
				output.write("\n");
				System.out.println();
			}
			System.out.println();
			System.out.println();
			System.out.println();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("shit");
		} finally {
			try {
				output.flush();
				output.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void readTest() {
		BufferedReader br = null;
		try {
			File q = new File("file/randomVector.txt");
			br = new BufferedReader(new InputStreamReader(
					new FileInputStream(q), "UTF-8"));
			String bufferedReadLine;
			boolean[][] randomVector = new boolean[RANDOM_VECTORS][DIMENSION];
			for (int i = 0; i < RANDOM_VECTORS; i++) {
				bufferedReadLine = br.readLine();
				if (bufferedReadLine.length() == DIMENSION) {
					for (int j = 0; j < DIMENSION; j++) {
						randomVector[i][j] = Integer.parseInt(bufferedReadLine
								.charAt(j) + "") == 1 ? true : false;
						System.out.print(randomVector[i][j]);
					}
				} else
					System.out.print("shit");
				System.out.println();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
