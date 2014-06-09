package matching;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

public class RandomGeneration {
	
	public static void randomVector(Configuration conf) {

		FSDataOutputStream output = null;
		BufferedWriter outputBR = null;
		try {
			FileSystem hdfs = FileSystem.get(conf);
			output = hdfs.create(EntityDriver.randomVectorPath, true);
			outputBR = new BufferedWriter(new OutputStreamWriter(output,"UTF-8"));

			Random random = new Random();
			boolean[][] randomVector = new boolean[EntityDriver.RANDOM_VECTORS][EntityDriver.DIMENSION];
			for (int i = 0; i < EntityDriver.RANDOM_VECTORS; i++) {

				for (int j = 0; j < EntityDriver.DIMENSION; j++) {
					randomVector[i][j] = random.nextBoolean();
					if (randomVector[i][j] == true) {

						outputBR.write("1");
					} else {
						outputBR.write("0");
					}
				}
				outputBR.write("\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				outputBR.flush();
				outputBR.close();
				output.flush();
				output.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
