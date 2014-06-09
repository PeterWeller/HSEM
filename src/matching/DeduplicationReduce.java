package matching;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DeduplicationReduce extends
		Reducer<Text, Text, Text, FloatWritable> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		if (values.iterator().hasNext()) {
			String[] signature = values.iterator().next().toString()
					.split("\t");
			char[] signature1 = signature[0].toCharArray();
			char[] signature2 = signature[1].toCharArray();
			float hammingDistance = 0.0f;
			for (int i = 0; i < EntityDriver.RANDOM_VECTORS; i++) {
				hammingDistance += signature1[i] == signature2[i] ? 1 : 0;
			}
			FloatWritable outputValue = new FloatWritable();
			outputValue.set(hammingDistance
					/ ((float) EntityDriver.RANDOM_VECTORS));
			context.write(key, outputValue);
		}
	}
}
