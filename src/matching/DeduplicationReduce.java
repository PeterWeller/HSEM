package matching;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DeduplicationReduce extends
		Reducer<Text, EntityPairData, Text, FloatWritable> {

	public void reduce(Text key, Iterable<EntityPairData> values, Context context)
			throws IOException, InterruptedException {
		if (values.iterator().hasNext()) {
			EntityPairData signatures = values.iterator().next();
			char[] signature1 = signatures.getFirstSig().toCharArray();
			char[] signature2 = signatures.getSecondSig().toCharArray();
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
