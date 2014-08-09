package matching;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class EntityReduce extends
		Reducer<IntWritable, EntityData, IntWritable, EntityPairData> {

	@SuppressWarnings("unchecked")
	public static Map.Entry[] getSortedHashtableByValue(Map h) {
		Set set = h.entrySet();
		Map.Entry[] entries = (Map.Entry[]) set.toArray(new Map.Entry[set
				.size()]);
		Arrays.sort(entries, new Comparator() {
			public int compare(Object arg0, Object arg1) {
				String key1 = String.valueOf(((Map.Entry) arg0).getValue()
						.toString());
				String key2 = String.valueOf(((Map.Entry) arg1).getValue()
						.toString());
				return key2.compareTo(key1);
			}
		});
		return entries;
	}

	public void reduce(IntWritable key, Iterable<EntityData> values,
			Context context) throws IOException, InterruptedException {

		IntWritable outputKey = new IntWritable();
		EntityPairData outputValue = new EntityPairData();
		// FloatWritable outputValue = new FloatWritable();
		Map<Integer, String> m = new HashMap<Integer, String>();
		for (EntityData val : values) {
			m.put(val.getEntityNum().get(), val.getSignature());
		}
		Map.Entry<Integer, String>[] entries = getSortedHashtableByValue(m);

		// float hammingDistance = 0.0f;
		for (int i = 0; i < entries.length; i++) {
			for (int j = 1; j <= EntityDriver.WINDOW
					&& (i + j) < entries.length; j++) {
				boolean[] firstSig = new boolean[EntityDriver.RANDOM_VECTORS];
				boolean[] secondSig = new boolean[EntityDriver.RANDOM_VECTORS];
				for (int b = 0; b < EntityDriver.RANDOM_VECTORS; b++) {
					firstSig[b] = entries[i].getValue().charAt(b) == '1' ? true
							: false;
					secondSig[b] = entries[i + j].getValue().charAt(b) == '1' ? true
							: false;
				}
				if (entries[i].getKey() < entries[i + j].getKey()) {
					outputValue.set(entries[i].getKey(),
							entries[i + j].getKey(), firstSig, secondSig);
				} else {
					outputValue.set(entries[i + j].getKey(),
							entries[i].getKey(), secondSig, firstSig);
				}
				// hammingDistance = 0.0f;
				// for (int k = 0; k < EntityDriver.RANDOM_VECTORS; k++) {
				// hammingDistance += entries[i].getValue().toCharArray()[k] ==
				// entries[i
				// + j].getValue().toCharArray()[k] ? 1 : 0;
				// }
				//
				// outputValue.set(hammingDistance
				// / ((float) EntityDriver.RANDOM_VECTORS));
				outputKey = outputValue.getFirstEntityNum();
				context.write(outputKey, outputValue);
			}
		}
	}
}