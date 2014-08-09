package matching;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DeduplicationMap extends
		Mapper<IntWritable, EntityPairData, Text, EntityPairData> {

	Text mapKey = new Text();
	public void map(IntWritable key, EntityPairData value, Context context)
			throws IOException, InterruptedException {
		mapKey.set(value.getFirstEntityNum().toString() + " "
				+ value.getSecondEntityNum().toString());
		context.write(mapKey, value);
	}
}
