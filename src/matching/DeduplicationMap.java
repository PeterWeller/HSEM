package matching;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DeduplicationMap extends Mapper<Object, Text, Text, Text> {

	Text mapKey = new Text();
	Text mapValue = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] segment = line.split("\t");// 原始数据为（实体编号+实体属性向量）

		mapKey.set(segment[0] + "\t" + segment[1]);
		mapValue.set(segment[2] + "\t" + segment[3]);
		context.write(mapKey, mapValue);
	}
}
