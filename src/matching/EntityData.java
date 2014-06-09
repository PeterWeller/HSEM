package matching;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class EntityData implements WritableComparable<EntityData> {

	private IntWritable entityNum;
	private boolean[] signature = new boolean[EntityDriver.RANDOM_VECTORS];

	public EntityData() {
		set(new IntWritable(), new boolean[EntityDriver.RANDOM_VECTORS]);
	}

	public void set(IntWritable intWritable, boolean[] bs) {
		this.entityNum = intWritable;
		for (int i = 0; i < EntityDriver.RANDOM_VECTORS; i++)
			signature[i] = bs[i];
	}

	public IntWritable getEntityNum() {
		return entityNum;
	}

	public String getSignature() {
		String sigString = "";
		for (int i = 0; i < EntityDriver.RANDOM_VECTORS; i++)
			sigString += signature[i] == true ? "1" : "0";
		return sigString;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		entityNum.set(in.readInt());
		for (int i = 0; i < signature.length; i++)
			signature[i] = in.readBoolean();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(entityNum.get());
		for (int i = 0; i < signature.length; i++)
			out.writeBoolean(signature[i]);
	}

	@Override
	public int compareTo(EntityData that) {
		return this.signature.toString().compareTo(that.signature.toString());
	}
}