package matching;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class EntityPairData implements WritableComparable<EntityPairData> {

	private IntWritable firstEntityNum = new IntWritable();
	private IntWritable secondEntityNum = new IntWritable();
	private boolean[] firstSignature = new boolean[EntityDriver.RANDOM_VECTORS];
	private boolean[] secondSignature = new boolean[EntityDriver.RANDOM_VECTORS];

	public EntityPairData() {
		int fen = 0;
		int sen = 0;
		set(fen, sen,
				new boolean[EntityDriver.RANDOM_VECTORS],
				new boolean[EntityDriver.RANDOM_VECTORS]);
	}

	public void set(int fen, int sen, boolean[] fs, boolean[] ss) {
		firstEntityNum.set(fen);
		secondEntityNum.set(sen);
		for (int i = 0; i < EntityDriver.RANDOM_VECTORS; i++) {
			firstSignature[i] = fs[i];
			secondSignature[i] = ss[i];
		}
	}

	public IntWritable getFirstEntityNum() {
		return firstEntityNum;
	}

	public IntWritable getSecondEntityNum() {
		return secondEntityNum;
	}
	
	public String getFirstSig() {
		String sigString = "";
		for (int i = 0; i < EntityDriver.RANDOM_VECTORS; i++)
			sigString += firstSignature[i] == true ? "1" : "0";
		return sigString;
	}

	public String getSecondSig() {
		String sigString = "";
		for (int i = 0; i < EntityDriver.RANDOM_VECTORS; i++)
			sigString += secondSignature[i] == true ? "1" : "0";
		return sigString;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		firstEntityNum.set(in.readInt());
		secondEntityNum.set(in.readInt());
		for (int i = 0; i < firstSignature.length; i++)
			firstSignature[i] = in.readBoolean();
		for (int i = 0; i < secondSignature.length; i++)
			secondSignature[i] = in.readBoolean();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(firstEntityNum.get());
		out.writeInt(secondEntityNum.get());
		for (int i = 0; i < firstSignature.length; i++)
			out.writeBoolean(firstSignature[i]);
		for (int i = 0; i < secondSignature.length; i++)
			out.writeBoolean(secondSignature[i]);
	}

	@Override
	public int compareTo(EntityPairData that) {
		return this.getFirstSig().compareTo(that.getFirstSig());
	}
}