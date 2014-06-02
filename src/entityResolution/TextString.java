package entityResolution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TextString implements WritableComparable<TextString> {
	private String text;
	private String value;

	public TextString() {
	}

	public TextString(String text, String value) {
		this.text = text;
		this.value = value;
	}

	public String getFirst() {
		return this.text;
	}

	public String getSecond() {
		return this.value;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		text = in.readUTF();
		value = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(text);
		out.writeUTF(value);
	}

	@Override
	public int compareTo(TextString that) {
		return this.text.compareTo(that.text);
	}
}
