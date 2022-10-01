package analysis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ItemTypeStatsValue implements Writable {
	private int unitsSold;
	private double totalProfit;
	
	public ItemTypeStatsValue() {
	}
	
	public ItemTypeStatsValue(final int unitsSold, final double totalProfit) {
		this.unitsSold = unitsSold;
		this.totalProfit = totalProfit;
	}
	
	public int getUnitsSold() {
		return this.unitsSold;
	}
	
	public double getTotalProfit() {
		return this.totalProfit;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.unitsSold = in.readInt();
		this.totalProfit = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.unitsSold);
		out.writeDouble(this.totalProfit);
	}
}
