package analysis;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ItemTypeStatsReduce extends Reducer<Text, ItemTypeStatsValue, Text, Text> {
	protected void reduce(final Text key, final Iterable<ItemTypeStatsValue> values, final Context context) throws IOException, InterruptedException {
		int unitsSold = 0;
		double totalProfit = 0;
		for (final ItemTypeStatsValue value : values) {
			unitsSold = value.getUnitsSold();
			totalProfit = value.getTotalProfit();
		}
		context.write(key, new Text("Units Sold: " + String.format("%d", unitsSold) + ", Total Profit: " + String.format("%.2f", totalProfit)));
	}
}
