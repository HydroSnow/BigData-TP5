package analysis;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ItemTypeStatsMap extends Mapper<Object, Text, Text, ItemTypeStatsValue> {
	protected void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
		final String[] splitted = value.toString().split(",");
		
		final String itemTypeStr = splitted[2];
		final String salesChannelStr = splitted[3];
		final String unitsSoldStr = splitted[8];
		final String totalProfitStr = splitted[13];
		
		// ignore header line
		if (itemTypeStr.equals("Item Type") && salesChannelStr.equals("Sales Channel") && unitsSoldStr.equals("Units Sold") && totalProfitStr.equals("Total Profit")) {
			return;
		}
		
		final int unitsSold = Integer.parseInt(unitsSoldStr);
		final double totalProfit = Double.parseDouble(totalProfitStr);
		
		final Text outputKey = new Text(itemTypeStr + ", " + salesChannelStr);
		final ItemTypeStatsValue outputValue = new ItemTypeStatsValue(unitsSold, totalProfit);

		context.write(outputKey, outputValue);
	}
}
