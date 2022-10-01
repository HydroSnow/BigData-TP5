package analysis;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ItemTypeTotalProfitMap extends Mapper<Object, Text, Text, DoubleWritable> {
	protected void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
		final String[] splitted = value.toString().split(",");
		
		final String itemTypeStr = splitted[2];
		
		// check if country is target
		if (!itemTypeStr.equals(context.getConfiguration().get("targetItemType"))) {
			return;
		}
		
		final String totalProfitStr = splitted[13];
		
		// ignore header line
		if (itemTypeStr.equals("Item Type") && totalProfitStr.equals("Total Profit")) {
			return;
		}
		
		final Text itemType = new Text(itemTypeStr);
		final double totalProfitDouble = Double.parseDouble(totalProfitStr);		
		final DoubleWritable totalProfit = new DoubleWritable(totalProfitDouble);
		
		context.write(itemType, totalProfit);
	}
}
