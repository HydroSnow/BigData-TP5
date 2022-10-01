package analysis;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RegionTotalProfitMap extends Mapper<Object, Text, Text, DoubleWritable> {
	protected void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
		final String[] splitted = value.toString().split(",");
		
		final String regionStr = splitted[0];
		
		// check if region is target
		if (!regionStr.equals(context.getConfiguration().get("targetRegion"))) {
			return;
		}
		
		final String totalProfitStr = splitted[13];
		
		// ignore header line
		if (regionStr.equals("Region") && totalProfitStr.equals("Total Profit")) {
			return;
		}
		
		final Text region = new Text(regionStr);
		final double totalProfitDouble = Double.parseDouble(totalProfitStr);		
		final DoubleWritable totalProfit = new DoubleWritable(totalProfitDouble);
		
		context.write(region, totalProfit);
	}
}
