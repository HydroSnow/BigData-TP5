package analysis;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountryTotalProfitMap extends Mapper<Object, Text, Text, DoubleWritable> {
	protected void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
		final String[] splitted = value.toString().split(",");
		
		final String countryStr = splitted[1];
		
		// check if country is target
		if (!countryStr.equals(context.getConfiguration().get("targetCountry"))) {
			return;
		}
		
		final String totalProfitStr = splitted[13];
		
		// ignore header line
		if (countryStr.equals("Country") && totalProfitStr.equals("Total Profit")) {
			return;
		}
		
		final Text country = new Text(countryStr);
		final double totalProfitDouble = Double.parseDouble(totalProfitStr);		
		final DoubleWritable totalProfit = new DoubleWritable(totalProfitDouble);
		
		context.write(country, totalProfit);
	}
}
