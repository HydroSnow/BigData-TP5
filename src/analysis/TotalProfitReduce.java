package analysis;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalProfitReduce extends Reducer<Text, DoubleWritable, Text, Text> {
	protected void reduce(final Text key, final Iterable<DoubleWritable> values, final Context context) throws IOException, InterruptedException {
		double sum = 0;
		for (final DoubleWritable value : values) {
			sum += value.get();
		}
		context.write(key, new Text(String.format("%.2f", sum)));
	}
}
