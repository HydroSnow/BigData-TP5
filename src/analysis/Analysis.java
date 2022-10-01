package analysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Analysis {

	public static void main(String[] allArgs) throws IOException, InterruptedException, ClassNotFoundException {
		final Configuration config = new Configuration();
		final String[] args = new GenericOptionsParser(config, allArgs).getRemainingArgs();

		if (args.length == 0) {
			System.out.println("Usage: command <regionTotalProfit, countryTotalProfit, itemTypeTotalProfit, itemTypeStats> ...");
			return;
		}

		final String subcommand = args[0];

		if (subcommand.equals("regionTotalProfit")) {
			if (args.length != 4) {
				System.out.println("Usage: command regionTotalProfit <region> <input file> <output directory>");
				return;
			}

			final String region = args[1];
			final String inputFile = args[2];
			final String outputFile = args[3];
			
			config.set("targetRegion", region);
			
			final Job job = Job.getInstance(config, "Analysis - Region Total Profit");

			job.setJarByClass(Analysis.class);
			job.setMapperClass(RegionTotalProfitMap.class);
			job.setReducerClass(TotalProfitReduce.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);

			FileInputFormat.addInputPath(job, new Path(inputFile));
			FileOutputFormat.setOutputPath(job, new Path(outputFile));

			if (job.waitForCompletion(true)) {
				System.exit(0);
			}

			System.exit(-1);
		}

		if (subcommand.equals("countryTotalProfit")) {
			if (args.length != 4) {
				System.out.println("Usage: command countryTotalProfit <country> <input file> <output directory>");
				return;
			}

			final String country = args[1];
			final String inputFile = args[2];
			final String outputFile = args[3];
			
			config.set("targetCountry", country);
			
			final Job job = Job.getInstance(config, "Analysis - Country Total Profit");

			job.setJarByClass(Analysis.class);
			job.setMapperClass(CountryTotalProfitMap.class);
			job.setReducerClass(TotalProfitReduce.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);

			FileInputFormat.addInputPath(job, new Path(inputFile));
			FileOutputFormat.setOutputPath(job, new Path(outputFile));

			if (job.waitForCompletion(true)) {
				System.exit(0);
			}

			System.exit(-1);
		}

		if (subcommand.equals("itemTypeTotalProfit")) {
			if (args.length != 4) {
				System.out.println("Usage: command itemTypeTotalProfit <item type> <input file> <output directory>");
				return;
			}

			final String itemType = args[1];
			final String inputFile = args[2];
			final String outputFile = args[3];
			
			config.set("targetItemType", itemType);
			
			final Job job = Job.getInstance(config, "Analysis - Item Type Total Profit");

			job.setJarByClass(Analysis.class);
			job.setMapperClass(ItemTypeTotalProfitMap.class);
			job.setReducerClass(TotalProfitReduce.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);

			FileInputFormat.addInputPath(job, new Path(inputFile));
			FileOutputFormat.setOutputPath(job, new Path(outputFile));

			if (job.waitForCompletion(true)) {
				System.exit(0);
			}

			System.exit(-1);
		}

		if (subcommand.equals("itemTypeStats")) {
			if (args.length != 3) {
				System.out.println("Usage: command itemTypeStats <input file> <output directory>");
				return;
			}

			final String inputFile = args[1];
			final String outputFile = args[2];
			
			final Job job = Job.getInstance(config, "Analysis - Item Type Stats");

			job.setJarByClass(Analysis.class);
			job.setMapperClass(ItemTypeStatsMap.class);
			job.setReducerClass(ItemTypeStatsReduce.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(ItemTypeStatsValue.class);

			FileInputFormat.addInputPath(job, new Path(inputFile));
			FileOutputFormat.setOutputPath(job, new Path(outputFile));

			if (job.waitForCompletion(true)) {
				System.exit(0);
			}

			System.exit(-1);
		}

		System.out.println("Unknown subcommand: " + subcommand);
		return;
	}

}
