import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class statistics {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text values, Context context)
				throws IOException, InterruptedException {
			String strValue = values.toString();
			double partialMin = Double.MAX_VALUE;
			double partialMax = Double.MIN_VALUE;
			double element = 0, partialCount = 0, partialSum = 0, partialSumOfSquares = 0;
			String[] lines = strValue.split("\n");
			for (int i = 0; i < lines.length; i++) {
				// System.out.println(i+": "+lines[i]);
				element = Double.parseDouble(lines[i]);

				// Computation of partial values
				if (element < partialMin) {
					partialMin = element;
				}
				if (element > partialMax) {
					partialMax = element;
				}
				partialCount++;
				partialSum += element;
				partialSumOfSquares += Math.pow(element, 2);
			}
			
			context.write(new Text("mapperKey"), new Text(partialSum + "\t"
					+ partialSumOfSquares + "\t" + partialMin + "\t"
					+ partialMax + "\t" + partialCount));
			
			System.out.println(partialSum + "\t"
					+ partialSumOfSquares + "\t" + partialMin + "\t"
					+ partialMax + "\t" + partialCount);
		}
	}

	public static class Reduce extends
			Reducer<Text, Text, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int i = 0;
			double min = Double.MAX_VALUE;
			double max = Double.MIN_VALUE;
			double[] totalValues = new double[5];
			double sum = 0, sumOfSquares = 0, count = 0, mean = 0;

			Iterator<Text> iterator = values.iterator();
			while (iterator.hasNext()) {
				Text value = iterator.next();
				String strValue = value.toString();
				StringTokenizer itr = new StringTokenizer(strValue, "\t"); // line to string token
				i = 0;
				while (itr.hasMoreTokens()) {
					String str2 = itr.nextToken();
					totalValues[i] = Double.parseDouble(str2);
					i++;
				}
				sum += totalValues[0];
				count += totalValues[4];
				sumOfSquares += totalValues[1];
				if (min > totalValues[2])
					min = totalValues[2];
				if (max < totalValues[3])
					max = totalValues[3];
			}

			System.out.println("totalSum: " + sum + " totalSumOfSquare: "
					+ sumOfSquares + " totalMin: " + min + " totalMax: " + max
					+ " totalCount: " + count);

			mean = sum / count;
			double sd = Math.sqrt((sumOfSquares / count)
					- (Math.pow((sum / count), 2)));

			context.write(new Text("Max"), new DoubleWritable(max));
			context.write(new Text("Min"), new DoubleWritable(min));
			context.write(new Text("Mean"),
					new DoubleWritable(Math.round(mean * 100d) / 100d));
			context.write(new Text("Standard Deviation"), new DoubleWritable(
					Math.round(sd * 10000d) / 10000d));
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs(); // get all args
		if (otherArgs.length < 2) {
			System.err.println("Usage: WordCount <in> <out>");
			System.exit(2);
		}
		
		int numberOfLines = 100;
		if (otherArgs.length == 3) numberOfLines = Integer.parseInt(otherArgs[2]);
		
		conf.setInt("numberOfLinesToProcess", numberOfLines);
		// conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator","--");
		// conf.set("textinputformat.record.delimiter", "--");
		// create a job with name "wordcount"
		Job job = new Job(conf, "statistics");
		job.setJarByClass(statistics.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// Add a combiner here, not required to successfully run the wordcount
		// program

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		/*
		 * job.setInputKeyClass(Text.class); job.setInputValueClass(Text.class);
		 */

		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(DoubleWritable.class);
		job.setInputFormatClass(multipleLineInputFormat.class);

		// set the HDFS path of the input data
		// FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		NLineInputFormat.addInputPath(job, new Path(otherArgs[0]));

		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}