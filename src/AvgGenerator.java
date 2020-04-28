import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * @author Shiqi Luo
 *
 */
public class AvgGenerator {
	public static class AvgRatingFillerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input user,movie,rating
			String[] user_movie_rating = value.toString().trim().split(",");
			int userID = Integer.parseInt(user_movie_rating[0]);

			context.write(new IntWritable(userID), new Text(user_movie_rating[1]+":"+user_movie_rating[2]));
		}
	}

	public static class AvgRatingFillerReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			double rateSum = 0;
			int rateCount = 0;
			for(Text str: values){
				String[] movie_rating = str.toString().split(":");
				int movieId = Integer.parseInt(movie_rating[0]);
				double rating = Double.parseDouble(movie_rating[1]);

				rateSum += rating;
				rateCount++;
				context.write(new Text(key.get()+","+movieId+","+rating), null);
			}
			
			if(rateCount != 0){
				double avgRating = rateSum/rateCount;
				context.write(new Text(key.get()+",avg,"+avgRating), null);
			}
		}

	}
	


	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setMapperClass(AvgRatingFillerMapper.class);
		job.setReducerClass(AvgRatingFillerReducer.class);

		job.setJarByClass(AvgGenerator.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		// job.setGroupingComparatorClass(GroupComparator.class);
		// job.setSortComparatorClass(KeyComparator.class);
		
		job.setNumReduceTasks(1);
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
