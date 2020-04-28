import java.io.IOException;
import java.util.HashSet;

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
public class DataReduceFilter {
	public static class FilterMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input user,movie,rating
			String[] user_movie_rating = value.toString().trim().split(",");
			int userID = Integer.parseInt(user_movie_rating[0]);

			context.write(new IntWritable(userID), new Text(user_movie_rating[0] + "," + user_movie_rating[1] + "," + user_movie_rating[2]));
		}
	}

	public static class FilterReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
		
		HashSet<Integer> userIDs = null;
		HashSet<Integer> movies = null;
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void setup(Reducer<IntWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			
			userIDs = new HashSet<>();
			movies = new HashSet<>();
		}

		// reduce method
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			if(userIDs.size() <= 10000){
				//System.out.println(key.get() + " : " + userIDs.size());
				userIDs.add(key.get());
				double rateSum = 0;
				int rateCount = 0;
				for(Text text: values){
					String[] movie_rating = text.toString().split(",");
					int movieID = Integer.parseInt(movie_rating[1]);
					double rating = Double.parseDouble(movie_rating[2]);
					
					if(!this.movies.contains(movieID)){
						this.movies.add(movieID);
						System.out.println(movieID);
					}
					
					rateSum += rating;
					rateCount++;
					context.write(text, null);
				}

				if(rateCount != 0){
					double avgRating = rateSum/rateCount;
					context.write(new Text(key.get()+",avg,"+avgRating), null);
				}
			}
		}

	}
	


	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setMapperClass(FilterMapper.class);
		job.setReducerClass(FilterReducer.class);

		job.setJarByClass(DataReduceFilter.class);

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
