import java.awt.event.ItemEvent;
import java.io.IOException;
import java.io.StringReader;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.opencsv.CSVReader;

public class TopMovieFilter {

	public static class UserMovieRatingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: userid,movieid,rating
			String[] user_movie_rating = value.toString().trim().split(",");
			int userID = Integer.parseInt(user_movie_rating[0]);
			int movieID = Integer.parseInt(user_movie_rating[1]);
			double rating = Double.parseDouble(user_movie_rating[2]);
			
			context.write(new IntWritable(movieID), new Text(userID+","+movieID+","+rating));
		}
	}

	public static class MovieMetadataMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		private int count = 0;
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void setup(Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			count = 0;
		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//input: ...movieid,...,vote_average,...
			CSVReader reader = new CSVReader(new StringReader(value.toString()));
			
			String[] line = null;
			try {
				line = reader.readNext();
				if(line[5] != null && line[22] != null && line[23] != null){
					double score = Double.parseDouble(line[2]) * Integer.parseInt(line[23]);
					context.write(new IntWritable(-100), new Text(line[5]+":"+score));
				}
			} catch (Exception e) {
				// System.out.println(++count);
				// e.printStackTrace();
			}
		}
	}

	public static class TopMovieRatingReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

		class Pair{
			int movieID;
			double voteAvg;

			Pair(String movieID, String avg){
				this.movieID = Integer.parseInt(movieID);
				this.voteAvg = Double.parseDouble(avg);
			}
			
			public int getMovieID(){
				return movieID;
			}
			
			public double getAvgVote(){
				return voteAvg;
			}
		}

		private PriorityBlockingQueue<Pair> map = null;
		private HashSet<Integer> topMovies = null;

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void setup(Reducer<IntWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			
			map = new PriorityBlockingQueue<Pair>(1000, new Comparator<Pair>(){

				@Override
				public int compare(Pair o1, Pair o2) {
					if(o1.getAvgVote() > o2.getAvgVote()){
						return 1;
					}
					else if(o1.getAvgVote() < o2.getAvgVote()){
						return -1;
					}
					else{
						return 0;
					}
				}
			});
			
			this.topMovies = new HashSet<>();
		}

		// reduce method
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			//key = -100
			//value = movieid:vote_avg
			
			//key = movieid
			//value = userid,movieid,rating
			
			if(key.get() == -100){
				for(Text str: values) {
					String[] line = str.toString().trim().split(":");
					this.map.add(new Pair(line[0], line[1]));
					if(this.map.size()>1000){
						this.map.poll();
					}
				}
				
				this.map.forEach(ItemEvent -> {
					System.out.println(ItemEvent.getMovieID());
					topMovies.add(ItemEvent.getMovieID());
				});
			}
			else{
				System.out.println("->: " + key.get());
				if(topMovies.contains(key.get())){
					for(Text str: values){
						context.write(str, null);
					}
					
				}
			}
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(TopMovieFilter.class);

		ChainMapper.addMapper(job, UserMovieRatingMapper.class, LongWritable.class, Text.class, IntWritable.class, Text.class, conf);
		ChainMapper.addMapper(job, MovieMetadataMapper.class, IntWritable.class, Text.class, IntWritable.class, Text.class, conf);

		job.setMapperClass(UserMovieRatingMapper.class);
		job.setMapperClass(MovieMetadataMapper.class);

		job.setReducerClass(TopMovieRatingReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserMovieRatingMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieMetadataMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}
}
