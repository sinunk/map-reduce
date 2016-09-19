package imp1;
import imp1.wordpair1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.io.DoubleWritable;

public class relativefreq1 {
	public static class PairsRelativeOccurrenceMapper extends Mapper<LongWritable, Text, wordpair1, IntWritable> {
	    private wordpair1 wordPair = new wordpair1();
	    private IntWritable ONE = new IntWritable(1);
	    private IntWritable totalCount = new IntWritable();
	    
	    @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        int neighbors = context.getConfiguration().getInt("neighbors", 2);
	        String[] tokens = value.toString().split("\\s+");
	        if (tokens.length > 1) {
	            for (int i = 0; i < tokens.length; i++) {
	                    tokens[i] = tokens[i].replaceAll("\\W+","");

	                    if(tokens[i].equals("")){
	                        continue;
	                    }

	                    wordPair.setWord(tokens[i]);

	                    int start = (i - neighbors < 0) ? 0 : i - neighbors;
	                    int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
	                    for (int j = start; j <= end; j++) {
	                        if (j == i) continue;
	                        wordPair.setNeighbor(tokens[j].replaceAll("\\W",""));
	                        context.write(wordPair, ONE);
	                    }
	                    wordPair.setNeighbor("*");
	                    totalCount.set(end - start);
	                    context.write(wordPair, totalCount);
	            }
	        }
	    }
	}



	public static class WordPairPartitioner extends Partitioner<wordpair1,IntWritable> {

	    @Override
	    public int getPartition(wordpair1 wordPair, IntWritable intWritable, int numPartitions) {
	        return wordPair.getWord().hashCode() % numPartitions;
	    }
	}

	public static class PairsRelativeOccurrenceReducer extends Reducer<wordpair1, IntWritable, wordpair1, DoubleWritable> {
	    private DoubleWritable totalCount = new DoubleWritable();
	    private DoubleWritable relativeCount = new DoubleWritable();
	    private Text currentWord = new Text("NOT_SET");
	    private Text flag = new Text("*");

	    @Override
	    protected void reduce(wordpair1 key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	        if (key.getNeighbor().equals(flag)) {
	            if (key.getWord().equals(currentWord)) {
	                totalCount.set(totalCount.get() + getTotalCount(values));
	            } else {
	                currentWord.set(key.getWord());
	                totalCount.set(0);
	                totalCount.set(getTotalCount(values));
	            }
	        } else {
	            int count = getTotalCount(values);
	            relativeCount.set((double) count / totalCount.get());
	            context.write(key, relativeCount);
	        }
	    }
	  private int getTotalCount(Iterable<IntWritable> values) {
	        int count = 0;
	        for (IntWritable value : values) {
	            count += value.get();
	        }
	        return count;
	    }
	}
	public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(relativefreq1.class);
        job.setJobName("Relative_Frequencies");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(PairsRelativeOccurrenceMapper.class);
        job.setReducerClass(PairsRelativeOccurrenceReducer.class);
        job.setPartitionerClass(WordPairPartitioner.class);
        job.setNumReduceTasks(3);

        job.setOutputKeyClass(wordpair1.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}