
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.DataInput;
import java.io.DataOutput;



public class zipnames2 {
	
  public static class ZipNamesMapper extends Mapper<LongWritable, Text, zip_age_names, IntWritable> {
    private static final zip_age_names zipAgeName = new zip_age_names();
    private static final IntWritable one = new IntWritable(1);
    protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
        String[] tokens = line.toString().split(",");
        zipAgeName.setZip(Integer.parseInt(tokens[4]));
        zipAgeName.setAge(Integer.parseInt(tokens[3]));
        zipAgeName.setFirstName(tokens[2]);
        zipAgeName.setLastName(tokens[1]);
        context.write(zipAgeName,one);
        }
        }

public static class ZipNamesReducer extends Reducer<zip_age_names, IntWritable, zip_age_names, Text> {
    
    StringBuilder allNames = new StringBuilder();
	
    protected void reduce(zip_age_names zipAgeName, Iterable<IntWritable> ones, Context context) throws IOException, InterruptedException {
    for (IntWritable one : ones){	
    	allNames.append(" "+zipAgeName.getLastName()+" "+zipAgeName.getFirstName());
    }
        context.write(zipAgeName,new Text (allNames.toString()));
        }
    }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: zipnames <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "zipnames");
    job.setJarByClass(zipnames2.class);
    job.setMapperClass(ZipNamesMapper.class); 
    job.setReducerClass(ZipNamesReducer.class);
		
	job.setNumReduceTasks(3);
	job.setMapOutputKeyClass(zip_age_names.class);
	job.setMapOutputValueClass(IntWritable.class);
	job.setOutputKeyClass(zip_age_names.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	configure(conf);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  
  public static void configure(Configuration conf){
	  System.out.println("Test+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
	  
  }
}
