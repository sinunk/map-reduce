
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.DataInput;
import java.io.DataOutput;



public class zipnames1 {
	
  public static class ZipNamesMapper extends Mapper<LongWritable, Text, zip_age, Text> {
    private static final zip_age zipAge = new zip_age();
    private static final Text fullName = new Text();
    
    protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
        String[] tokens = line.toString().split(",");
        zipAge.setZip(Integer.parseInt(tokens[4]));
        zipAge.setAge(Integer.parseInt(tokens[3]));
        fullName.set(tokens[2]+" "+tokens[1]);
        context.write(zipAge, fullName);
        }
        }

public static class ZipNamesReducer extends Reducer<zip_age, Text, zip_age, Text> {
    
    StringBuilder allNames = new StringBuilder();
    
    protected void reduce(zip_age zipAge, Iterable<Text> fullNames, Context context) throws IOException, InterruptedException {
        
        for (Text fullName : fullNames)
        	{
        	allNames.append(" " + fullName);
        	
        	}
        
        context.write(zipAge, new Text (allNames.toString()));
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
    job.setJarByClass(zipnames1.class);
    job.setMapperClass(ZipNamesMapper.class); 
    job.setReducerClass(ZipNamesReducer.class);
		
	job.setNumReduceTasks(3);
	job.setOutputKeyClass(zip_age.class);
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
