
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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class ziphousevalue2 {
	
  public static class ZipHouseValueMapper extends Mapper<LongWritable, Text, Text, runningtotal> {
    private static final Text zip = new Text();
    private static final runningtotal runningTotal = new runningtotal();
    private  Map<String,runningtotal> map;

    protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
    	Map<String, runningtotal> map = getMap();
        String[] tokens = line.toString().split(",");
        runningTotal.setSum(Integer.parseInt(tokens[4]));
        runningTotal.setCount(1);
        map.put(tokens[3],runningTotal);
        }
    protected void cleanup(Context context) throws IOException, InterruptedException {
          Map<String, runningtotal> map = getMap();
          for (Map.Entry<String, runningtotal> entry : map.entrySet()) {
            context.write(new Text(entry.getKey()), runningTotal);
          }       
        }
        public Map<String, runningtotal> getMap() {
          if (null == map) {
            map = new HashMap<String, runningtotal>();
          }
          return map;
        }
      }
	    
	
public static class ZipHouseValueReducer extends Reducer<Text, runningtotal, Text, DoubleWritable> {
     
    protected void reduce(Text zip, Iterable<runningtotal> runningTotals, Context context) throws IOException, InterruptedException {
       int sum = 0;
       int count = 0;
        for (runningtotal runningTotal: runningTotals)
        	{
        	sum += runningTotal.getSum().get();
        	count += runningTotal.getCount().get();}
        
        context.write(zip, new DoubleWritable(sum/count));
        }
    }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: ziphousevalue <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "ziphousevalue");
    job.setJarByClass(ziphousevalue2.class);
    job.setMapperClass(ZipHouseValueMapper.class); 
    job.setReducerClass(ZipHouseValueReducer.class);
		
	job.setNumReduceTasks(3);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(runningtotal.class);
	job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	configure(conf);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  
  public static void configure(Configuration conf){
	  System.out.println("Test+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
	  
  }
}
