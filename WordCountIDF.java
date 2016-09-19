import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountIDF extends Configured implements Tool {
	
	  // where to put the data in hdfs when we're done
    private static final String OUTPUT_PATH = "output";
 
    // where to read the data from.
    private static final String INPUT_PATH = "input";

    public class WordCountIDFReducer extends Reducer<Text, Text, Text, Text> {
    	 
        private final DecimalFormat DF = new DecimalFormat("###.########");
        private static Text OUT_KEY = new Text(); 
        private static Text OUT_VALUE = new Text(); 
        
        public WordCountIDFReducer() {
        }
        
     
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // get the number of documents indirectly from the file-system (stored in the job name on purpose)
            int numberOfDocumentsInCorpus = Integer.parseInt(context.getJobName());
            // total frequency of this word
            int numberOfDocumentsInCorpusWhereKeyAppears = 0;
            Map<String, String> tempFrequencies = new HashMap<String, String>();
            for (Text val : values) {
                String[] documentAndFrequencies = val.toString().split("=");
                numberOfDocumentsInCorpusWhereKeyAppears++;
                tempFrequencies.put(documentAndFrequencies[0], documentAndFrequencies[1]);
            }
            for (String document : tempFrequencies.keySet()) {
                String[] wordFrequenceAndTotalWords = tempFrequencies.get(document).split("/");
     
                //word frequency
                double tf = Double.valueOf(Double.valueOf(wordFrequenceAndTotalWords[0])
                        / Double.valueOf(wordFrequenceAndTotalWords[1]));
     
                //inverse document frequency 
                double idf = (double) numberOfDocumentsInCorpus / (double) numberOfDocumentsInCorpusWhereKeyAppears;
     
                //given that log(10) = 0, just consider the term frequency in documents
                double tfIdf = (numberOfDocsInCorpus == numberOfDocsInCorpusWithKey) ? tf 
                	     : tf * Math.log10(idf); 
                	 
                	   OUT_KEY.set(key + "@" + entry.getKey()); 
                	   OUT_VALUE.set("[" + numberOfDocsInCorpusWithKey + "/" 
                	     + numberOfDocsInCorpus + " , " 
                	     + wordFrequenceAndTotalWords[0] + "/" 
                	     + wordFrequenceAndTotalWords[1] + " , " + DF.format(tfIdf) 
                	     + "]"); 
                	   context.write(OUT_KEY, OUT_VALUE);
            }
        }
        
    }

public static class WordCountIDFMapper extends Mapper<LongWritable, Text, Text, Text> {
 
    public WordCountIDFMapper() {
    }
    private static Text INTERM_KEY = new Text(); 
    private static Text INTERM_VALUE = new Text(); 
    
   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	   String[] wordAndCounters = value.toString().split("\t"); 
	   String[] wordAndDoc = wordAndCounters[0].split("@"); 
	  
	   INTERM_KEY.set(wordAndDoc[0]); 
	   INTERM_VALUE.set(wordAndDoc[1] + "=" + wordAndCounters[1]); 
	   context.write(INTERM_KEY, INTERM_VALUE);
    }
}
 
    public int run(String[] args) throws Exception {
 
        Configuration conf = getConf();
        Job job = new Job(conf, "IDF");
 
        job.setJarByClass(WordCountIDF.class);
        job.setMapperClass(WordCountIDFMapper.class);
        job.setReducerClass(WordCountIDFReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        
        //Getting the number of documents from the original input directory.
        Path inputPath = new Path("input");
        FileSystem fs = inputPath.getFileSystem(conf);
        FileStatus[] stat = fs.listStatus(inputPath);
        job.setJobName(String.valueOf(stat.length));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
 
    public static void main(String[] args) throws Exception {
        
        int res = ToolRunner.run(new Configuration(), new WordCountIDF(), args);
        System.exit(res);
    }
}
