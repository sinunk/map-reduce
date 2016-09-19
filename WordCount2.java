import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.io.*;
import org.apache.hadoop.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount2 extends Configured implements Tool {

	public final static IntWritable ONE = new IntWritable(1);
	
	private Path inputPath;
	private Path outputPath;
	
	public WordCount2(String[] args) {
		
		inputPath = new Path(args[0]);
		outputPath = new Path(args[1]);
	}
	
	public static String[] words(String text) {
	    StringTokenizer st = new StringTokenizer(text);
	    ArrayList<String> result = new ArrayList<String>();
	    while (st.hasMoreTokens())
	    	result.add(st.nextToken());
	    return Arrays.copyOf(result.toArray(),result.size(),String[].class);
	}
	
	public static class PairsMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {

			String[] tokens = WordCount2.words(value.toString());
			
			for (int i = 0; i < tokens.length-1; i++) {
				context.write(new TextPair(tokens[i], tokens[i+1]), ONE);				
			}
		}
	}

	public static class PairsReducer extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {

		@Override
		public void reduce(TextPair key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable value : values) {	
				sum += value.get();
			}
			
			context.write(key, new IntWritable(sum));
		}
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = new Job(conf, "Pairs");

		job.setMapperClass(PairsMapper.class);
		job.setReducerClass(PairsReducer.class);

		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(IntWritable.class);

		TextInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, outputPath);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setJarByClass(WordCount2.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCount2(args), args);
		System.exit(res);
	}
/**
 * A Writable implementation that stores a pair of Text objects
 * 
 * Reference: Tom White's "Hadoop the definitive guide"
 */
public static class TextPair implements WritableComparable<TextPair> {

	private Text first;
	private Text second;

	public TextPair() {
		set(new Text(), new Text());
	}

	public TextPair(String first, String second) {
		set(new Text(first), new Text(second));
	}

	public TextPair(Text first, Text second) {
		set(first, second);
	}

	public void set(Text first, Text second) {
		this.first = first;
		this.second = second;
	}

	public Text getFirst() {
		return first;
	}

	public Text getSecond() {
		return second;
	}

	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof TextPair) {
			TextPair tp = (TextPair) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}

	@Override
	public String toString() {
		return first + "\t" + second;
	}

	public int compareTo(TextPair tp) {
		int cmp = first.compareTo(tp.first);
		if (cmp != 0) {
			return cmp;
		}
		return second.compareTo(tp.second);
	}

	public static class Comparator extends WritableComparator {
 
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
 
		public Comparator() {
			super(TextPair.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1,
                    byte[] b2, int s2, int l2) {
   
			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
				if (cmp != 0) {
					return cmp;
				}
				return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
                                    b2, s2 + firstL2, l2 - firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}
	}

	static {
		WritableComparator.define(TextPair.class, new Comparator());
	}
	

	public static class FirstComparator extends WritableComparator {
 
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
 
		public FirstComparator() {
			super(TextPair.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1,
                    byte[] b2, int s2, int l2) {
   
			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}
 
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			if (a instanceof TextPair && b instanceof TextPair) {
				return ((TextPair) a).first.compareTo(((TextPair) b).first);
			}
			return super.compare(a, b);
		}

	}

}
public static class StringToIntMapWritable implements Writable {
	
	private HashMap<String, Integer> hm = new HashMap<String, Integer>();

	public void clear() {
		hm.clear();
	}

	public String toString() {
		return hm.toString();
	}

	public void readFields(DataInput in) throws IOException {
		int len = in.readInt();
		hm.clear();
		for (int i=0; i<len; i++) {
			int l = in.readInt();
			byte[] ba = new byte[l];
			in.readFully(ba);
			String key = new String(ba);
			Integer value = in.readInt();
			hm.put(key, value);
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(hm.size());
		Iterator<Entry<String, Integer>> it = hm.entrySet().iterator();

		while (it.hasNext()) {
			Map.Entry<String,Integer> pairs = (Map.Entry<String,Integer>)it.next();
			String k = (String) pairs.getKey();
			Integer v = (Integer)pairs.getValue();	        
			out.writeInt(k.length());
			out.writeBytes(k);
			out.writeInt(v);
		}
	}

	public void increment(String t) {
		int count = 1;
		if (hm.containsKey(t)) {
			count = hm.get(t) + count;
		}
		hm.put(t, count);
	}

	public void increment(String t, int value) {
		int count = value;
		if (hm.containsKey(t)) {
			count = hm.get(t) + count;
		}
		hm.put(t, count);
	}

	public void sum(StringToIntMapWritable h) {
		Iterator<Entry<String, Integer>> it = h.hm.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String,Integer> pairs = (Map.Entry<String,Integer>)it.next();
			String k = (String) pairs.getKey();
			Integer v = (Integer)pairs.getValue();
			increment(k, v);
		}
	}
	
}

}
