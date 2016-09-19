import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.IntWritable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class runningtotal implements Writable,WritableComparable<runningtotal> {

    private static IntWritable sum;
    private static IntWritable count;

    public runningtotal(IntWritable sum, IntWritable count) {
        this.sum = sum;
        this.count = count;
    }

    public runningtotal(int sum, int count) {
        this(new IntWritable(sum),new IntWritable(count));
    }

    public runningtotal() {
        this.sum = new IntWritable();
        this.count = new IntWritable();
    }

    @Override
    public int compareTo(runningtotal other) {
        int returnVal = this.sum.compareTo(other.getSum());
        if(returnVal == 0){
        returnVal = count.compareTo(other.getCount());
        }
        return returnVal;}

    public static runningtotal read(DataInput in) throws IOException {
        runningtotal runningTotal = new runningtotal();
        runningTotal.readFields(in);
        return runningTotal;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        sum.write(out);
        count.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sum.readFields(in);
        count.readFields(in);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        runningtotal runningTotal = (runningtotal) o;

        if (count != null ? !count.equals(runningTotal.count) : runningTotal.count != null) return false;
        if (sum != null ? !sum.equals(runningTotal.sum) : runningTotal.sum != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = sum != null ? sum.hashCode() : 0;
        result = 163 * result + (count != null ? count.hashCode() : 0);
        return result;
    }

    public void setSum(int sum){
        this.sum.set(sum);
    }
    public void setCount(int count){
        this.count.set(count);
    }

    public static IntWritable getSum() {
        return sum;
    }

    public static IntWritable getCount() {
        return count;
    }
}