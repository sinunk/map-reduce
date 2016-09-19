import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.IntWritable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class zip_age implements Writable,WritableComparable<zip_age> {

    private IntWritable zip;
    private IntWritable age;

    public zip_age(IntWritable zip, IntWritable age) {
        this.zip = zip;
        this.age = age;
    }

    public zip_age(int zip, int age) {
        this(new IntWritable(zip),new IntWritable(age));
    }

    public zip_age() {
        this.zip = new IntWritable();
        this.age = new IntWritable();
    }

    @Override
    public int compareTo(zip_age other) {
        int returnVal = this.zip.compareTo(other.getZip());
        if(returnVal == 0){
        returnVal = age.compareTo(other.getAge());
        }
        return returnVal;}

    public static zip_age read(DataInput in) throws IOException {
        zip_age zipAge = new zip_age();
        zipAge.readFields(in);
        return zipAge;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        zip.write(out);
        age.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        zip.readFields(in);
        age.readFields(in);
    }

    @Override
    public String toString() {
        return "{zip=["+zip+"]"+
               " age=["+age+"]}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        zip_age zipAge = (zip_age) o;

        if (age != null ? !age.equals(zipAge.age) : zipAge.age != null) return false;
        if (zip != null ? !zip.equals(zipAge.zip) : zipAge.zip != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = zip != null ? zip.hashCode() : 0;
        result = 163 * result + (age != null ? age.hashCode() : 0);
        return result;
    }

    public void setZip(int zip){
        this.zip.set(zip);
    }
    public void setAge(int age){
        this.age.set(age);
    }

    public IntWritable getZip() {
        return zip;
    }

    public IntWritable getAge() {
        return age;
    }
}