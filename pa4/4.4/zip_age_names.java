import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.IntWritable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class zip_age_names implements Writable,WritableComparable<zip_age_names> {

    private IntWritable zip;
    private IntWritable age;
    private Text firstName;
    private Text lastName;

    public zip_age_names(IntWritable zip, IntWritable age, Text firstName, Text lastName) {
        this.zip = zip;
        this.age = age;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    public zip_age_names(int zip, int age, String firstName, String lastName) {
        this(new IntWritable(zip),new IntWritable(age),new Text(firstName), new Text(lastName));
    }

    public zip_age_names() {
        this.zip = new IntWritable();
        this.age = new IntWritable();
        this.firstName = new Text();
        this.lastName = new Text();
    }

    @Override
    public int compareTo(zip_age_names other) {
        int returnVal = this.zip.compareTo(other.getZip());
        if(returnVal == 0){
        returnVal = age.compareTo(other.getAge());
        if(returnVal == 0){
            returnVal = firstName.compareTo(other.getFirstName());
            }
        }
        return returnVal;}

    public static zip_age_names read(DataInput in) throws IOException {
        zip_age_names zipAgeName = new zip_age_names();
        zipAgeName.readFields(in);
        return zipAgeName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        zip.write(out);
        age.write(out);
        firstName.write(out);
        lastName.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        zip.readFields(in);
        age.readFields(in);
        firstName.readFields(in);
        lastName.readFields(in);
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

        zip_age_names zipAgeName = (zip_age_names) o;
        
        if (firstName != null ? !firstName.equals(zipAgeName.firstName) : zipAgeName.firstName != null) return false;
        if (age != null ? !age.equals(zipAgeName.age) : zipAgeName.age != null) return false;
        if (zip != null ? !zip.equals(zipAgeName.zip) : zipAgeName.zip != null) return false;
        

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
    public void setFirstName(String firstName){
        this.firstName.set(firstName);
    }
    public void setLastName(String lastName){
        this.lastName.set(lastName);
    }
    public IntWritable getZip() {
        return zip;
    }

    public IntWritable getAge() {
        return age;
    }
    public Text getFirstName() {
        return firstName;
    }
    public Text getLastName() {
        return lastName;
    }
}