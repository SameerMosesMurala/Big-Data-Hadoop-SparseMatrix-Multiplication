import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Elem implements Writable {
Elem M;
Elem N;
public short tag;
int index;
double value;
Elem () {}
    Elem (short t,int id, double v) { 
	tag = t; index=id;value = v;
	
	}
	public void write ( DataOutput out ) throws IOException {
       out.writeShort(tag);
		out.writeInt(index);
		out.writeDouble(value);
    }

    public void readFields ( DataInput in ) throws IOException {
        tag = in.readShort();
		index = in.readInt();
		value = in.readDouble();
        
    }
}

class Pair implements WritableComparable<Pair> {
int I;
int J;
//public int I;
//public int J;
public int compareTo ( Pair o ) {
return (I == o.I) ? J−o.J : I−o.I;
}
Pair(int i,int j)
{
I=i;
J=j;
}
public void write ( DataOutput out ) throws IOException {
}
    public void readFields ( DataInput in ) throws IOException {
	}
}

public class Multiply {

public static class MatrixMMMapper extends Mapper<Object,Text,IntWritable,Elem > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
			int i=s.nextInt();
			int j=s.nextInt();
			double v=s.nextDouble();
            //Elem m = new Elem(0,i,v);
            context.write(new IntWritable(j),new Elem((short)0,i,v));
            s.close();
        }
    }

	
public static class MatrixMNMapper extends Mapper<Object,Text,IntWritable,Elem > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
			int i=s.nextInt();
			int j=s.nextInt();
			double v=s.nextDouble();
            //Elem m = new Elem(0,i,v);
            context.write(new IntWritable(i),new Elem((short)1,j,v));
            s.close();
        }
    }
	
	public static class MatrixReducer extends Reducer<IntWritable,Elem,Pair,DoubleWritable> {
	static Vector<Elem> A = new Vector<Elem>();
    static Vector<Elem> B = new Vector<Elem>();
        @Override
        public void reduce ( IntWritable key, Iterable<Elem> values, Context context )
                           throws IOException, InterruptedException {
            for (Elem v: values)
                if (v.tag == 0)
                    A.add(v);
                else B.add(v);
            for ( Elem a: A )
                for ( Elem b: B )
                    context.write(new Pair(a.index,b.index),new DoubleWritable(a.value*b.value));
        }
    }
	
	public static class Mapper2 extends Mapper<Pair,DoubleWritable,Pair,DoubleWritable> {
        @Override
        public void map ( Pair key, DoubleWritable value, Context context )
                        throws IOException, InterruptedException {
            context.write(key,value);
        }
    }
	
	public static class Reducer2 extends Reducer<Pair,DoubleWritable,Pair,DoubleWritable> {
        public void reduce ( Pair key, Iterable<DoubleWritable> values, Context context )
                           throws IOException, InterruptedException {
						   double m=0;
						 for (DoubleWritable v: values) {
                m = m+v.get();
                
            };  
                    context.write(key,new DoubleWritable(m));
        }
    }
    public static void main ( String[] args ) throws Exception {
	Job job = Job.getInstance();
        job.setJobName("MyJob");
        job.setJarByClass(Multiply.class);
        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Elem.class);
        //job.setMapperClass(MyMapper.class);
        job.setReducerClass(MatrixReducer.class);
        //job.setInputFormatClass(TextInputFormat.class);
		MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,MatrixMMMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,MatrixMNMapper.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);                                  
        //FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);
		Job job1 = Job.getInstance();
        job1.setJobName("MyJob1");
        job1.setJarByClass(Multiply.class);
        job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(DoubleWritable.class);
        job1.setMapOutputKeyClass(Pair.class);
        job1.setMapOutputValueClass(DoubleWritable.class);
        job1.setMapperClass(Mapper2.class);
        job1.setReducerClass(Reducer2.class);
	    job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job1,args[2]);
        FileOutputFormat.setOutputPath(job1,new Path(args[3]));
        job1.waitForCompletion(true);
    }
}
