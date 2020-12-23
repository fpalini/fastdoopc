package benchmark.task;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import fastdoop.Record;

public class CountMapReduce {
	
	public static class CountMapper extends Mapper<Text, Record, Text, LongWritable> {
		@Override
		protected void map(Text key, Record value, Context context) throws IOException, InterruptedException {
			long a, c, g, t, n;
			a = c = g = t = n = 0;
			
			for (int i = value.getStartValue(); i < value.getEndValue()+1; i++) 
				switch ((char)value.getBuffer()[i]) {
					case 'A': a++; break;
					case 'C': c++; break;
					case 'G': g++; break;
					case 'T': t++; break;
					case 'N': n++; break;
					
					case 'a': a++; break;
					case 'c': c++; break;
					case 'g': g++; break;
					case 't': t++; break;
					case 'n': n++; break;
					
					default: try { 
						throw new Exception("Character "+(char)value.getBuffer()[i]+" not valid for the read."); 
						} catch (Exception e) { e.printStackTrace(); }
				}
			
			context.write(new Text("A"), new LongWritable(a));
			context.write(new Text("C"), new LongWritable(c));
			context.write(new Text("G"), new LongWritable(g));
			context.write(new Text("T"), new LongWritable(t));
			context.write(new Text("N"), new LongWritable(n));
		}
	}

	public static class CountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		@Override
		protected void reduce(Text nucleotide, Iterable<LongWritable> counts, Context context) throws IOException, InterruptedException {
			long sum = 0;

			for (LongWritable count : counts)
				sum += count.get();
			
			context.write(nucleotide, new LongWritable(sum));
		}
	}
}
