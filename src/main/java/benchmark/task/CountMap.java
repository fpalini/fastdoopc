package benchmark.task;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import fastdoop.Record;

/**
 * 
 * @author Giuseppe Angri - info@giuseppeangri.com
 * @author Giovanni De Costanzo - decostanzo.g@gmail.com
 * @author Francesco Isernia - isernia.francesco@gmail.com
 * 
 */
public class CountMap extends Mapper<Text, Record, Text, Text> {

	@Override
	public void map(Text key, Record value, Context context) {
		
		int startIndex = value.getStartValue();
		int end = (value.getEndValue()-value.getStartValue()+1);

		process(value.getBuffer(), startIndex, startIndex+end);

	}
	
	public void process(byte[] buffer, int start, int end) {
		
		int a_count=0, c_count=0, g_count=0, t_count=0, n_count=0;

		for(int i=start; i<end; i++){
			
			char c = (char)buffer[i]; 
						
			if(c=='A' || c=='a')
				a_count++;
			else if(c=='C' || c=='c')
				c_count++;
			else if(c=='G' || c=='g')
				g_count++;
			else if(c=='T' || c=='t')
				t_count++;
			else if(c=='N' || c=='n')
				n_count++;
		}
	}
	
}
