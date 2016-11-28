package temapred.jobs.calculuspercommonword;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.math.stat.descriptive.moment.StandardDeviation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class Reducer extends TableReducer<Text, DoubleWritable, Text> {

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
	throws IOException, InterruptedException {
		ArrayList<Double>valuesArray = new ArrayList<Double>();
		
		for (DoubleWritable val : values) {
			valuesArray.add(val.get());
		}
		
		double[] tabDouble = new double[valuesArray.size()];
		double mean = 0;
		for(int i = 0; i < valuesArray.size(); i++){
			double current = valuesArray.get(i);
			tabDouble[i] = current ;
			mean += current;
		}
		
		mean = mean/(double)valuesArray.size();
		
		StandardDeviation standardDeviation = new StandardDeviation();
		double dStandardDeviation = standardDeviation.evaluate(tabDouble);

		Put put = new Put(Bytes.toBytes(key.toString()));
		put.add(Bytes.toBytes("calculus"), Bytes.toBytes("standardDeviation"), Bytes.toBytes(dStandardDeviation));
		put.add(Bytes.toBytes("calculus"), Bytes.toBytes("mean"), Bytes.toBytes(mean));
		context.write(key,put);
	}
}
