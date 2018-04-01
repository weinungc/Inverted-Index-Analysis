package my_package.Inverted_Index_Project;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class InvertedIndexJob {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		if (args.length != 2) {
			System.err.println("Usage: Word count <input path> <output path>");
			System.exit(-1);
		}

		// Creating a Hadoop job and assigning a job name for identification.
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "Hadoop MapReduce Inverted Index");
		job.setJarByClass(InvertedIndexJob.class);
		job.setJobName("Inverted Index Job");
		// The HDFS input and
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
//mapper
class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
	Text word = new Text();
	Text id = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] cont = value.toString().split("\t");
		String[] token = cont[1].split(" ");
		id.set(cont[0]);

		for(int i =0; i< token.length;i++) {
			word.set(token[i]);
			context.write(word,id);
		}
	}

}


// reducer
class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
	Text result = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		HashMap<String, Integer> invertCount = new HashMap<String, Integer>();
		
		
		//count frequent in id 
		for (Text value : values) {
			String docID = value.toString();
			
			if (invertCount.containsKey(docID))
				invertCount.put(docID, invertCount.get(docID) + 1);
			else
				invertCount.put(docID, 1);
		
		}
		
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, Integer> map : invertCount.entrySet()) {
			sb.append(map.getKey() + ":" + map.getValue() + "\t");
		}
		result.set(sb.toString());
		context.write(key, result);
	}

}