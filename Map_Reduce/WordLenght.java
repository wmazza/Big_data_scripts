import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordLenght {

  //Define Mapper and Map function
  public static class Map
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
	private Text lenght = new Text();
	private String temp;
	private int len;

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	  String line = value.toString();
	  
	  //Replace special chars
	  String clean_line = line.replaceAll("[,.]", "").replaceAll("['-]", " ");
	 
      StringTokenizer itr = new StringTokenizer(clean_line);
	  
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
		
		temp = word.toString();
		len = temp.length();
		lenght.set(Integer.toString(len));
		
        context.write(lenght, one);
      }
    }
  }


  //Define Reducer and Reduce function
  public static class Reduce
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
	 
    //Instantiate Job
    Job job = Job.getInstance(conf, "word lenght");
    job.setJarByClass(WordLenght.class);
    
    //Pass Mapper and Map function to the Job
    job.setMapperClass(Map.class);

    //Specify a Combiner to aggregate middle data from Mapping
    job.setCombinerClass(Reduce.class);

    //Pass Reducer and Reduce function to the Job
    job.setReducerClass(Reduce.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    //Specify path of input files
    FileInputFormat.addInputPath(job, new Path(args[0]));

    //Specify path for output file
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    //Submit the Job to the cluster and wait for execution to complete
    System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	
  }
}
