import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieGenres {

    public static class MovieMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text outputKey = new Text();
        private final IntWritable outputValue = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(";");

            if (parts.length >= 5) {
                String genres = parts[4];
                List<String> genreList = Arrays.asList(genres.split(","));
                String yearStr = parts[3];

                try {
                    int year = Integer.parseInt(yearStr);

                    if (year >= 2000 && year <= 2006) {
                        String g = getGenreCombination(genreList);
                        if(!g.equals("No-Genre")){
                            outputKey.set("2000-2006," + g);
                            context.write(outputKey, outputValue);
                        }

                    } else if (year >= 2007 && year <= 2013) {
                        String g = getGenreCombination(genreList);
                        if(!g.equals("No-Genre")){
                            outputKey.set("2007-2013," + g);
                            context.write(outputKey, outputValue);
                        }
                    } else if (year >= 2014 && year <= 2020) {
                        String g = getGenreCombination(genreList);
                        if(!g.equals("No-Genre")){
                            outputKey.set("2014-2020," + g);
                            context.write(outputKey, outputValue);
                        }
                    }
                } catch (NumberFormatException e) {
                    
                }
            }
        }

        private String getGenreCombination(List<String> genreList) {

            List<String> OneGenere = new ArrayList();
            OneGenere.add("Action");
            OneGenere.add("Drama");

            int count=0;
            for(String str : OneGenere){
                if(genreList.contains(str)){
                    count +=1;
                }
            }
            if (count ==2){
                return "Action-Drama";
            }

            List<String> TwoGenere = new ArrayList();
            TwoGenere.add("Comedy");
            TwoGenere.add("Romance");

            int count1=0;
            for(String str : TwoGenere){
                if(genreList.contains(str)){
                    count1 +=1;
                }
            }
            if (count1 ==2){
                return "Comedy-Romance";
            }

            List<String> ThreeG = new ArrayList();
            ThreeG.add("Adventure");
            ThreeG.add("Sci-Fi");

            int count2=0;
            for(String str : ThreeG){
                if(genreList.contains(str)){
                    count2 +=1;
                }
            }
            if (count2 ==2){
                return "Adventure-Sci-Fi";
            }
            return "No-Genre";
        }
    }

    public static class MovieReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable outputValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            outputValue.set(sum);
            context.write(key, outputValue);
        }
    }
    public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "MovieGenres");
    job.setJarByClass(MovieGenres.class);
    job.setMapperClass(MovieGenres.MovieMapper.class);
    job.setReducerClass(MovieGenres.MovieReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
