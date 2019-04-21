//package org.myorg;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordCount {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                if(token.startsWith("a")){
                  word.set("A:");
                  output.collect(word, one);
                }
                if(token.startsWith("b")){
                  word.set("B:");
                  output.collect(word, one);
                }
                if(token.startsWith("c")){
                  word.set("C:");
                  output.collect(word, one);
                }
                if(token.startsWith("d")){
                  word.set("D:");
                  output.collect(word, one);
                }
                if(token.startsWith("e")){
                  word.set("E:");
                  output.collect(word, one);
                }
                if(token.startsWith("f")){
                  word.set("F:");
                  output.collect(word, one);
                }
                if(token.startsWith("g")){
                  word.set("G:");
                  output.collect(word, one);
                }
                if(token.startsWith("h")){
                  word.set("H:");
                  output.collect(word, one);
                }
                if(token.startsWith("i")){
                  word.set("I:");
                  output.collect(word, one);
                }
                if(token.startsWith("j")){
                  word.set("J:");
                  output.collect(word, one);
                }
                if(token.startsWith("k")){
                  word.set("K:");
                  output.collect(word, one);
                }
                if(token.startsWith("l")){
                  word.set("L:");
                  output.collect(word, one);
                }
                if(token.startsWith("m")){
                  word.set("M:");
                  output.collect(word, one);
                }
                if(token.startsWith("n")){
                  word.set("N:");
                  output.collect(word, one);
                }
                if(token.startsWith("o")){
                  word.set("O:");
                  output.collect(word, one);
                }
                if(token.startsWith("p")){
                  word.set("P:");
                  output.collect(word, one);
                }
                if(token.startsWith("q")){
                  word.set("Q:");
                  output.collect(word, one);
                }
                if(token.startsWith("r")){
                  word.set("R:");
                  output.collect(word, one);
                }
                if(token.startsWith("s")){
                  word.set("S:");
                  output.collect(word, one);
                }
                if(token.startsWith("t")){
                  word.set("T:");
                  output.collect(word, one);
                }
                if(token.startsWith("u")){
                  word.set("U:");
                  output.collect(word, one);
                }
                if(token.startsWith("v")){
                  word.set("V:");
                  output.collect(word, one);
                }
                if(token.startsWith("w")){
                  word.set("W:");
                  output.collect(word, one);
                }
                if(token.startsWith("x")){
                  word.set("X:");
                  output.collect(word, one);
                }
                if(token.startsWith("y")){
                  word.set("Y:");
                  output.collect(word, one);
                }
                if(token.startsWith("z")){
                  word.set("Z:");
                  output.collect(word, one);
                }

                //output.collect(word, one);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("wordcount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
