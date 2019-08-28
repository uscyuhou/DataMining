/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.StringTokenizer;

import java.lang.System;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ArrayWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Yu_Hou_Average {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
//        private IntWritable[] temp = new IntWritable[2];

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {


            StringTokenizer itr = new StringTokenizer(value.toString());

            //split value
            String[] valueList = value.toString().split(",");
            //clean
            String cleanValue = valueList[3].toLowerCase().replaceAll("'", "").replaceAll("-", "").replaceAll("[^a-zA-Z0-9 ]", " ").replaceAll("( )+", " ").trim();
            if (valueList[18].equals("page_count") || cleanValue.equals("") ){
                return;
            }
            int pageCount = Integer.parseInt(valueList[18]);

//
//
//            System.out.println(key);
//            System.out.println(cleanValue);

            word.set(cleanValue);
            context.write(word, new IntWritable(pageCount));


//            while (itr.hasMoreTokens()) {
//                word.set(itr.nextToken());
//                context.write(word, one);
//            }
        }
    }


    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
//            System.out.println(key);
//            System.out.println(values);
            int sum = 0;
            int count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }


            float x = (float)sum/count;
            String xx = String.format("%.3f", x);
            String str = String.valueOf(count)+ "\t" + xx;
            result.set(str);
            context.write(key, result);
        }
    }

    public static class IntComparator extends WritableComparator {

        public IntComparator() {
            super(IntWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {



            Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
            Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

            return v1.compareTo(v2) * (-1);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Yu_Hou_Average.class);
        job.setMapperClass(TokenizerMapper.class);

//        job.setSortComparatorClass(IntComparator.class);

        job.setReducerClass(IntSumReducer.class);



        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
