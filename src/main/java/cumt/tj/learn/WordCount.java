package cumt.tj.learn;

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

/**
 * Created by sky on 17-5-15.
 * wordCount程序在map阶段接受输入<key,value>(key是当前输入的行号，value是对应行的内容），然后对此行进行切词，
 *     每切下一个词就将其组织成<word,1>的形式输出，表示word出现了一次。
 * 在reduce阶段，TaskTracker会接收到<word,{1,1,1,1,……}>形式的数据，也就是特定单词及其出现次数的情况，其中“1”表示word的频数。
 *     所以reduce每接收一个<word,{1,1,1,1,……}>，就会在word的频数上加1,最后组织成<word,sum>直接输出。
 * 输入数据来自文件，输出数据写入文件
 */
public class WordCount {

    //继承Mapper接口，设置map输入类型为<Object,Text>，输出类型为<Text,IntWritable>
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        //one表现单词出现一次
        private final static IntWritable one = new IntWritable(1);
        //word存储切下的单词
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //对行进行切词
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                //遍历，将切下的单词存入word
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    //继承Reducer接口，设置reduce输入类型为<Text,IntWritable>,输出类型为<Text,IntWritable>
    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {

        //result记录单词的频数
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;

            //对<key,value-list>遍历，计算value的和
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            //收集结果
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        //每次先删除output文件夹，不然会报错
        FileUtil.deleteDir(args[1]);
        Configuration conf = new Configuration();

        //配置作业名为word count
        Job job = Job.getInstance(conf, "word count");

        //配置各个作业类，如实现map的类为TokenizerMapper.class
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //从第一个命令行参数读取输入文件夹，从第二个命令行参数读取输出文件夹
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
