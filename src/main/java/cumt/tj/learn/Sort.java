package cumt.tj.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by sky on 17-5-16.
 * 对输入文件中的数据进行排序。输入文件中每行内容均为一个数字，即一个数据。
 * 输出中每行有两个间隔的数字，其中，第二个代表原始数据，第一个代表这个原始数据在数据集中的排位
 * 设计思路：
 *      在hadoop进行MapReduce过程中，默认会进行一个排序。默认排序按照key进行排序，对于数字类型，直接按照大小进行排序，如果是String类型，
 *      按照字典序进行排序。由于输入文件中都为数字，可以用在map函数中使用IntWrtiable封装数据，作为key值传输到reduce函数中，这样，当
 *      reduce函数拿到<key,value-list>之后，就将key作为value(因为输出的第二位为原始数据)输出，并根据value-list中的元素个数决定输出
 *      的次数（某个数据重复的次数)。在输出中排位以全局变量的形式出现，以统计数据的排位
 */
public class Sort {

    //继承Mapper接口，设置map输入类型为<Object,Text>，输出类型为<IntWritable,IntWritable>
    public static class Map
            extends Mapper<Object, Text, IntWritable, IntWritable> {

        //one表现单词出现一次，这个和wordcount一样
        private final static IntWritable one = new IntWritable(1);
        //number存储每一行的数字
        private IntWritable number=new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //存储每行的数字
            number.set(Integer.parseInt(value.toString()));
            context.write(number, one);
        }
    }

    //继承Reducer接口，设置reduce输入类型为<IntWritable,IntWritable>,输出类型为<IntWritable,IntWritable>
    public static class Reduce
            extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

        //index记录数字的行号，即排位
        private IntWritable index = new IntWritable(1);

        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            //对<key,value-list>遍历，进行输出
            for(IntWritable val:values){
                context.write(index,key);
                index=new IntWritable(index.get()+1);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        //每次先删除output文件夹，不然会报错
        FileUtil.deleteDir(args[1]);
        Configuration conf = new Configuration();

        //配置作业名为sort
        Job job = Job.getInstance(conf, "sort");

        //配置各个作业类
        job.setJarByClass(Sort.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        //从第一个命令行参数读取输入文件夹，从第二个命令行参数读取输出文件夹
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
