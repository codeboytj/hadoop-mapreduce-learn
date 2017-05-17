package cumt.tj.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by sky on 17-5-17.
 * 多表连接
 * 输入文件factory包含工厂名列factoryname和地址编号列addressed，输入表文件address包含地址名列addressname和地址编号列
 * addressID。要求从输入数 * 据中找到工厂名列和地址名列的对应关系，输出工厂名-地址名表。
 * 设计思路：
 *      问题相当与单表链接类似，问题是怎么在map中识别哪个是来自factory的数据，哪个是address的数据？分析输入文件，发现
 *      factory中的地址编号数据在末尾，而address中的地址编号数据在行首，可以据此区分两个文件。
 */
public class MTJoin {

    //继承Mapper接口，设置map输入类型为<Object,Text>，输出类型为<Text, Text>
    public static class Map
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //1. 将每行数据分成2个字符串，str1与str2
            String line = value.toString();
            int i = 0;
            //1.1 过滤掉表头
            if (line.contains("factoryname") || line.contains("addressID")) {
                return;
            }
            //1.2 找出数据分割点
            while (line.charAt(i) > '9' || line.charAt(i) < '0') {
                i++;
            }
            //此时i记录的是地址编号的索引
            //2. 建立左右表<key,value>对
            //单独声明变量可以方便调试
            String str1;String str2;
            if (line.charAt(0) > '9' || line.charAt(0) < '0') {
                str1=line.substring(0,i-1);str2=line.substring(i);
                //左表行首为字母
                //2.1 建立左表形成的<key,value>，<str2,"1+"+str1>
                context.write(new Text(str2), new Text("1+"+str1));
            } else {
                str1=line.substring(0,i+1);str2=line.substring(i+2);
                //2.2 建立右表形成的<key,value>，<str1,"2+"+str2>
                context.write(new Text(str1), new Text("2+" + str2));
            }
        }
    }

    //继承Reducer接口，设置reduce输入类型为<Text, Text>，输出类型为<Text, Text>
    public static class Reduce
            extends Reducer<Text, Text, Text, Text> {

        //index记录数字的行号，即排位
        private static int time = 0;

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            //1. 首先要输出表头
            if (time == 0) {
                context.write(new Text("factoryname"), new Text("addressname"));
                time++;
            }
            //2. value-list分割成factoryname数组与addressname数组
            ArrayList<String> factoryNames = new ArrayList<String>();
            ArrayList<String> addressNames = new ArrayList<String>();
            String valStr;
            for (Text val : values) {
                valStr = val.toString();
                if (valStr.startsWith("1+")) {
                    factoryNames.add(valStr.substring(2));
                } else {
                    addressNames.add(valStr.substring(2));
                }
            }
            //3. 将factoryname数组与addressname数组，结果写入输出
            Iterator<String> factoryIterator = factoryNames.iterator();
            Iterator<String> addressIterator;
            String factoryItem;
            String addressItem;
            while (factoryIterator.hasNext()) {
                factoryItem = factoryIterator.next();

                //遍历完一次，需要重新赋值，然后再遍历……
                addressIterator = addressNames.iterator();
                while (addressIterator.hasNext()) {
                    addressItem = addressIterator.next();
                    //输出
                    context.write(new Text(factoryItem), new Text(addressItem));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        //每次先删除output文件夹，不然会报错
        FileUtil.deleteDir(args[1]);
        Configuration conf = new Configuration();

        //配置作业名为sort
        Job job = Job.getInstance(conf, "multiple table join");

        //配置各个作业类
        job.setJarByClass(MTJoin.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //从第一个命令行参数读取输入文件夹，从第二个命令行参数读取输出文件夹
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
