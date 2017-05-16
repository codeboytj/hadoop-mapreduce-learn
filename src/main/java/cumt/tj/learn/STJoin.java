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
import java.util.*;

/**
 * Created by sky on 17-5-16.
 * 单表连接
 * 将输入表文件中的child-parent映射关系做成grandchild-grandparent的关系
 * 设计思路：
 *      问题相当于关系数据库里面的表连接，左表与右表相同的连接。幸运的是，MapReduce的shuffle阶段会把相同key的<key,value>连接成<key,value-list>。
 *      将左表看成是grandchild-parent表，右表看成是parent-grandparent表，链接的过程就是parent相同的grandchild与grandparent的链接。
 *      为了利用shuffle过程，在map阶段可以读入左表将，parent设成key,grandchild设置为value;可以读入右表将，parent设成key,grandparent设置为value;
 *      这样就能链接成<parent,{grandchild1,……,grandchildn,,grandparent,……,grandparentn}>的形式。为了从拿到的value-list中分开哪些是child，哪些是parent，需要在map
 *      阶段进行value的标记，从左表读取的value为child，字符串开头以"1+"标记,从右表读取的value为grandparent，字符串开头以"2+"标记。
 *      而reduce阶段就是将<key,value-list>的形式输出成grandchild-grandparent的表。从value-list中分别读出grandchild与grandparent的数组，进行
 *      笛卡尔积就是grandchild-grandparent对的数组了。
 */
public class STJoin {

    //继承Mapper接口，设置map输入类型为<Object,Text>，输出类型为<Text, Text>
    public static class Map
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //1. 将每行数据分成2个字符串，str1与str2
            String line=value.toString();
            int i=0;
            while (line.charAt(i)!=' '){
                i++;
            }
            String str1=line.substring(0,i);String str2=line.substring(i+1);
            //2 过滤掉表头
            if(!str1.equals("child")){
                //3.1 建立左表形成的<key,value>，<str2,"1+"+str1>
                context.write(new Text(str2),new Text("1+"+str1));
                //3.2 建立右表形成的<key,value>，<str1,"2+"+str2>
                context.write(new Text(str1),new Text("2+"+str2));
            }
        }
    }

    //继承Reducer接口，设置reduce输入类型为<Text, Text>，输出类型为<Text, Text>
    public static class Reduce
            extends Reducer<Text, Text,Text, Text> {

        //index记录数字的行号，即排位
        private static int time=0;

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            //1. 首先要输出表头
            if(time==0){
                context.write(new Text("grandchild"),new Text("grandparent"));
                time++;
            }
            //2. value-list分割成grandchild数组与grandparent数组
            ArrayList<String> grandchilds=new ArrayList<String>();
            ArrayList<String> grandparents=new ArrayList<String>();
            String valStr;
            for(Text val:values){
                valStr=val.toString();
                if(valStr.startsWith("1+")){
                    grandchilds.add(valStr.substring(2));
                }else {
                    grandparents.add(valStr.substring(2));
                }
            }
            //3. 将grandchild数组与grandparent数组进行笛卡尔积，结果写入输出
            Iterator<String> childIterator=grandchilds.iterator();
            Iterator<String> parentIterator;
            String childItem;String parentItem;
            while (childIterator.hasNext()){
                childItem=childIterator.next();

                //遍历完一次，需要重新赋值，然后再遍历……
                parentIterator=grandparents.iterator();
                while (parentIterator.hasNext()){
                    parentItem=parentIterator.next();
                    //输出
                    context.write(new Text(childItem),new Text(parentItem));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        //每次先删除output文件夹，不然会报错
        FileUtil.deleteDir(args[1]);
        Configuration conf = new Configuration();

        //配置作业名为sort
        Job job = Job.getInstance(conf, "single table join");

        //配置各个作业类
        job.setJarByClass(STJoin.class);
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
