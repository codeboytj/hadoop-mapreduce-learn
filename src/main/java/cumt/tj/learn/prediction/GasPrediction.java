package cumt.tj.learn.prediction;

import cumt.tj.learn.FileUtil;
import cumt.tj.learn.util.arima.ARIMA;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;

/**
 * Created by sky on 17-5-22.
 * 这是一个利用时间序列预测模型ARIMA，预测危险气体浓度的例子，Java版本的实现，原版代码取自github：https://github.com/AdairZhao/ARIMA
 * 输入文件中数据是气体浓度的时间序列
 * 程序通过mapreduce计算出每天气体浓度的最大值并输出到文件之中
 * 计算出最大值之后，程序通过arima模型对未来一天的气体浓度最大值进行预测
 * 预测结果通过控制台打印出来
 *
 */
public class GasPrediction {

    public static BufferedWriter bw;

    public GasPrediction(){
    }

    public static class MaxConcentrationMapper
            extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            //1.分割每行数据
            int day;int concentration;
            for(int i=0;;i++){
                if (line.charAt(i)==',') {
                    day=Integer.parseInt(line.substring(0,i));
                    concentration=Integer.parseInt(line.substring(i+1));
                    break;
                }
            }
            //2.输出结果
            context.write(new IntWritable(day), new IntWritable(concentration));
        }
    }

    public static class MaxConcentrationReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            //计算每一天的最大值
            int maxValue = Integer.MIN_VALUE;
            for (IntWritable value : values) {
                maxValue = Math.max(maxValue, value.get());
            }
            //写入文件
            context.write(key, new IntWritable(maxValue));
            if(bw==null){
                try {
                    bw=new BufferedWriter(new FileWriter("outputs/GasPrediction/maxGasDataPerDay"));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
//            bw.write(maxValue);
            System.out.println(String.valueOf(maxValue));
            bw.write(String.valueOf(maxValue));
            bw.flush();
            bw.newLine();
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //数据预处理，提取出气体浓度每天的最大值

        //每次先删除output文件夹，不然会报错
        FileUtil.deleteDir(args[1]);
        Configuration conf = new Configuration();

        //配置作业名为sort
        Job job = Job.getInstance(conf, "get max value per day");

        //配置各个作业类
        job.setJarByClass(GasPrediction.class);
        job.setMapperClass(MaxConcentrationMapper.class);
        job.setReducerClass(MaxConcentrationReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        //从第一个命令行参数读取输入文件夹，从第二个命令行参数读取输出文件夹
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if(job.waitForCompletion(true)){
            bw.close();
            //如果成功的话，就开始使用arima模型对后面一天的最大值进行预测
            ARIMA.prediction("outputs/GasPrediction/maxGasDataPerDay");
        }
//        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
