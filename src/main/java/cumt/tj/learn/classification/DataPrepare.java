package cumt.tj.learn.classification;

import java.io.*;
import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.commons.cli2.util.HelpFormatter;
import org.apache.commons.io.Charsets;
import org.apache.mahout.classifier.evaluation.Auc;
import org.apache.mahout.classifier.sgd.CsvRecordFactory;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;

import java.util.Locale;
/**
 * Created by sky on 17-5-19.
 * 《mahout实践指南》第5章的例子中的数据集准备阶段
 * 利用某监测系统对某危险气体浓度检测值作为样本，对危险气体浓度是否会达到危险进行预测。
 */
public class DataPrepare {
    //原始数据的位置
    private static final String originalFile="inputs/GasPrediction/gasdata";
    private static final String modifiedFile="inputs/GasPrediction/gasData.csv";

    /**
     * 准备数据集，数据预处理
     * 将原来数据集中的时间列删除，添加新列“是否危险”，用于分析。
     * 对于该危险气体，浓度大于0.18视为危险状态。
     * @throws IOException
     */
    public void getData() throws IOException {
        BufferedReader br=new BufferedReader(new FileReader(originalFile));
        BufferedWriter bw=new BufferedWriter(new FileWriter(modifiedFile));

        //line为读入的行，tmp[]存储读入行按照“,”进行分割的字符串数组，writeLine表示最终要写入输出文件的行
        String line;String writeLine;String tmp[];

        //读入文件，对每行进行遍历
        //第一行，表头
        line=br.readLine();
        tmp=line.split(",");
        writeLine=tmp[0]+",DangerOrNot";
        bw.write(writeLine);bw.newLine();bw.flush();

        //数据行
        while ((line=br.readLine())!=null){
            tmp=line.split(",");

            //如果浓度大于0.18,表示具有危险性
            if(Double.parseDouble(tmp[0])>0.18){
                writeLine=tmp[0]+",YES";
            }else {
                writeLine=tmp[0]+",NO";
            }

            bw.write(writeLine);bw.newLine();bw.flush();
        }

        //关闭流
        br.close();
        bw.close();
    }
}
