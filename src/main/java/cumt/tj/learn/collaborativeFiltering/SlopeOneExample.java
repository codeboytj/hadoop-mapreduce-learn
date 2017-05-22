package cumt.tj.learn.collaborativeFiltering;


import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.CachingRecommender;
import org.apache.mahout.cf.taste.impl.recommender.slopeone.SlopeOneRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

import java.io.*;
import java.util.List;

/**
 * Created by sky on 17-5-18.
 * mahout初试，编写一个简单的推荐系统
 */
public class SlopeOneExample {
    private String inputFile="inputs/SlopeOneExample/ml-1m/ratings.dat";
    private String outputFile="inputs/SlopeOneExample/ml-1m/ratings.csv";

    public static void main(String[] args) throws IOException, TasteException {
        SlopeOneExample slopeOneExample=new SlopeOneExample();
        //1. 建立模型
        DataModel model=slopeOneExample.createModel();
        //2. 在模型上创建推荐系统
        CachingRecommender cachingRecommender=new CachingRecommender(new SlopeOneRecommender(model));
//        CachingRecommender cachingRecommender=null;
        //3. 遍历每位用户，打印推荐结果
        for(LongPrimitiveIterator it=model.getUserIDs();it.hasNext();){
            long userId=it.nextLong();
            //每位用户推荐10部电影
            List<RecommendedItem> recommendedItems=cachingRecommender.recommend(userId,10);
            //打印推荐结果
            for(RecommendedItem recommendedItem:recommendedItems){
                System.out.println("User "+userId+": "+recommendedItem);
            }
        }
    }

    /**
     * 将原始文件转换为mahout容易使用的文件
     * 原始文件没一行为UserID::MovieID::Vote::datetime，如1::1193::5::978300760，表示用户1在978300760表示的那个时间给id为
     * 1193的电影评分5分。这不是mahout容易处理的格式，将分隔符'::'变为','，并且只留下UserID,MovieID两列，如1,1193。
     * 但是，为什么只留下两列？用户评分就这么扔掉了？也就是说这个例子是依据用户观看电影的历史记录进行推荐，而不是根据用户
     * 的喜好进行推荐
     * @throws IOException
     */
    private void createCsvRatingsFile() throws IOException {
        BufferedReader br=new BufferedReader(new FileReader(inputFile));
        BufferedWriter bw=new BufferedWriter(new FileWriter(outputFile));

        //line为读入的行，tmp[]存储读入行按照“::”进行分割的字符串数组，writeLine表示最终要写入输出文件的行
        String line=null;String writeLine=null;String tmp[];

        //读入文件，对每行进行遍历
        while ((line=br.readLine())!=null){
            tmp=line.split("::");
            writeLine=tmp[0]+","+tmp[1];
            bw.write(writeLine);
            bw.newLine();
            bw.flush();
        }

        //关闭流
        br.close();
        bw.close();
    }

    public DataModel createModel() throws IOException {
        //1. 将原始文件转换为mahout容易使用的文件
        createCsvRatingsFile();
        //2. 依据mahout易处理的文件建立模型
        File ratingsFile=new File(outputFile);
        return new FileDataModel(ratingsFile);
    }
}
