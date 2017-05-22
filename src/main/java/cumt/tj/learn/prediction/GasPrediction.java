package cumt.tj.learn.prediction;

import cumt.tj.learn.util.arima.ARIMA;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;
/**
 * Created by sky on 17-5-22.
 * 这是一个利用时间序列预测模型ARIMA，预测危险气体浓度的例子，Java版本的实现，原版代码取自github：https://github.com/AdairZhao/ARIMA
 * 输入文件中数据是气体浓度的时间序列
 * 预测结果通过控制台打印出来
 * 这个并没有用到hadoop平台的东西，是简单的预测算法利用。
 */
public class GasPrediction {
    public static void main(String[] args) {

        Scanner ino=null;

        try {
            ArrayList<Double> arraylist=new ArrayList<Double>();
            ino=new Scanner(new File("inputs/GasPrediction/gas"));
            while(ino.hasNext())
            {
                arraylist.add(Double.parseDouble(ino.next()));
            }
            double[] dataArray=new double[arraylist.size()-1];
            for(int i=0;i<arraylist.size()-1;i++)
                dataArray[i]=arraylist.get(i);

            //System.out.println(arraylist.size());

            ARIMA arima=new ARIMA(dataArray);

            int []model=arima.getARIMAmodel();
            System.out.println("Best model is [p,q]="+"["+model[0]+" "+model[1]+"]");
            System.out.println("Predict value="+arima.aftDeal(arima.predictValue(model[0],model[1])));
            System.out.println("Predict error="+(arima.aftDeal(arima.predictValue(model[0],model[1]))-arraylist.get(arraylist.size()-1))/arraylist.get(arraylist.size()-1)*100+"%");

            //	String[] str = (String[])list1.toArray(new String[0]);

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally{
            ino.close();
        }
    }
}
