# 预测

## 气体浓度最大值的预测

程序GasPrediction.java，利用mapreduce函数以及时间序列预测模型ARIMA，预测危险气体浓度的例子，大体步骤如下：

1. 程序通过mapreduce从输入文件中计算出每天气体浓度的最大值并输出到文件之中，[输入文件](https://github.com/codeboytj/hadoop-mapreduce-learn/blob/master/inputs/GasPrediction/dataPerDay)中数据是气体浓度的时间序列key/value对，key表示日期，value表示气体浓度乘以100以后的值，比如第一行(1,16)表示在1号那天监测到气体浓度为0.16%，每天检测到的数据不止一个
2. 计算出最大值之后，程序通过arima模型对未来一天的气体浓度最大值进行预测
3. 预测结果通过控制台打印出来

程序结果显示预测22日的气体浓度最大值为0.17%，预测误差为0.00%，效果还是很好的
