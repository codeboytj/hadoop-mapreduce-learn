# 《mahout实践指南》第5章学习

其中的LogisticModelParameters.java、RunLogistic.java以及TrainLogistic.java都来自[mahout官方github](https://github.com/apache/mahout/tree/master/examples/src/main/java/org/apache/mahout/classifier/sgd)的代码

这个例子做的只是分类预测，就是说将气体浓度值分为是否危险两类，利用模型进行预测是否危险。尴尬的是其实结果
只显示了模型的评估结果，并不知道要如何进行预测。