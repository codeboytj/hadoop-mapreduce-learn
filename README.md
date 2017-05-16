# hadoop-mapreduce-learn

## wordcount初试

参照这位大神的博客http://blog.csdn.net/napoay/article/details/68491469

### idea运行配置

修改运行配置，加入参数，在idea菜单栏依次点击：RUN->EditConfiguration->WordCount->program arguments，输入

```
input/
output
```

这样，程序会读取根目录input文件夹下的文件，进行计算之后，将结果输出到根目录的output文件夹

### wordcount疑问

1. 行中如果出现标点符号会计算错误
2. 可不可以不输出到文件，直接获取输出数据打印到控制台?