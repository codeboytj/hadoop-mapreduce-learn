package cumt.tj.learn.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Created by sky on 17-6-1.
 * Java操作Hbase实例
 */
public class CreateHBaseTable {
    public static void main(String[] args) throws IOException {
        Configuration conf= HBaseConfiguration.create();

        //创建表格
        //表名，列族名
        String tableName=args[0];String familyName=args[1];
        Admin admin= ConnectionFactory.createConnection(conf).getAdmin();
        createTable(conf,admin,tableName,familyName);

    }

    public static void createTable(Configuration configuration,Admin admin,String tableName,String familyName) throws IOException {

        TableName tName= TableName.valueOf(tableName);
        if(admin.tableExists(tName)){
            //如果表存在，先删除
            admin.disableTable(tName);
            admin.deleteTable(tName);
        }

        //设置新表格
        HTableDescriptor htd=new HTableDescriptor(tName);
        HColumnDescriptor hcd=new HColumnDescriptor(familyName);
        //增加新列族
        htd.addFamily(hcd);

        //新增表格
        admin.createTable(htd);
    }
}
