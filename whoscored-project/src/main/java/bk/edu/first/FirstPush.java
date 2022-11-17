package bk.edu.first;

import bk.edu.conf.ConfigName;
import bk.edu.utils.SparkUtil;
import bk.edu.utils.TimeUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;

public class FirstPush implements Serializable {
    private static SparkUtil sparkUtil;

    public FirstPush() {
        sparkUtil = new SparkUtil("who-scored", "save to hdfs", "yarn");
    }

    private void saveData(){
        Dataset<Row> df = sparkUtil.getSparkSession().read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("file:///Data.csv");
        df.printSchema();
        df.show(3);
        df.write().parquet(ConfigName.PATH_RAW_DATA + "/" + TimeUtil.getDate(ConfigName.FORMAT_TIME));
    }
    public static void main(String[] args){
        FirstPush firstPush = new FirstPush();
        firstPush.saveData();
    }
}
