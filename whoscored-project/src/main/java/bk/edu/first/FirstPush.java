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

    private void pushData(){
        Dataset<Row> df = sparkUtil.getSparkSession().read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("file:/inputfile/gkOverview.csv");
        df.printSchema();
        df.show(3);
        df.write().mode("overwrite").parquet("/user/" +ConfigName.GK_OVERVIEW +"/2023-02-08");

        df = sparkUtil.getSparkSession().read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("file:/inputfile/playerDefensive.csv");
        df.printSchema();
        df.show(3);
        df.write().mode("overwrite").parquet("/user/" +ConfigName.PLAYER_DEFENSIVE +"/2023-02-08");

        df = sparkUtil.getSparkSession().read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("file:/inputfile/playerMiscellaneous.csv");
        df.printSchema();
        df.show(3);
        df.write().mode("overwrite").parquet("/user/" +ConfigName.PLAYER_MISCELLANEOUS +"/2023-02-08");

        df = sparkUtil.getSparkSession().read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("file:/inputfile/playerOverview.csv");
        df.printSchema();
        df.show(3);
        df.write().mode("overwrite").parquet("/user/" +ConfigName.PLAYER_OVERVIEW +"/2023-02-08");
        df = sparkUtil.getSparkSession().read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("file:/inputfile/playerPassing.csv");
        df.printSchema();
        df.show(3);
        df.write().mode("overwrite").parquet("/user/" +ConfigName.PLAYER_PASSING +"/2023-02-08");

        df = sparkUtil.getSparkSession().read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("file:/inputfile/playerPassType.csv");
        df.printSchema();
        df.show(3);
        df.write().mode("overwrite").parquet("/user/" +ConfigName.PLAYER_PASS_TYPE +"/2023-02-08");

        df = sparkUtil.getSparkSession().read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("file:/inputfile/playerPossession.csv");
        df.printSchema();
        df.show(3);
        df.write().mode("overwrite").parquet("/user/" +ConfigName.PLAYER_POSSESSION +"/2023-02-08");

        df = sparkUtil.getSparkSession().read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("file:/inputfile/shots.csv");
        df.printSchema();
        df.show(3);
        df.write().mode("overwrite").parquet("/user/" +ConfigName.SHOT +"/2023-02-08");

        df = sparkUtil.getSparkSession().read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("file:/inputfile/teamOverview.csv");
        df.printSchema();
        df.show(3);
        df.write().mode("overwrite").parquet("/user/" +ConfigName.TEAM_OVERVIEW +"/2023-02-08");


        df = sparkUtil.getSparkSession().read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("file:/inputfile/result_matches.csv");
        df.printSchema();
        df.show(3);
        df.write().mode("overwrite").parquet("/user/" +ConfigName.RESULT_MATCHES +"/2023-02-08");


    }
    public static void main(String[] args){
        FirstPush firstPush = new FirstPush();
        firstPush.pushData();
    }
}