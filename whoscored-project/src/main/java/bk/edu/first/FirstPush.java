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
        df.write().parquet(ConfigName.GK_OVERVIEW + "/" + TimeUtil.getDate(ConfigName.FORMAT_TIME));

        df = sparkUtil.getSparkSession().read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("file:/inputfile/playerDefensive.csv");
        df.printSchema();
        df.show(3);
        df.write().parquet(ConfigName.PLAYER_DEFENSIVE + "/" + TimeUtil.getDate(ConfigName.FORMAT_TIME));

        df = sparkUtil.getSparkSession().read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("file:/inputfile/playerMiscellaneous.csv");
        df.printSchema();
        df.show(3);
        df.write().parquet(ConfigName.PLAYER_MISCELLANEOUS + "/" + TimeUtil.getDate(ConfigName.FORMAT_TIME));

        df = sparkUtil.getSparkSession().read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("file:/inputfile/playerOverview.csv");
        df.printSchema();
        df.show(3);
        df.write().parquet(ConfigName.PLAYER_OVERVIEW + "/" + TimeUtil.getDate(ConfigName.FORMAT_TIME));
        df = sparkUtil.getSparkSession().read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("file:/inputfile/playerPassing.csv");
        df.printSchema();
        df.show(3);
        df.write().parquet(ConfigName.PLAYER_PASSING + "/" + TimeUtil.getDate(ConfigName.FORMAT_TIME));

        df = sparkUtil.getSparkSession().read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("file:/inputfile/playerPassType.csv");
        df.printSchema();
        df.show(3);
        df.write().parquet(ConfigName.PLAYER_PASS_TYPE+ "/" + TimeUtil.getDate(ConfigName.FORMAT_TIME));

        df = sparkUtil.getSparkSession().read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("file:/inputfile/playerPossession.csv");
        df.printSchema();
        df.show(3);
        df.write().parquet(ConfigName.PLAYER_POSSESSION + "/" + TimeUtil.getDate(ConfigName.FORMAT_TIME));

        df = sparkUtil.getSparkSession().read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("file:/inputfile/shots.csv");
        df.printSchema();
        df.show(3);
        df.write().parquet(ConfigName.SHOT + "/" + TimeUtil.getDate(ConfigName.FORMAT_TIME));

        df = sparkUtil.getSparkSession().read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("file:/inputfile/teamOverview.csv");
        df.printSchema();
        df.show(3);
        df.write().parquet(ConfigName.TEAM_OVERVIEW + "/" + TimeUtil.getDate(ConfigName.FORMAT_TIME));
        
        df = sparkUtil.getSparkSession().read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("file:/inputfile/result_matches.csv");
        df.printSchema();
        df.show(3);
        df.write().parquet(ConfigName.RESULT_MATCHES + "/" + TimeUtil.getDate(ConfigName.FORMAT_TIME));


    }
    public static void main(String[] args){
        FirstPush firstPush = new FirstPush();
        firstPush.pushData();
    }
}
