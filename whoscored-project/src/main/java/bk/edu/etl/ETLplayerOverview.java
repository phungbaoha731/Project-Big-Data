package bk.edu.etl;

import bk.edu.conf.ConfigName;
import bk.edu.utils.SparkUtil;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Serializable;

import static org.apache.spark.sql.functions.*;
public class ETLplayerOverview implements Serializable {
    private static SparkUtil sparkUtil;

    public ETLplayerOverview() {
        sparkUtil = new SparkUtil("who-scored", "save to hdfs", "yarn");
    }

    public Dataset<Row> playerOverview(){
        Dataset<Row> df = sparkUtil.getSparkSession().read().parquet(ConfigName.PLAYER_OVERVIEW + "/04-05-2023");
        Dataset<Row> dfTime = sparkUtil.getSparkSession().read().parquet(ConfigName.RESULT_MATCHES + "/04-05-2023")
                .select("Match_ID", "Date", "Home", "Away", "Score")
                .withColumnRenamed("Match_ID", "Match_ID2");

        df = df.na().fill(0);
        df.createOrReplaceTempView("overview");
        Dataset<Row> finalDf = df.select("Match_ID", "Tournament");
        finalDf = finalDf.join(dfTime, dfTime.col("Match_ID2").equalTo(finalDf.col("Match_ID")), "inner").drop("Match_ID2").distinct();



        Dataset<Row> maxValueDf = df.select("Match_ID","Player", "Performance_Gls");
        Dataset<Row> maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Performance_Gls").as("max_Performance_Gls"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueColumn.show();
        Dataset<Row> maxValueGroupBy = maxValueDf.join(maxValueColumn,
                maxValueDf.col("Performance_Gls").equalTo(maxValueColumn.col("max_Performance_Gls"))
                        .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Att").drop("Match_ID2")
                .groupBy("Match_ID", "max_Performance_Gls").agg(collect_list("Player").as("Player_max_Performance_Gls"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


                maxValueDf = df.select("Match_ID","Player", "Performance_Gls");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Performance_Gls").as("max_Performance_Gls"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Performance_Gls").equalTo(maxValueColumn.col("max_Performance_Gls"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Performance_Gls").drop("Match_ID2")
                .groupBy("Match_ID", "max_Performance_Gls").agg(collect_list("Player").as("Player_max_Performance_Gls"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Performance_Ast");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Performance_Ast").as("max_Performance_Ast"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Performance_Ast").equalTo(maxValueColumn.col("max_Performance_Ast"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Performance_Ast").drop("Match_ID2")
                .groupBy("Match_ID", "max_Performance_Ast").agg(collect_list("Player").as("Player_max_Performance_Ast"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Performance_PK");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Performance_PK").as("max_Performance_PK"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Performance_PK").equalTo(maxValueColumn.col("max_Performance_PK"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Performance_PK").drop("Match_ID2")
                .groupBy("Match_ID", "max_Performance_PK").agg(collect_list("Player").as("Player_max_Performance_PK"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Performance_PKatt");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Performance_PKatt").as("max_Performance_PKatt"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Performance_PKatt").equalTo(maxValueColumn.col("max_Performance_PKatt"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Performance_PKatt").drop("Match_ID2")
                .groupBy("Match_ID", "max_Performance_PKatt").agg(collect_list("Player").as("Player_max_Performance_PKatt"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Performance_Sh");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Performance_Sh").as("max_Performance_Sh"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Performance_Sh").equalTo(maxValueColumn.col("max_Performance_Sh"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Performance_Sh").drop("Match_ID2")
                .groupBy("Match_ID", "max_Performance_Sh").agg(collect_list("Player").as("Player_max_Performance_Sh"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Performance_SoT");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Performance_SoT").as("max_Performance_SoT"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Performance_SoT").equalTo(maxValueColumn.col("max_Performance_SoT"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Performance_SoT").drop("Match_ID2")
                .groupBy("Match_ID", "max_Performance_SoT").agg(collect_list("Player").as("Player_max_Performance_SoT"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Performance_CrdY");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Performance_CrdY").as("max_Performance_CrdY"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Performance_CrdY").equalTo(maxValueColumn.col("max_Performance_CrdY"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Performance_CrdY").drop("Match_ID2")
                .groupBy("Match_ID", "max_Performance_CrdY").agg(collect_list("Player").as("Player_max_Performance_CrdY"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Performance_CrdR");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Performance_CrdR").as("max_Performance_CrdR"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Performance_CrdR").equalTo(maxValueColumn.col("max_Performance_CrdR"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Performance_CrdR").drop("Match_ID2")
                .groupBy("Match_ID", "max_Performance_CrdR").agg(collect_list("Player").as("Player_max_Performance_CrdR"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Performance_Touches");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Performance_Touches").as("max_Performance_Touches"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Performance_Touches").equalTo(maxValueColumn.col("max_Performance_Touches"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Performance_Touches").drop("Match_ID2")
                .groupBy("Match_ID", "max_Performance_Touches").agg(collect_list("Player").as("Player_max_Performance_Touches"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Performance_Tkl");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Performance_Tkl").as("max_Performance_Tkl"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Performance_Tkl").equalTo(maxValueColumn.col("max_Performance_Tkl"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Performance_Tkl").drop("Match_ID2")
                .groupBy("Match_ID", "max_Performance_Tkl").agg(collect_list("Player").as("Player_max_Performance_Tkl"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Performance_Int");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Performance_Int").as("max_Performance_Int"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Performance_Int").equalTo(maxValueColumn.col("max_Performance_Int"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Performance_Int").drop("Match_ID2")
                .groupBy("Match_ID", "max_Performance_Int").agg(collect_list("Player").as("Player_max_Performance_Int"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Performance_Blocks");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Performance_Blocks").as("max_Performance_Blocks"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Performance_Blocks").equalTo(maxValueColumn.col("max_Performance_Blocks"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Performance_Blocks").drop("Match_ID2")
                .groupBy("Match_ID", "max_Performance_Blocks").agg(collect_list("Player").as("Player_max_Performance_Blocks"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Expected_xG");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Expected_xG").as("max_Expected_xG"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Expected_xG").equalTo(maxValueColumn.col("max_Expected_xG"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Expected_xG").drop("Match_ID2")
                .groupBy("Match_ID", "max_Expected_xG").agg(collect_list("Player").as("Player_max_Expected_xG"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Expected_npxG");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Expected_npxG").as("max_Expected_npxG"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Expected_npxG").equalTo(maxValueColumn.col("max_Expected_npxG"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Expected_npxG").drop("Match_ID2")
                .groupBy("Match_ID", "max_Expected_npxG").agg(collect_list("Player").as("Player_max_Expected_npxG"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Expected_xAG");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Expected_xAG").as("max_Expected_xAG"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Expected_xAG").equalTo(maxValueColumn.col("max_Expected_xAG"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Expected_xAG").drop("Match_ID2")
                .groupBy("Match_ID", "max_Expected_xAG").agg(collect_list("Player").as("Player_max_Expected_xAG"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "SCA");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("SCA").as("max_SCA"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("SCA").equalTo(maxValueColumn.col("max_SCA"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("SCA").drop("Match_ID2")
                .groupBy("Match_ID", "max_SCA").agg(collect_list("Player").as("Player_max_SCA"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "GCA");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("GCA").as("max_GCA"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("GCA").equalTo(maxValueColumn.col("max_GCA"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("GCA").drop("Match_ID2")
                .groupBy("Match_ID", "max_GCA").agg(collect_list("Player").as("Player_max_GCA"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Passes_Cmp");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Passes_Cmp").as("max_Passes_Cmp"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Passes_Cmp").equalTo(maxValueColumn.col("max_Passes_Cmp"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Passes_Cmp").drop("Match_ID2")
                .groupBy("Match_ID", "max_Passes_Cmp").agg(collect_list("Player").as("Player_max_Passes_Cmp"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Passes_Att");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Passes_Att").as("max_Passes_Att"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Passes_Att").equalTo(maxValueColumn.col("max_Passes_Att"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Passes_Att").drop("Match_ID2")
                .groupBy("Match_ID", "max_Passes_Att").agg(collect_list("Player").as("Player_max_Passes_Att"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Passes_Cmp%");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Passes_Cmp%").as("max_Passes_Cmp%"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Passes_Cmp%").equalTo(maxValueColumn.col("max_Passes_Cmp%"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Passes_Cmp%").drop("Match_ID2")
                .groupBy("Match_ID", "max_Passes_Cmp%").agg(collect_list("Player").as("Player_max_Passes_Cmp%"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Prog");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Prog").as("max_Prog"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Prog").equalTo(maxValueColumn.col("max_Prog"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Prog").drop("Match_ID2")
                .groupBy("Match_ID", "max_Prog").agg(collect_list("Player").as("Player_max_Prog"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Passes_Succ");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Passes_Succ").as("max_Passes_Succ"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Passes_Succ").equalTo(maxValueColumn.col("max_Passes_Succ"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Passes_Succ").drop("Match_ID2")
                .groupBy("Match_ID", "max_Passes_Succ").agg(collect_list("Player").as("Player_max_Passes_Succ"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Dribbles_Att");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Dribbles_Att").as("max_Dribbles_Att"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Dribbles_Att").equalTo(maxValueColumn.col("max_Dribbles_Att"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Dribbles_Att").drop("Match_ID2")
                .groupBy("Match_ID", "max_Dribbles_Att").agg(collect_list("Player").as("Player_max_Dribbles_Att"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();





        maxValueDf = df.select("Match_ID","Player", "Receiving_Rec");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Receiving_Rec").as("max_Receiving_Rec"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Receiving_Rec").equalTo(maxValueColumn.col("max_Receiving_Rec"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Receiving_Rec").drop("Match_ID2")
                .groupBy("Match_ID", "max_Receiving_Rec").agg(collect_list("Player").as("Player_max_Receiving_Rec"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Receiving_Prog");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Receiving_Prog").as("max_Receiving_Prog"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Receiving_Prog").equalTo(maxValueColumn.col("max_Receiving_Prog"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Receiving_Prog").drop("Match_ID2")
                .groupBy("Match_ID", "max_Receiving_Prog").agg(collect_list("Player").as("Player_max_Receiving_Prog"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        finalDf.show();
        finalDf.printSchema();

        return finalDf;
    }

    public void convertMaxPlayerOverview(Dataset<Row> df){
        StructType structChild = DataTypes.createStructType(
                new StructField[] {
                        DataTypes.createStructField("Player", DataTypes.createArrayType(DataTypes.StringType), false),
                        DataTypes.createStructField("score", DataTypes.IntegerType, false)
                }
        );
        StructType structDoubleChild = DataTypes.createStructType(
                new StructField[] {
                        DataTypes.createStructField("Player", DataTypes.createArrayType(DataTypes.StringType), false),
                        DataTypes.createStructField("score", DataTypes.DoubleType, false)
                }
        );
        StructType struct = DataTypes.createStructType(
                new StructField[] {
                        DataTypes.createStructField("Match_ID", DataTypes.StringType, false),
                        DataTypes.createStructField("Tournament", DataTypes.StringType, false),
                        DataTypes.createStructField("Date", DataTypes.StringType, false),
                        DataTypes.createStructField("Home", DataTypes.StringType, false),
                        DataTypes.createStructField("Away", DataTypes.StringType, false),
                        DataTypes.createStructField("Score", DataTypes.StringType, false),
                        DataTypes.createStructField("max_Performance_Gls",structChild , false),
                        DataTypes.createStructField("max_Performance_Ast",structChild , false),
                        DataTypes.createStructField("max_Performance_PK",structChild , false),
                        DataTypes.createStructField("max_Performance_PKatt",structChild , false),
                        DataTypes.createStructField("max_Performance_Sh",structChild , false),
                        DataTypes.createStructField("max_Performance_SoT",structChild , false),
                        DataTypes.createStructField("max_Performance_CrdY",structChild , false),
                        DataTypes.createStructField("max_Performance_CrdR",structChild , false),
                        DataTypes.createStructField("max_Performance_Touches",structChild , false),
                        DataTypes.createStructField("max_Performance_Tkl",structDoubleChild , false),
                        DataTypes.createStructField("max_Performance_Int",structChild , false),
                        DataTypes.createStructField("max_Performance_Blocks",structChild , false),
                        DataTypes.createStructField("max_Expected_xG",structChild , false),
                        DataTypes.createStructField("max_Expected_npxG",structChild , false),
                        DataTypes.createStructField("max_Expected_xAG",structChild , false),
                        DataTypes.createStructField("max_SCA",structChild , false),
                        DataTypes.createStructField("max_GCA",structChild , false),
                        DataTypes.createStructField("max_Passes_Cmp",structChild , false),
                        DataTypes.createStructField("max_Passes_Att",structChild , false),
                        DataTypes.createStructField("max_Passes_Cmp%",structChild , false),
                        DataTypes.createStructField("max_Prog",structChild , false),
                        DataTypes.createStructField("max_Passes_Succ",structChild , false),
                        DataTypes.createStructField("max_Dribbles_Att",structChild , false)

                }
        );
        Dataset<Row> dfFinal = df.map(new MapFunction<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), v1.getString(1),
                        v1.getString(2), v1.getString(3), v1.getString(4),
                        v1.getString(5),
                        RowFactory.create(v1.getSeq(7), v1.getInt(6)),
                        RowFactory.create(v1.getSeq(9), v1.getInt(8)),
                        RowFactory.create(v1.getSeq(11), v1.getInt(10)),
                        RowFactory.create(v1.getSeq(13), v1.getInt(12)),
                        RowFactory.create(v1.getSeq(15), v1.getInt(14)),
                        RowFactory.create(v1.getSeq(17), v1.getInt(16)),
                        RowFactory.create(v1.getSeq(19), v1.getInt(18)),
                        RowFactory.create(v1.getSeq(21), v1.getInt(20)),
                        RowFactory.create(v1.getSeq(23), v1.getInt(22)),
                        RowFactory.create(v1.getSeq(25), v1.getInt(24)),
                        RowFactory.create(v1.getSeq(27), v1.getInt(26)),
                        RowFactory.create(v1.getSeq(29), v1.getInt(28)),
                        RowFactory.create(v1.getSeq(31), v1.getDouble(30)),
                        RowFactory.create(v1.getSeq(33), v1.getDouble(32)),
                        RowFactory.create(v1.getSeq(35), v1.getDouble(34)),
                        RowFactory.create(v1.getSeq(37), v1.getInt(36)),
                        RowFactory.create(v1.getSeq(39), v1.getInt(38)),
                        RowFactory.create(v1.getSeq(41), v1.getInt(40)),
                        RowFactory.create(v1.getSeq(43), v1.getInt(42)),
                        RowFactory.create(v1.getSeq(45), v1.getDouble(44)),
                        RowFactory.create(v1.getSeq(47), v1.getInt(46)),
                        RowFactory.create(v1.getSeq(49), v1.getInt(48)),
                        RowFactory.create(v1.getSeq(51), v1.getInt(50)));

            }
        }, RowEncoder.apply(struct));
        dfFinal.write().mode("overwrite").parquet("max" + ConfigName.PLAYER_OVERVIEW + "/04-05-2023");
    }

    public static void main(String[] args){
        ETLplayerOverview etl = new ETLplayerOverview();
        Dataset<Row> playerOverview = etl.playerOverview();
        // etl.convertMaxGkOverview(gkOverview);
    }
}
