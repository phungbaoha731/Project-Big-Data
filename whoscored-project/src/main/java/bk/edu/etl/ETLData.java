package bk.edu.etl;

import bk.edu.conf.ConfigName;
import bk.edu.utils.SparkUtil;
import bk.edu.utils.TimeUtil;
import org.apache.spark.internal.config.R;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

import java.io.Serializable;

import static org.apache.spark.sql.functions.*;
public class ETLData implements Serializable {
    private static SparkUtil sparkUtil;

    public ETLData() {
        sparkUtil = new SparkUtil("who-scored", "save to hdfs", "yarn");
    }

    public Dataset<Row> gkOverView(){
        Dataset<Row> df = sparkUtil.getSparkSession().read().parquet(ConfigName.GK_OVERVIEW + "/04-05-2023");
        Dataset<Row> dfTime = sparkUtil.getSparkSession().read().parquet(ConfigName.RESULT_MATCHES + "/04-05-2023")
                .select("Match_ID", "Date", "Home", "Away", "Score")
                .withColumnRenamed("Match_ID", "Match_ID2");

        df = df.na().fill(0);
        df.createOrReplaceTempView("overview");
        Dataset<Row> finalDf = df.select("Match_ID", "Tournament");
        finalDf = finalDf.join(dfTime, dfTime.col("Match_ID2").equalTo(finalDf.col("Match_ID")), "inner").drop("Match_ID2").distinct();

        Dataset<Row> maxValueDf = df.select("Match_ID","Player", "Shot_Stopping_SoTA");
        Dataset<Row> maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Shot_Stopping_SoTA").as("max_Shot_Stopping_SoTA"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueColumn.show();
        Dataset<Row> maxValueGroupBy = maxValueDf.join(maxValueColumn,
                maxValueDf.col("Shot_Stopping_SoTA").equalTo(maxValueColumn.col("max_Shot_Stopping_SoTA"))
                        .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Shot_Stopping_SoTA").drop("Match_ID2")
                .groupBy("Match_ID", "max_Shot_Stopping_SoTA").agg(collect_list("Player").as("Player_max_Shot_Stopping_SoTA"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();

        maxValueDf = df.select("Match_ID","Player", "Shot_Stopping_GA");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Shot_Stopping_GA").as("max_Shot_Stopping_GA"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Shot_Stopping_GA").equalTo(maxValueColumn.col("max_Shot_Stopping_GA"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Shot_Stopping_GA").drop("Match_ID2")
                .groupBy("Match_ID", "max_Shot_Stopping_GA").agg(collect_list("Player").as("Player_max_Shot_Stopping_GA"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();

        maxValueDf = df.select("Match_ID","Player", "Shot_Stopping_Saves");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Shot_Stopping_Saves").as("max_Shot_Stopping_Saves"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Shot_Stopping_Saves").equalTo(maxValueColumn.col("max_Shot_Stopping_Saves"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Shot_Stopping_Saves").drop("Match_ID2")
                .groupBy("Match_ID", "max_Shot_Stopping_Saves").agg(collect_list("Player").as("Player_max_Shot_Stopping_Saves"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Shot_Stopping_Save%");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Shot_Stopping_Save%").as("max_Shot_Stopping_Save%"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Shot_Stopping_Save%").equalTo(maxValueColumn.col("max_Shot_Stopping_Save%"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Shot_Stopping_Save%").drop("Match_ID2")
                .groupBy("Match_ID", "max_Shot_Stopping_Save%").agg(collect_list("Player").as("Player_max_Shot_Stopping_Save%"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();
//
        maxValueDf = df.select("Match_ID","Player", "Shot_Stopping_PSxG");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Shot_Stopping_PSxG").as("max_Shot_Stopping_PSxG"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Shot_Stopping_PSxG").equalTo(maxValueColumn.col("max_Shot_Stopping_PSxG"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Shot_Stopping_PSxG").drop("Match_ID2")
                .groupBy("Match_ID", "max_Shot_Stopping_PSxG").agg(collect_list("Player").as("Player_max_Shot_Stopping_PSxG"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();

        maxValueDf = df.select("Match_ID","Player", "Launched_Cmp");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Launched_Cmp").as("max_Launched_Cmp"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Launched_Cmp").equalTo(maxValueColumn.col("max_Launched_Cmp"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Launched_Cmp").drop("Match_ID2")
                .groupBy("Match_ID", "max_Launched_Cmp").agg(collect_list("Player").as("Player_max_Launched_Cmp"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();

        maxValueDf = df.select("Match_ID","Player", "Launched_Att");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Launched_Att").as("max_Launched_Att"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Launched_Att").equalTo(maxValueColumn.col("max_Launched_Att"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Launched_Att").drop("Match_ID2")
                .groupBy("Match_ID", "max_Launched_Att").agg(collect_list("Player").as("Player_max_Launched_Att"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();

        maxValueDf = df.select("Match_ID","Player", "Launched_Cmp%");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Launched_Cmp%").as("max_Launched_Cmp%"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Launched_Cmp%").equalTo(maxValueColumn.col("max_Launched_Cmp%"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Launched_Cmp%").drop("Match_ID2")
                .groupBy("Match_ID", "max_Launched_Cmp%").agg(collect_list("Player").as("Player_max_Launched_Cmp%"))
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


        maxValueDf = df.select("Match_ID","Player", "Passes_Thr");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Passes_Thr").as("max_Passes_Thr"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Passes_Thr").equalTo(maxValueColumn.col("max_Passes_Thr"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Passes_Thr").drop("Match_ID2")
                .groupBy("Match_ID", "max_Passes_Thr").agg(collect_list("Player").as("Player_max_Passes_Thr"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();

        maxValueDf = df.select("Match_ID","Player", "Passes_Launch%");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Passes_Launch%").as("max_Passes_Launch%"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Passes_Launch%").equalTo(maxValueColumn.col("max_Passes_Launch%"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Passes_Launch%").drop("Match_ID2")
                .groupBy("Match_ID", "max_Passes_Launch%").agg(collect_list("Player").as("Player_max_Passes_Launch%"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Passes_AvgLen");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Passes_AvgLen").as("max_Passes_AvgLen"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Passes_AvgLen").equalTo(maxValueColumn.col("max_Passes_AvgLen"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Passes_AvgLen").drop("Match_ID2")
                .groupBy("Match_ID", "max_Passes_AvgLen").agg(collect_list("Player").as("Player_max_Passes_AvgLen"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();

        maxValueDf = df.select("Match_ID","Player", "Goal_Kicks_Att");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Goal_Kicks_Att").as("max_Goal_Kicks_Att"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Goal_Kicks_Att").equalTo(maxValueColumn.col("max_Goal_Kicks_Att"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Goal_Kicks_Att").drop("Match_ID2")
                .groupBy("Match_ID", "max_Goal_Kicks_Att").agg(collect_list("Player").as("Player_max_Goal_Kicks_Att"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").
                        equalTo(maxValueGroupBy.col("Match_ID2")), "inner")
                .drop("Match_ID2").distinct();

        maxValueDf = df.select("Match_ID","Player", "Goal_Kicks_Launch%");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Goal_Kicks_Launch%").as("max_Goal_Kicks_Launch%"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Goal_Kicks_Launch%").equalTo(maxValueColumn.col("max_Goal_Kicks_Launch%"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Goal_Kicks_Launch%").drop("Match_ID2")
                .groupBy("Match_ID", "max_Goal_Kicks_Launch%").agg(collect_list("Player").as("Player_max_Goal_Kicks_Launch%"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").
                        equalTo(maxValueGroupBy.col("Match_ID2")), "inner")
                .drop("Match_ID2").distinct();

        maxValueDf = df.select("Match_ID","Player", "Goal_Kicks_AvgLen");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Goal_Kicks_AvgLen").as("max_Goal_Kicks_AvgLen"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Goal_Kicks_AvgLen").equalTo(maxValueColumn.col("max_Goal_Kicks_AvgLen"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Goal_Kicks_AvgLen").drop("Match_ID2")
                .groupBy("Match_ID", "max_Goal_Kicks_AvgLen").agg(collect_list("Player").as("Player_max_Goal_Kicks_AvgLen"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").
                        equalTo(maxValueGroupBy.col("Match_ID2")), "inner")
                .drop("Match_ID2").distinct();

        maxValueDf = df.select("Match_ID","Player", "Crosses_Opp");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Crosses_Opp").as("max_Crosses_Opp"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Crosses_Opp").equalTo(maxValueColumn.col("max_Crosses_Opp"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Crosses_Opp").drop("Match_ID2")
                .groupBy("Match_ID", "max_Crosses_Opp").agg(collect_list("Player").as("Player_max_Crosses_Opp"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").
                        equalTo(maxValueGroupBy.col("Match_ID2")), "inner")
                .drop("Match_ID2").distinct();

        maxValueDf = df.select("Match_ID","Player", "Crosses_Stp");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Crosses_Stp").as("max_Crosses_Stp"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Crosses_Stp").equalTo(maxValueColumn.col("max_Crosses_Stp"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Crosses_Stp").drop("Match_ID2")
                .groupBy("Match_ID", "max_Crosses_Stp").agg(collect_list("Player").as("Player_max_Crosses_Stp"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").
                        equalTo(maxValueGroupBy.col("Match_ID2")), "inner")
                .drop("Match_ID2").distinct();

        maxValueDf = df.select("Match_ID","Player", "Crosses_Stp%");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Crosses_Stp%").as("max_Crosses_Stp%"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Crosses_Stp%").equalTo(maxValueColumn.col("max_Crosses_Stp%"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Crosses_Stp%").drop("Match_ID2")
                .groupBy("Match_ID", "max_Crosses_Stp%").agg(collect_list("Player").as("Player_max_Crosses_Stp%"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").
                        equalTo(maxValueGroupBy.col("Match_ID2")), "inner")
                .drop("Match_ID2").distinct();

        maxValueDf = df.select("Match_ID","Player", "Sweeper_#OPA");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Sweeper_#OPA").as("max_Sweeper_#OPA"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Sweeper_#OPA").equalTo(maxValueColumn.col("max_Sweeper_#OPA"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Sweeper_#OPA").drop("Match_ID2")
                .groupBy("Match_ID", "max_Sweeper_#OPA").agg(collect_list("Player").as("Player_max_Sweeper_#OPA"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").
                        equalTo(maxValueGroupBy.col("Match_ID2")), "inner")
                .drop("Match_ID2").distinct();

        maxValueDf = df.select("Match_ID","Player", "Sweeper_AvgDist");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Sweeper_AvgDist").as("max_Sweeper_AvgDist"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Sweeper_AvgDist").equalTo(maxValueColumn.col("max_Sweeper_AvgDist"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Sweeper_AvgDist").drop("Match_ID2")
                .groupBy("Match_ID", "max_Sweeper_AvgDist").agg(collect_list("Player").as("Player_max_Sweeper_AvgDist"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").
                        equalTo(maxValueGroupBy.col("Match_ID2")), "inner")
                .drop("Match_ID2").distinct();


        finalDf.show();
        finalDf.printSchema();

        return finalDf;
    }

    public void convertMaxGkOverview(Dataset<Row> df){
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
                        DataTypes.createStructField("max_Shot_Stopping_SoTA",structChild , false),
                        DataTypes.createStructField("max_Shot_Stopping_GA",structChild , false),
                        DataTypes.createStructField("max_Shot_Stopping_Saves",structChild , false),
                        DataTypes.createStructField("max_Shot_Stopping_Save%",structDoubleChild , false),
                        DataTypes.createStructField("max_Shot_Stopping_PSxG",structDoubleChild , false),
                        DataTypes.createStructField("max_Launched_Cmp",structChild , false),
                        DataTypes.createStructField("max_Launched_Att",structChild , false),
                        DataTypes.createStructField("max_Launched_Cmp%",structDoubleChild , false),
                        DataTypes.createStructField("max_Passes_Att",structChild , false),
                        DataTypes.createStructField("max_Passes_Thr",structChild , false),
                        DataTypes.createStructField("max_Passes_Launch%",structDoubleChild , false),
                        DataTypes.createStructField("max_Passes_AvgLen",structDoubleChild , false),
                        DataTypes.createStructField("max_Goal_Kicks_Att",structChild , false),
                        DataTypes.createStructField("max_Goal_Kicks_Launch%",structDoubleChild , false),
                        DataTypes.createStructField("max_Goal_Kicks_AvgLen",structDoubleChild , false),
                        DataTypes.createStructField("max_Crosses_Opp",structChild , false),
                        DataTypes.createStructField("max_Crosses_Stp",structChild , false),
                        DataTypes.createStructField("max_Crosses_Stp%",structDoubleChild , false),
                        DataTypes.createStructField("max_Sweeper_#OPA",structChild , false),
                        DataTypes.createStructField("max_Sweeper_AvgDist",structDoubleChild , false)
                }
        );
        Dataset<Row> dfFinal = df.map(new Function1<Row, Row>() {
            @Override
            public Row apply(Row v1) {
                return RowFactory.create(v1.getString(0), v1.getString(1),
                        v1.getString(2), v1.getString(3), v1.getString(4),
                        v1.getString(5),
                        RowFactory.create(v1.getSeq(7), v1.getInt(6)),
                        RowFactory.create(v1.getSeq(9), v1.getInt(8)),
                        RowFactory.create(v1.getSeq(11), v1.getInt(10)),
                        RowFactory.create(v1.getSeq(13), v1.getDouble(12)),
                        RowFactory.create(v1.getSeq(15), v1.getDouble(14)),
                        RowFactory.create(v1.getSeq(17), v1.getInt(16)),
                        RowFactory.create(v1.getSeq(19), v1.getInt(18)),
                        RowFactory.create(v1.getSeq(21), v1.getDouble(20)),
                        RowFactory.create(v1.getSeq(23), v1.getInt(22)),
                        RowFactory.create(v1.getSeq(25), v1.getInt(24)),
                        RowFactory.create(v1.getSeq(27), v1.getDouble(26)),
                        RowFactory.create(v1.getSeq(29), v1.getDouble(28)),
                        RowFactory.create(v1.getSeq(31), v1.getInt(30)),
                        RowFactory.create(v1.getSeq(33), v1.getDouble(32)),
                        RowFactory.create(v1.getSeq(35), v1.getDouble(34)),
                        RowFactory.create(v1.getSeq(37), v1.getInt(36)),
                        RowFactory.create(v1.getSeq(39), v1.getInt(38)),
                        RowFactory.create(v1.getSeq(41), v1.getDouble(40)),
                        RowFactory.create(v1.getSeq(43), v1.getInt(42)),
                        RowFactory.create(v1.getSeq(45), v1.getDouble(44)));
            }
        }, RowEncoder.apply(struct));
        dfFinal.write().mode("overwrite").parquet("max" + ConfigName.GK_OVERVIEW + "/04-05-2023");
    }

    public static void main(String[] args){
        ETLData etl = new ETLData();
        Dataset<Row> gkOverview = etl.gkOverView();
        etl.convertMaxGkOverview(gkOverview);
    }
}
