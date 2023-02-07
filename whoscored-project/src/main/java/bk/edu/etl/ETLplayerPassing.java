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

import java.io.Serializable;

import static org.apache.spark.sql.functions.*;
public class ETLplayerPassing implements Serializable {
    private static SparkUtil sparkUtil;

    public ETLplayerPassing() {
        sparkUtil = new SparkUtil("who-scored", "save to hdfs", "yarn");
    }

    public Dataset<Row> playerPassing(){
        Dataset<Row> df = sparkUtil.getSparkSession().read().parquet(ConfigName.PLAYER_PASSING + "/04-05-2023");
        Dataset<Row> dfTime = sparkUtil.getSparkSession().read().parquet(ConfigName.RESULT_MATCHES + "/04-05-2023")
                .select("Match_ID", "Date", "Home", "Away", "Score")
                .withColumnRenamed("Match_ID", "Match_ID2");

        df = df.na().fill(0);
        df.createOrReplaceTempView("overview");
        Dataset<Row> finalDf = df.select("Match_ID", "Tournament");
        finalDf = finalDf.join(dfTime, dfTime.col("Match_ID2").equalTo(finalDf.col("Match_ID")), "inner").drop("Match_ID2").distinct();



        Dataset<Row> maxValueDf = df.select("Match_ID","Player", "Total_Cmp");
        Dataset<Row> maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Total_Cmp").as("max_Total_Cmp"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueColumn.show();
        Dataset<Row> maxValueGroupBy = maxValueDf.join(maxValueColumn,
                maxValueDf.col("Total_Cmp").equalTo(maxValueColumn.col("max_Total_Cmp"))
                        .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Total_Cmp").drop("Match_ID2")
                .groupBy("Match_ID", "max_Total_Cmp").agg(collect_list("Player").as("Player_max_Total_Cmp"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();



        maxValueDf = df.select("Match_ID","Player", "Total_Cmp");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Total_Cmp").as("max_Total_Cmp"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Total_Cmp").equalTo(maxValueColumn.col("max_Total_Cmp"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Total_Cmp").drop("Match_ID2")
                .groupBy("Match_ID", "max_Total_Cmp").agg(collect_list("Player").as("Player_max_Total_Cmp"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Total_Att");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Total_Att").as("max_Total_Att"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Total_Att").equalTo(maxValueColumn.col("max_Total_Att"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Total_Att").drop("Match_ID2")
                .groupBy("Match_ID", "max_Total_Att").agg(collect_list("Player").as("Player_max_Total_Att"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Total_Cmp%");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Total_Cmp%").as("max_Total_Cmp%"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Total_Cmp%").equalTo(maxValueColumn.col("max_Total_Cmp%"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Total_Cmp%").drop("Match_ID2")
                .groupBy("Match_ID", "max_Total_Cmp%").agg(collect_list("Player").as("Player_max_Total_Cmp%"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Total_TotDist");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Total_TotDist").as("max_Total_TotDist"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Total_TotDist").equalTo(maxValueColumn.col("max_Total_TotDist"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Total_TotDist").drop("Match_ID2")
                .groupBy("Match_ID", "max_Total_TotDist").agg(collect_list("Player").as("Player_max_Total_TotDist"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "PrgDist");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("PrgDist").as("max_PrgDist"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("PrgDist").equalTo(maxValueColumn.col("max_PrgDist"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("PrgDist").drop("Match_ID2")
                .groupBy("Match_ID", "max_PrgDist").agg(collect_list("Player").as("Player_max_PrgDist"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Short_Cmp");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Short_Cmp").as("max_Short_Cmp"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Short_Cmp").equalTo(maxValueColumn.col("max_Short_Cmp"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Short_Cmp").drop("Match_ID2")
                .groupBy("Match_ID", "max_Short_Cmp").agg(collect_list("Player").as("Player_max_Short_Cmp"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Short_Att");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Short_Att").as("max_Short_Att"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Short_Att").equalTo(maxValueColumn.col("max_Short_Att"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Short_Att").drop("Match_ID2")
                .groupBy("Match_ID", "max_Short_Att").agg(collect_list("Player").as("Player_max_Short_Att"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Short_Cmp%");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Short_Cmp%").as("max_Short_Cmp%"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Short_Cmp%").equalTo(maxValueColumn.col("max_Short_Cmp%"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Short_Cmp%").drop("Match_ID2")
                .groupBy("Match_ID", "max_Short_Cmp%").agg(collect_list("Player").as("Player_max_Short_Cmp%"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Medium_Cmp");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Medium_Cmp").as("max_Medium_Cmp"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Medium_Cmp").equalTo(maxValueColumn.col("max_Medium_Cmp"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Medium_Cmp").drop("Match_ID2")
                .groupBy("Match_ID", "max_Medium_Cmp").agg(collect_list("Player").as("Player_max_Medium_Cmp"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Medium_Att");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Medium_Att").as("max_Medium_Att"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Medium_Att").equalTo(maxValueColumn.col("max_Medium_Att"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Medium_Att").drop("Match_ID2")
                .groupBy("Match_ID", "max_Medium_Att").agg(collect_list("Player").as("Player_max_Medium_Att"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Medium_Cmp%");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Medium_Cmp%").as("max_Medium_Cmp%"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Medium_Cmp%").equalTo(maxValueColumn.col("max_Medium_Cmp%"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Medium_Cmp%").drop("Match_ID2")
                .groupBy("Match_ID", "max_Medium_Cmp%").agg(collect_list("Player").as("Player_max_Medium_Cmp%"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Long_Cmp");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Long_Cmp").as("max_Long_Cmp"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Long_Cmp").equalTo(maxValueColumn.col("max_Long_Cmp"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Long_Cmp").drop("Match_ID2")
                .groupBy("Match_ID", "max_Long_Cmp").agg(collect_list("Player").as("Player_max_Long_Cmp"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Long_Att");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Long_Att").as("max_Long_Att"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Long_Att").equalTo(maxValueColumn.col("max_Long_Att"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Long_Att").drop("Match_ID2")
                .groupBy("Match_ID", "max_Long_Att").agg(collect_list("Player").as("Player_max_Long_Att"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Long_Cmp%");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Long_Cmp%").as("max_Long_Cmp%"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Long_Cmp%").equalTo(maxValueColumn.col("max_Long_Cmp%"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Long_Cmp%").drop("Match_ID2")
                .groupBy("Match_ID", "max_Long_Cmp%").agg(collect_list("Player").as("Player_max_Long_Cmp%"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Ast");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Ast").as("max_Ast"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Ast").equalTo(maxValueColumn.col("max_Ast"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Ast").drop("Match_ID2")
                .groupBy("Match_ID", "max_Ast").agg(collect_list("Player").as("Player_max_Ast"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "xAG");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("xAG").as("max_xAG"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("xAG").equalTo(maxValueColumn.col("max_xAG"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("xAG").drop("Match_ID2")
                .groupBy("Match_ID", "max_xAG").agg(collect_list("Player").as("Player_max_xAG"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "xA");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("xA").as("max_xA"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("xA").equalTo(maxValueColumn.col("max_xA"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("xA").drop("Match_ID2")
                .groupBy("Match_ID", "max_xA").agg(collect_list("Player").as("Player_max_xA"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "KP");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("KP").as("max_KP"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("KP").equalTo(maxValueColumn.col("max_KP"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("KP").drop("Match_ID2")
                .groupBy("Match_ID", "max_KP").agg(collect_list("Player").as("Player_max_KP"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "PPA");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("PPA").as("max_PPA"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("PPA").equalTo(maxValueColumn.col("max_PPA"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("PPA").drop("Match_ID2")
                .groupBy("Match_ID", "max_PPA").agg(collect_list("Player").as("Player_max_PPA"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "CrsPA");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("CrsPA").as("max_CrsPA"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("CrsPA").equalTo(maxValueColumn.col("max_CrsPA"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("CrsPA").drop("Match_ID2")
                .groupBy("Match_ID", "max_CrsPA").agg(collect_list("Player").as("Player_max_CrsPA"))
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


       // finalDf.show();
        finalDf.printSchema();

        return finalDf;
    }

    public void convertMaxPlayerPassing(Dataset<Row> df){
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
                        DataTypes.createStructField("max_Total_Cmp",structChild , false),
                        DataTypes.createStructField("max_Total_Att",structChild , false),
                        DataTypes.createStructField("max_Total_Cmp%",structDoubleChild , false),
                        DataTypes.createStructField("max_Total_TotDist",structChild , false),
                        DataTypes.createStructField("max_PrgDist",structChild , false),
                        DataTypes.createStructField("max_Short_Cmp",structChild , false),
                        DataTypes.createStructField("max_Short_Cmp%",structChild , false),
                        DataTypes.createStructField("max_Medium_Cmp",structDoubleChild , false),
                        DataTypes.createStructField("max_Medium_Att",structChild , false),
                        DataTypes.createStructField("max_Medium_Cmp%",structDoubleChild , false),
                        DataTypes.createStructField("max_Long_Cmp",structChild , false),
                        DataTypes.createStructField("max_Long_Att",structChild , false),
                        DataTypes.createStructField("max_Long_Cmp%",structDoubleChild , false),
                        DataTypes.createStructField("max_Ast",structChild , false),
                        DataTypes.createStructField("max_xAG",structDoubleChild , false),
                        DataTypes.createStructField("max_xA",structDoubleChild , false),
                        DataTypes.createStructField("max_KP",structChild , false),
                        DataTypes.createStructField("max_PPA",structChild , false),
                        DataTypes.createStructField("max_CrsPA",structChild , false),
                        DataTypes.createStructField("max_Prog",structChild , false)
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
                        RowFactory.create(v1.getSeq(11), v1.getDouble(10)),
                        RowFactory.create(v1.getSeq(13), v1.getInt(12)),
                        RowFactory.create(v1.getSeq(15), v1.getInt(14)),
                        RowFactory.create(v1.getSeq(17), v1.getInt(16)),
                        RowFactory.create(v1.getSeq(19), v1.getInt(18)),
                        RowFactory.create(v1.getSeq(21), v1.getDouble(20)),
                        RowFactory.create(v1.getSeq(23), v1.getInt(22)),
                        RowFactory.create(v1.getSeq(25), v1.getDouble(24)),
                        RowFactory.create(v1.getSeq(27), v1.getInt(26)),
                        RowFactory.create(v1.getSeq(29), v1.getInt(28)),
                        RowFactory.create(v1.getSeq(31), v1.getDouble(30)),
                        RowFactory.create(v1.getSeq(33), v1.getInt(32)),
                        RowFactory.create(v1.getSeq(35), v1.getDouble(34)),
                        RowFactory.create(v1.getSeq(37), v1.getDouble(36)),
                        RowFactory.create(v1.getSeq(39), v1.getInt(38)),
                        RowFactory.create(v1.getSeq(41), v1.getInt(40)),
                        RowFactory.create(v1.getSeq(43), v1.getInt(42)),
                        RowFactory.create(v1.getSeq(45), v1.getInt(44)));

            }
        }, RowEncoder.apply(struct));
        dfFinal.write().mode("overwrite").parquet("max" + ConfigName.PLAYER_PASSING + "/04-05-2023");
    }

    public static void main(String[] args){
        ETLData etl = new ETLplayerPassing();
        Dataset<Row> playerPassing = etl.playerPassing();
        // etl.convertMaxGkOverview(gkOverview);
    }
}
