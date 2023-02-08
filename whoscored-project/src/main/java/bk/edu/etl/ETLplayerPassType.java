package bk.edu.etl;

import bk.edu.conf.ConfigName;
import bk.edu.utils.SparkUtil;
import bk.edu.utils.TimeUtil;
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
public class ETLplayerPassType implements Serializable {
    private static SparkUtil sparkUtil;

    public ETLplayerPassType() {
        sparkUtil = new SparkUtil("who-scored", "save to hdfs", "yarn");
    }

    public Dataset<Row> playerPassType(){
        Dataset<Row> df = sparkUtil.getSparkSession().read().parquet("/user/" +ConfigName.PLAYER_PASS_TYPE + "/04-05-2023");
        Dataset<Row> dfTime = sparkUtil.getSparkSession().read().parquet("/user/" +ConfigName.RESULT_MATCHES + "/04-05-2023")
                .select("Match_ID", "Date", "Home", "Away", "Score")
                .withColumnRenamed("Match_ID", "Match_ID2");

        df = df.na().fill(0);
        df.createOrReplaceTempView("overview");
        Dataset<Row> finalDf = df.select("Match_ID", "Tournament");
        finalDf = finalDf.join(dfTime, dfTime.col("Match_ID2").equalTo(finalDf.col("Match_ID")), "inner").drop("Match_ID2").distinct();



        Dataset<Row> maxValueDf = df.select("Match_ID","Player", "Att");
        Dataset<Row> maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Att").as("max_Att"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueColumn.show();
        Dataset<Row> maxValueGroupBy = maxValueDf.join(maxValueColumn,
                maxValueDf.col("Att").equalTo(maxValueColumn.col("max_Att"))
                        .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Att").drop("Match_ID2")
                .groupBy("Match_ID", "max_Att").agg(collect_list("Player").as("Player_max_Att"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();



        maxValueDf = df.select("Match_ID","Player", "Att");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Att").as("max_Att"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Att").equalTo(maxValueColumn.col("max_Att"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Att").drop("Match_ID2")
                .groupBy("Match_ID", "max_Att").agg(collect_list("Player").as("Player_max_Att"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Pass_Type_Live");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Pass_Type_Live").as("max_Pass_Type_Live"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Pass_Type_Live").equalTo(maxValueColumn.col("max_Pass_Type_Live"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Pass_Type_Live").drop("Match_ID2")
                .groupBy("Match_ID", "max_Pass_Type_Live").agg(collect_list("Player").as("Player_max_Pass_Type_Live"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Pass_Type_Dead");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Pass_Type_Dead").as("max_Pass_Type_Dead"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Pass_Type_Dead").equalTo(maxValueColumn.col("max_Pass_Type_Dead"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Pass_Type_Dead").drop("Match_ID2")
                .groupBy("Match_ID", "max_Pass_Type_Dead").agg(collect_list("Player").as("Player_max_Pass_Type_Dead"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Pass_Type_FK");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Pass_Type_FK").as("max_Pass_Type_FK"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Pass_Type_FK").equalTo(maxValueColumn.col("max_Pass_Type_FK"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Pass_Type_FK").drop("Match_ID2")
                .groupBy("Match_ID", "max_Pass_Type_FK").agg(collect_list("Player").as("Player_max_Pass_Type_FK"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Pass_Type_TB");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Pass_Type_TB").as("max_Pass_Type_TB"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Pass_Type_TB").equalTo(maxValueColumn.col("max_Pass_Type_TB"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Pass_Type_TB").drop("Match_ID2")
                .groupBy("Match_ID", "max_Pass_Type_TB").agg(collect_list("Player").as("Player_max_Pass_Type_TB"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Pass_Type_Sw");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Pass_Type_Sw").as("max_Pass_Type_Sw"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Pass_Type_Sw").equalTo(maxValueColumn.col("max_Pass_Type_Sw"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Pass_Type_Sw").drop("Match_ID2")
                .groupBy("Match_ID", "max_Pass_Type_Sw").agg(collect_list("Player").as("Player_max_Pass_Type_Sw"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Pass_Type_Crs");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Pass_Type_Crs").as("max_Pass_Type_Crs"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Pass_Type_Crs").equalTo(maxValueColumn.col("max_Pass_Type_Crs"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Pass_Type_Crs").drop("Match_ID2")
                .groupBy("Match_ID", "max_Pass_Type_Crs").agg(collect_list("Player").as("Player_max_Pass_Type_Crs"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Pass_Type_TI");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Pass_Type_TI").as("max_Pass_Type_TI"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Pass_Type_TI").equalTo(maxValueColumn.col("max_Pass_Type_TI"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Pass_Type_TI").drop("Match_ID2")
                .groupBy("Match_ID", "max_Pass_Type_TI").agg(collect_list("Player").as("Player_max_Pass_Type_TI"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Pass_Type_CK");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Pass_Type_CK").as("max_Pass_Type_CK"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Pass_Type_CK").equalTo(maxValueColumn.col("max_Pass_Type_CK"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Pass_Type_CK").drop("Match_ID2")
                .groupBy("Match_ID", "max_Pass_Type_CK").agg(collect_list("Player").as("Player_max_Pass_Type_CK"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Corner_Kicks_In");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Corner_Kicks_In").as("max_Corner_Kicks_In"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Corner_Kicks_In").equalTo(maxValueColumn.col("max_Corner_Kicks_In"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Corner_Kicks_In").drop("Match_ID2")
                .groupBy("Match_ID", "max_Corner_Kicks_In").agg(collect_list("Player").as("Player_max_Corner_Kicks_In"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Corner_Kicks_Out");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Corner_Kicks_Out").as("max_Corner_Kicks_Out"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Corner_Kicks_Out").equalTo(maxValueColumn.col("max_Corner_Kicks_Out"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Corner_Kicks_Out").drop("Match_ID2")
                .groupBy("Match_ID", "max_Corner_Kicks_Out").agg(collect_list("Player").as("Player_max_Corner_Kicks_Out"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Corner_Kicks_Str");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Corner_Kicks_Str").as("max_Corner_Kicks_Str"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Corner_Kicks_Str").equalTo(maxValueColumn.col("max_Corner_Kicks_Str"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Corner_Kicks_Str").drop("Match_ID2")
                .groupBy("Match_ID", "max_Corner_Kicks_Str").agg(collect_list("Player").as("Player_max_Corner_Kicks_Str"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Outcomes_Cmp");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Outcomes_Cmp").as("max_Outcomes_Cmp"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Outcomes_Cmp").equalTo(maxValueColumn.col("max_Outcomes_Cmp"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Outcomes_Cmp").drop("Match_ID2")
                .groupBy("Match_ID", "max_Outcomes_Cmp").agg(collect_list("Player").as("Player_max_Outcomes_Cmp"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Outcomes_Off");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Outcomes_Off").as("max_Outcomes_Off"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Outcomes_Off").equalTo(maxValueColumn.col("max_Outcomes_Off"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Outcomes_Off").drop("Match_ID2")
                .groupBy("Match_ID", "max_Outcomes_Off").agg(collect_list("Player").as("Player_max_Outcomes_Off"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        maxValueDf = df.select("Match_ID","Player", "Outcomes_Blocks");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Outcomes_Blocks").as("max_Outcomes_Blocks"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Outcomes_Blocks").equalTo(maxValueColumn.col("max_Outcomes_Blocks"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Outcomes_Blocks").drop("Match_ID2")
                .groupBy("Match_ID", "max_Outcomes_Blocks").agg(collect_list("Player").as("Player_max_Outcomes_Blocks"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


        finalDf.show();
        finalDf.printSchema();

        return finalDf;
    }

    public void convertMaxPlayerPassType(Dataset<Row> df){
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
                        DataTypes.createStructField("max_Att",structChild , false),
                        DataTypes.createStructField("max_Pass_Type_Live",structChild , false),
                        DataTypes.createStructField("max_Pass_Type_Dead",structChild , false),
                        DataTypes.createStructField("max_Pass_Type_FK",structChild , false),
                        DataTypes.createStructField("max_Pass_Type_TB",structChild , false),
                        DataTypes.createStructField("max_Pass_Type_Sw",structChild , false),
                        DataTypes.createStructField("max_Pass_Type_Crs",structChild , false),
                        DataTypes.createStructField("max_Pass_Type_TI",structChild , false),
                        DataTypes.createStructField("max_Pass_Type_CK",structChild , false),
                        DataTypes.createStructField("max_Corner_Kicks_In",structChild , false),
                        DataTypes.createStructField("max_Corner_Kicks_Out",structChild , false),
                        DataTypes.createStructField("max_Corner_Kicks_Str",structChild , false),
                        DataTypes.createStructField("max_Outcomes_Cmp",structChild , false),
                        DataTypes.createStructField("max_Outcomes_Off",structChild , false),
                        DataTypes.createStructField("max_Outcomes_Blocks",structChild , false)

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
                        RowFactory.create(v1.getSeq(31), v1.getInt(30)),
                        RowFactory.create(v1.getSeq(33), v1.getInt(32)),
                        RowFactory.create(v1.getSeq(35), v1.getInt(34)));

            }
        }, RowEncoder.apply(struct));
        dfFinal.write().mode("overwrite").parquet("/user/max" + ConfigName.PLAYER_PASS_TYPE +"/2023-02-08");
    }

    public static void main(String[] args){
        ETLplayerPassType etl = new ETLplayerPassType();
        Dataset<Row> playerPassType = etl.playerPassType();
        // etl.convertMaxGkOverview(gkOverview);
    }
}
