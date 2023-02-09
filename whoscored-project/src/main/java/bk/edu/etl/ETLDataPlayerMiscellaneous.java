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
import scala.Function1;

import java.io.Serializable;

import static org.apache.spark.sql.functions.*;
public class ETLDataPlayerMiscellaneous implements Serializable {
    private static SparkUtil sparkUtil;

    public ETLDataPlayerMiscellaneous() {
        sparkUtil = new SparkUtil("who-scored", "save to hdfs", "yarn");
    }

    public Dataset<Row> playerMiscellaneous(){
        Dataset<Row> df = sparkUtil.getSparkSession().read().parquet("/user/" +ConfigName.PLAYER_MISCELLANEOUS + "/2023-02-08");
        Dataset<Row> dfTime = sparkUtil.getSparkSession().read().parquet("/user/" +ConfigName.RESULT_MATCHES + "/2023-02-08")
                .select("Match_ID", "Date", "Home", "Away", "Score")
                .withColumnRenamed("Match_ID", "Match_ID2");

        df = df.na().fill(0);
        df.createOrReplaceTempView("overview");
        Dataset<Row> finalDf = df.select("Match_ID", "Tournament");
        finalDf = finalDf.join(dfTime, dfTime.col("Match_ID2").equalTo(finalDf.col("Match_ID")), "inner").drop("Match_ID2").distinct();

        Dataset<Row> maxValueDf = df.select("Match_ID","Player", "Touches");
        Dataset<Row> maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Touches").as("max_Touches"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueColumn.show();
        Dataset<Row> maxValueGroupBy = maxValueDf.join(maxValueColumn,
                maxValueDf.col("Touches").equalTo(maxValueColumn.col("max_Touches"))
                        .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Touches").drop("Match_ID2")
                .groupBy("Match_ID", "max_Touches").agg(collect_list("Player").as("Player_max_Touches"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();

        maxValueDf = df.select("Match_ID","Player", "Touche_Def_Pen");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Touche_Def_Pen").as("max_Touche_Def_Pen"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Touche_Def_Pen").equalTo(maxValueColumn.col("max_Touche_Def_Pen"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Touche_Def_Pen").drop("Match_ID2")
                .groupBy("Match_ID", "max_Touche_Def_Pen").agg(collect_list("Player").as("Player_max_Touche_Def_Pen"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();

        maxValueDf = df.select("Match_ID","Player", "Touche_Def_3rd");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Touche_Def_3rd").as("max_Touche_Def_3rd"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Touche_Def_3rd").equalTo(maxValueColumn.col("max_Touche_Def_3rd"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Touche_Def_3rd").drop("Match_ID2")
                .groupBy("Match_ID", "max_Touche_Def_3rd").agg(collect_list("Player").as("Player_max_Touche_Def_3rd"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();

        maxValueDf = df.select("Match_ID","Player", "Touche_Mid_3rd");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Touche_Mid_3rd").as("max_Touche_Mid_3rd"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Touche_Mid_3rd").equalTo(maxValueColumn.col("max_Touche_Mid_3rd"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Touche_Mid_3rd").drop("Match_ID2")
                .groupBy("Match_ID", "max_Touche_Mid_3rd").agg(collect_list("Player").as("Player_max_Touche_Mid_3rd"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();

        maxValueDf = df.select("Match_ID","Player", "Touche_Att_3rd");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Touche_Att_3rd").as("max_Touche_Att_3rd"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Touche_Att_3rd").equalTo(maxValueColumn.col("max_Touche_Att_3rd"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Touche_Att_3rd").drop("Match_ID2")
                .groupBy("Match_ID", "max_Touche_Att_3rd").agg(collect_list("Player").as("Player_max_Touche_Att_3rd"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();

        maxValueDf = df.select("Match_ID","Player", "Touche_Att_Pen");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Touche_Att_Pen").as("max_Touche_Att_Pen"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Touche_Att_Pen").equalTo(maxValueColumn.col("max_Touche_Att_Pen"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Touche_Att_Pen").drop("Match_ID2")
                .groupBy("Match_ID", "max_Touche_Att_Pen").agg(collect_list("Player").as("Player_max_Touche_Att_Pen"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();

        maxValueDf = df.select("Match_ID","Player", "Touche_Live");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Touche_Live").as("max_Touche_Live"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Touche_Live").equalTo(maxValueColumn.col("max_Touche_Live"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Touche_Live").drop("Match_ID2")
                .groupBy("Match_ID", "max_Touche_Live").agg(collect_list("Player").as("Player_max_Touche_Live"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();

        maxValueDf = df.select("Match_ID","Player", "Dribbles_Succ");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Dribbles_Succ").as("max_Dribbles_Succ"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Dribbles_Succ").equalTo(maxValueColumn.col("max_Dribbles_Succ"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Dribbles_Succ").drop("Match_ID2")
                .groupBy("Match_ID", "max_Dribbles_Succ").agg(collect_list("Player").as("Player_max_Dribbles_Succ"))
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

        maxValueDf = df.select("Match_ID","Player", "Dribbles_Succ%");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Dribbles_Succ%").as("max_Dribbles_Succ%"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Dribbles_Succ%").equalTo(maxValueColumn.col("max_Dribbles_Succ%"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Dribbles_Succ%").drop("Match_ID2")
                .groupBy("Match_ID", "max_Dribbles_Succ%").agg(collect_list("Player").as("Player_max_Dribbles_Succ%"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();
//

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

    public void convertMaxPlayerMiscellaneous(Dataset<Row> df){
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
                        DataTypes.createStructField("max_Touches",structChild , false),
                        DataTypes.createStructField("max_Touche_Def_Pen",structChild , false),
                        DataTypes.createStructField("max_Touche_Def_3rd",structChild , false),
                        DataTypes.createStructField("max_Touche_Mid_3rd",structChild , false),
                        DataTypes.createStructField("max_Touche_Att_3rd",structChild , false),
                        DataTypes.createStructField("max_Touche_Att_Pen",structChild , false),
                        DataTypes.createStructField("max_Touche_Live",structChild , false),
                        DataTypes.createStructField("max_Dribbles_Succ",structChild , false),
                        DataTypes.createStructField("max_Dribbles_Att",structDoubleChild , false),
                        DataTypes.createStructField("max_Dribbles_Succ%",structDoubleChild , false),
                        DataTypes.createStructField("max_Receiving_Rec",structChild , false),
                        DataTypes.createStructField("max_Receiving_Prog",structChild , false),
                }
        );
        Dataset<Row> dfFinal = df.map(new MapFunction<Row, Row>() {
            @Override
            public Row call(Row v1) {
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
                        RowFactory.create(v1.getSeq(23), v1.getDouble(22)),
                        RowFactory.create(v1.getSeq(25), v1.getDouble(24)),
                        RowFactory.create(v1.getSeq(27), v1.getInt(26)),
                        RowFactory.create(v1.getSeq(29), v1.getInt(28)));
            }
        }, RowEncoder.apply(struct));
        dfFinal.write().mode("overwrite").parquet("/user/max" + ConfigName.PLAYER_MISCELLANEOUS + "/2023-02-08");
    }

    public static void main(String[] args){
        ETLDataPlayerMiscellaneous etl = new ETLDataPlayerMiscellaneous();
        Dataset<Row> playerMiscellaneous = etl.playerMiscellaneous();
        etl.convertMaxPlayerMiscellaneous(playerMiscellaneous);
    }
}
