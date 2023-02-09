package bk.edu.etl;

import bk.edu.conf.ConfigName;
import bk.edu.utils.SparkUtil;
import bk.edu.utils.TimeUtil;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

import static org.apache.spark.sql.functions.*;
public class ETLDataPlayerDefensive implements Serializable {
    private static SparkUtil sparkUtil;

    public ETLDataPlayerDefensive() {
        sparkUtil = new SparkUtil("who-scored", "save to hdfs", "yarn");
    }

    public Dataset<Row> playerDefensive(){
        Dataset<Row> df = sparkUtil.getSparkSession().read().parquet("/user/" +ConfigName.PLAYER_DEFENSIVE +"/2023-02-08");
        Dataset<Row> dfTime = sparkUtil.getSparkSession().read().parquet("/user/" +ConfigName.RESULT_MATCHES +"/2023-02-08")
                .select("Match_ID", "Date", "Home", "Away", "Score")
                .withColumnRenamed("Match_ID", "Match_ID2");

        df = df.na().fill(0);
        df.createOrReplaceTempView("overview");
        Dataset<Row> finalDf = df.select("Match_ID", "Tournament");
        finalDf = finalDf.join(dfTime, dfTime.col("Match_ID2").equalTo(finalDf.col("Match_ID")), "inner").drop("Match_ID2").distinct();

        Dataset<Row> maxValueDf = df.select("Match_ID","Player", "Tackles_Tkl");
        Dataset<Row> maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Tackles_Tkl").as("max_Tackles_Tkl"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueColumn.show();
        Dataset<Row> maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Tackles_Tkl").equalTo(maxValueColumn.col("max_Tackles_Tkl"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Tackles_Tkl").drop("Match_ID2")
                .groupBy("Match_ID", "max_Tackles_Tkl").agg(collect_list("Player").as("Player_max_Tackles_Tkl"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();

        maxValueDf = df.select("Match_ID","Player", "Tackles_Def_3rd");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Tackles_Def_3rd").as("max_Tackles_Def_3rd"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Tackles_Def_3rd").equalTo(maxValueColumn.col("max_Tackles_Def_3rd"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Tackles_Def_3rd").drop("Match_ID2")
                .groupBy("Match_ID", "max_Tackles_Def_3rd").agg(collect_list("Player").as("Player_max_Tackles_Def_3rd"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();

        // Tackles_Mid_3rd

        maxValueDf = df.select("Match_ID","Player", "Tackles_Mid_3rd");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Tackles_Mid_3rd").as("max_Tackles_Mid_3rd"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Tackles_Mid_3rd").equalTo(maxValueColumn.col("max_Tackles_Mid_3rd"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Tackles_Mid_3rd").drop("Match_ID2")
                .groupBy("Match_ID", "max_Tackles_Mid_3rd").agg(collect_list("Player").as("Player_max_Tackles_Mid_3rd"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();

        // Tackles_Att_3rd

        maxValueDf = df.select("Match_ID","Player", "Tackles_Att_3rd");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Tackles_Att_3rd").as("max_Tackles_Att_3rd"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Tackles_Att_3rd").equalTo(maxValueColumn.col("max_Tackles_Att_3rd"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Tackles_Att_3rd").drop("Match_ID2")
                .groupBy("Match_ID", "max_Tackles_Att_3rd").agg(collect_list("Player").as("Player_max_Tackles_Att_3rd"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();

        // Dribbles_Tkl
        maxValueDf = df.select("Match_ID","Player", "Dribbles_Tkl");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Dribbles_Tkl").as("max_Dribbles_Tkl"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Dribbles_Tkl").equalTo(maxValueColumn.col("max_Dribbles_Tkl"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Dribbles_Tkl").drop("Match_ID2")
                .groupBy("Match_ID", "max_Dribbles_Tkl").agg(collect_list("Player").as("Player_max_Dribbles_Tkl"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();

        // Dribbles_Att
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

        // Dribbles_Tkl% - Double
        maxValueDf = df.select("Match_ID","Player", "Dribbles_Tkl%");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Dribbles_Tkl%").as("max_Dribbles_Tkl%"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Dribbles_Tkl%").equalTo(maxValueColumn.col("max_Dribbles_Tkl%"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Dribbles_Tkl%").drop("Match_ID2")
                .groupBy("Match_ID", "max_Dribbles_Tkl%").agg(collect_list("Player").as("Player_max_Dribbles_Tkl%"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();

        // Blocks

        maxValueDf = df.select("Match_ID","Player", "Blocks");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Blocks").as("max_Blocks"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Blocks").equalTo(maxValueColumn.col("max_Blocks"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Blocks").drop("Match_ID2")
                .groupBy("Match_ID", "max_Blocks").agg(collect_list("Player").as("Player_max_Blocks"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();

        // Interceptions

        maxValueDf = df.select("Match_ID","Player", "Int");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Int").as("max_Int"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Int").equalTo(maxValueColumn.col("max_Int"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Int").drop("Match_ID2")
                .groupBy("Match_ID", "max_Int").agg(collect_list("Player").as("Player_max_Int"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();

        // Tkl-Int

        maxValueDf = df.select("Match_ID","Player", "Tkl-Int");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Tkl-Int").as("max_Tkl-Int"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Tkl-Int").equalTo(maxValueColumn.col("max_Tkl-Int"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Tkl-Int").drop("Match_ID2")
                .groupBy("Match_ID", "max_Tkl-Int").agg(collect_list("Player").as("Player_max_Tkl-Int"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();

        // Clearances

        maxValueDf = df.select("Match_ID","Player", "Clr");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Clr").as("max_Clr"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Clr").equalTo(maxValueColumn.col("max_Clr"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Clr").drop("Match_ID2")
                .groupBy("Match_ID", "max_Clr").agg(collect_list("Player").as("Player_max_Clr"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();

        // Errors

        maxValueDf = df.select("Match_ID","Player", "Err");
        maxValueColumn = maxValueDf.groupBy("Match_ID")
                .agg(max("Err").as("max_Err"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        maxValueGroupBy = maxValueDf.join(maxValueColumn,
                        maxValueDf.col("Err").equalTo(maxValueColumn.col("max_Err"))
                                .and(maxValueDf.col("Match_ID").equalTo(maxValueColumn.col("Match_ID2"))), "inner")
                .drop("Err").drop("Match_ID2")
                .groupBy("Match_ID", "max_Err").agg(collect_list("Player").as("Player_max_Err"))
                .withColumnRenamed("Match_ID", "Match_ID2");
        finalDf = finalDf.join(maxValueGroupBy, finalDf.col("Match_ID").equalTo(maxValueGroupBy.col("Match_ID2")), "inner").drop("Match_ID2").distinct();


//

        finalDf.show();
        finalDf.printSchema();

        return finalDf;
    }

    public void convertMaxPlayerDefensive(Dataset<Row> df){
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
                        DataTypes.createStructField("max_Tackles_Tkl",structChild , false),
                        DataTypes.createStructField("max_Tackles_Def_3rd",structChild , false),
                        DataTypes.createStructField("max_Tackles_Mid",structChild , false),
                        DataTypes.createStructField("max_Tackles_Att",structChild , false),
                        DataTypes.createStructField("max_Dribbles_Tkl",structChild , false),
                        DataTypes.createStructField("max_Dribbles_Att",structDoubleChild , false),
                        DataTypes.createStructField("max_Dribbles_Tkl%",structDoubleChild , false),
                        DataTypes.createStructField("max_Blocks",structChild , false),
                        DataTypes.createStructField("max_Int",structChild , false),
                        DataTypes.createStructField("max_Tkl-Int",structChild , false),
                        DataTypes.createStructField("max_Clr",structChild , false),
                        DataTypes.createStructField("max_Err",structChild , false),
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
                        RowFactory.create(v1.getSeq(17), v1.getDouble(16)),
                        RowFactory.create(v1.getSeq(19), v1.getDouble(18)),
                        RowFactory.create(v1.getSeq(21), v1.getInt(20)),
                        RowFactory.create(v1.getSeq(23), v1.getInt(22)),
                        RowFactory.create(v1.getSeq(25), v1.getInt(24)),
                        RowFactory.create(v1.getSeq(27), v1.getInt(26)),
                        RowFactory.create(v1.getSeq(29), v1.getInt(28)));
            }
        }, RowEncoder.apply(struct));
        dfFinal.write().mode("overwrite").parquet("/user/max" + ConfigName.PLAYER_DEFENSIVE +"/2023-02-08");
    }

    public static void main(String[] args){
        ETLDataPlayerDefensive etl = new ETLDataPlayerDefensive();
        Dataset<Row> playerDefensive = etl.playerDefensive();
        etl.convertMaxPlayerDefensive(playerDefensive);
    }
}