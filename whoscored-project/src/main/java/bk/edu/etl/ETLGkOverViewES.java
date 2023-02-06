package bk.edu.etl;

import bk.edu.conf.ConfigName;
import bk.edu.storage.ElasticStorage;
import bk.edu.utils.SparkUtil;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Serializable;

public class ETLGkOverViewES implements Serializable {
    public final ElasticStorage esStorage;

    protected static SparkUtil sparkUtil;


    public ETLGkOverViewES(){
        esStorage = new ElasticStorage();
        sparkUtil = new SparkUtil("who-scored", "save gk overview to es", "yarn");
    }

    public void writeToEs(){
        Dataset<Row> df = sparkUtil.getSparkSession().read().parquet("max" + ConfigName.GK_OVERVIEW + "/04-05-2023");
        System.out.println("Start write to elastic");
        df.foreach(new ForeachFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                esStorage.saveMaxGkOverView(row);
                System.out.println(row.getString(0) + "is saved");
            }
        });
        System.out.println("End");
    }
    public static void main(String[] args){
        ETLGkOverViewES etlGkOverViewES = new ETLGkOverViewES();
        etlGkOverViewES.writeToEs();
    }

}
