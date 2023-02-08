package bk.edu.pushing;

import bk.edu.conf.ConfigName;
import bk.edu.model.StatsInt;
import bk.edu.storage.ElasticStorage;
import bk.edu.utils.SparkUtil;
import bk.edu.utils.TimeUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.elasticsearch.action.index.IndexRequest;
import scala.Serializable;
import scala.collection.JavaConverters;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ETLPlayerPassingES implements Serializable {
    protected static ElasticStorage esStorage;

    protected static SparkUtil sparkUtil;

    public ETLPlayerPassingES(){
        esStorage = new ElasticStorage();
        sparkUtil = new SparkUtil("who-scored", "save player passing to es", "yarn");
    }

    public void writeToEs(){
        Dataset<Row> df = sparkUtil.getSparkSession().read().parquet("/user" + ConfigName.PLAYER_PASSING + "/2023-02-08");
        df = df.na().fill(0);
        df.printSchema();
        //df.show(10);
        List<Row> listDf = df.collectAsList();
        System.out.println("Start write to elastic");
        for(int i = 0; i < listDf.size(); i++){
            savePlayerPassing(listDf.get(i));
            System.out.println("save " + i);
        }
        System.out.println("End");
    }

    public void savePlayerPassing(Row row){
        String tournament = row.getString(0);
        String matchId = row.getString(1);
        String playerId = row.getString(2);
        String player = row.getString(3);
        int number = Integer.parseInt(row.getString(4));
        String nation = row.getString(5).split(" ")[1];
        String pos = row.getString(6);
        int age = Integer.parseInt(row.getString(7));
        int min = Integer.parseInt(row.getString(8));
        int totalCmp = Integer.parseInt(row.getString(9));
        double totalCmpPercent = Double.parseDouble(row.getString(10));
        int totalTotDist = Integer.parseInt(row.getString(11));
        int totalPrgDist = Integer.parseInt(row.getString(12));
        int shortCmp = Integer.parseInt(row.getString(13));
        int shortAtt = Integer.parseInt(row.getString(14));
        double shortCmpPercent = Double.parseDouble(row.getString(15));
        int mediumCmp = Integer.parseInt(row.getString(16));
        int mediumAtt = Integer.parseInt(row.getString(17));
        double mediumCmpPercent = Double.parseDouble(row.getString(18));
        int longCmp = Integer.parseInt(row.getString(19));
        int longAtt = Integer.parseInt(row.getString(20));
        double longCmpPercent = Double.parseDouble(row.getString(21));
        int ast = Integer.parseInt(row.getString(22));
        int xag = Integer.parseInt(row.getString(23));
        int xa = Integer.parseInt(row.getString(24));
        int kp = Integer.parseInt(row.getString(25));
        int onethree = Integer.parseInt(row.getString(26));
        int ppa = Integer.parseInt(row.getString(27));
        int crsPA = Integer.parseInt(row.getString(28));
        int prog = Integer.parseInt(row.getString(29));

        Map<String, Object> map = new HashMap<>();
        map.put("Tournament", tournament);
        map.put("PlayerID", playerId);
        map.put("Player", player);
        map.put("Number", number);
        map.put("Nation", nation);
        map.put("Pos", pos);
        map.put("Age", age);
        map.put("Min", min);
        map.put("TotalCmp", totalCmp);
        map.put("TotalCmpPercent", totalCmpPercent);
        map.put("TotalTotDist", totalTotDist);
        map.put("TotalPrgDist", totalPrgDist);
        map.put("ShortCmp", shortCmp);
        map.put("ShortAtt", shortAtt);
        map.put("ShortCmpPercent", shortCmpPercent);
        map.put("MediumCmp", mediumCmp);
        map.put("MediumAtt", mediumAtt);
        map.put("MediumCmpPercent", mediumCmpPercent);
        map.put("LongCmp", longCmp);
        map.put("LongAtt", longAtt);
        map.put("LongCmpPercent", longCmpPercent);
        map.put("Ast", ast);
        map.put("xAG", xag);
        map.put("xA", xa);
        map.put("KP", kp);
        map.put("OneThree", onethree);
        map.put("PPA", ppa);
        map.put("CrsPA", crsPA);
        map.put("Prog", prog);

        esStorage.getBulk().add(new IndexRequest()
                .index("max_" + ConfigName.PLAYER_PASSING_INDEX)
                .id(matchId).source(map));

    }


    public static void main(String[] args){
        ETLPlayerPassingES etlPlayerPassingES = new ETLPlayerPassingES();
        etlPlayerPassingES.writeToEs();
        esStorage.close();
    }
}   
