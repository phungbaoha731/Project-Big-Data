package bk.edu.pushing;

import bk.edu.conf.ConfigName;
import bk.edu.model.StatsDouble;
import bk.edu.model.StatsInt;
import bk.edu.storage.ElasticStorage;
import bk.edu.utils.SparkUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.elasticsearch.action.index.IndexRequest;
import scala.collection.JavaConverters;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PlayerDefensiveES implements Serializable {
    protected static ElasticStorage esStorage;

    protected static SparkUtil sparkUtil;

    public PlayerDefensiveES(){
        esStorage = new ElasticStorage();
        sparkUtil = new SparkUtil("who-scored", "save gk overview to es", "yarn");
    }

    public void writeToEs(){
        Dataset<Row> df = sparkUtil.getSparkSession().read().parquet("/user/max" + ConfigName.PLAYER_DEFENSIVE + "/2023-02-08");
        df = df.na().fill(0);
        df.printSchema();
        //df.show(10);
        List<Row> listDf = df.collectAsList();
        System.out.println("Start write to elastic");
        for(int i = 0; i < listDf.size(); i++){
            saveMaxDefensive(listDf.get(i));
            System.out.println("save " + i);
        }

        df = sparkUtil.getSparkSession().read().parquet("/user/" + ConfigName.PLAYER_DEFENSIVE + "/2023-02-08");
        df = df.na().fill(0);
        df.printSchema();
        //df.show(10);
        List<Row> listDf2 = df.collectAsList();
        System.out.println("Start write to elastic");
        for(int i = 0; i < listDf2.size(); i++){
            saveDefensive(listDf2.get(i));
            System.out.println("save " + i);
        }


        //spark-submit --master yarn --deploy-mode client --driver-memory 1g --executor-cores 2 --num-executors 1 --executor-memory 2g --class bk.edu.pushing.ETLGkOverViewES whoscored-project-1.0-SNAPSHOT-jar-with-dependencies.jar

    }

    public void saveMaxDefensive(Row row){
        String MatchId = row.getString(0);
        String tournament = row.getString(1);
        String dateStr = row.getString(2);
        Long date = null;
        DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");
        try {
            date = dateFormat.parse(dateStr).getTime();
        } catch (ParseException e) {
            System.out.println("Date wrong");
        }
        if(date == null){
            return;
        }

        String home = row.getString(3);
        String away = row.getString(4);
        String score = row.getString(5).replace("?", "-");

        Row maxTacklesTkl = row.getStruct(6);
        List<String> playerTacklesTkl = JavaConverters.seqAsJavaList(maxTacklesTkl.getSeq(0));
        int maxShot = maxTacklesTkl.getInt(1);
        StatsInt statsTacklesTkl = new StatsInt(playerTacklesTkl, maxShot);

        Row maxTacklesDef3rd = row.getStruct(7);
        List<String> playerTacklesDef3rd = JavaConverters.seqAsJavaList(maxTacklesDef3rd.getSeq(0));
        int maxTacklesDef3rdInt = maxTacklesDef3rd.getInt(1);
        StatsInt statsTacklesDef3rd = new StatsInt(playerTacklesDef3rd, maxTacklesDef3rdInt);

        Row maxTacklesMid = row.getStruct(8);
        List<String> playerTacklesMid = JavaConverters.seqAsJavaList(maxTacklesMid.getSeq(0));
        int maxTacklesMidInt = (int) maxTacklesMid.getDouble(1);
        StatsInt statsTacklesMid = new StatsInt(playerTacklesMid, maxTacklesMidInt);

        Row maxTacklesAtt = row.getStruct(9);
        List<String> playerTacklesAtt = JavaConverters.seqAsJavaList(maxTacklesAtt.getSeq(0));
        int maxTacklesAttInt = maxTacklesAtt.getInt(1);
        StatsInt statsTacklesAtt = new StatsInt(playerTacklesAtt, maxTacklesAttInt);

        Row maxDribblesTkl = row.getStruct(10);
        List<String> playerDribblesTkl = JavaConverters.seqAsJavaList(maxDribblesTkl.getSeq(0));
        int maxDribblesTklInt = (int)maxDribblesTkl.getDouble(1);
        StatsInt statsDribblesTkl = new StatsInt(playerDribblesTkl, maxDribblesTklInt);

        Row maxDribblesAtt = row.getStruct(11);
        List<String> playerDribblesAtt = JavaConverters.seqAsJavaList(maxDribblesAtt.getSeq(0));
        double maxDribblesAttInt = maxDribblesAtt.getDouble(1);
        StatsDouble statsDribblesAtt = new StatsDouble(playerDribblesAtt, maxDribblesAttInt);

        Row maxDribblesTklP = row.getStruct(12);
        List<String> playerDribblesTklP = JavaConverters.seqAsJavaList(maxDribblesTklP.getSeq(0));
        double maxDribblesTklPInt = maxDribblesTklP.getDouble(1);
        StatsDouble statsDribblesTklP = new StatsDouble(playerDribblesTklP, maxDribblesTklPInt);

        Row maxBlocks = row.getStruct(13);
        List<String> playerBlocks = JavaConverters.seqAsJavaList(maxBlocks.getSeq(0));
        int maxBlocksInt = maxBlocks.getInt(1);
        StatsInt statsBlocks = new StatsInt(playerBlocks, maxBlocksInt);

        Row maxInt = row.getStruct(14);
        List<String> playerInt = JavaConverters.seqAsJavaList(maxInt.getSeq(0));
        int maxIntInt = maxInt.getInt(1);
        StatsInt statsInt = new StatsInt(playerInt, maxIntInt);

        Row maxTklInt = row.getStruct(15);
        List<String> playerTklInt = JavaConverters.seqAsJavaList(maxTklInt.getSeq(0));
        int maxTklIntInt = maxTklInt.getInt(1);
        StatsInt statsTklInt = new StatsInt(playerTklInt, maxTklIntInt);

        Row maxClr = row.getStruct(16);
        List<String> playerClr = JavaConverters.seqAsJavaList(maxClr.getSeq(0));
        int maxClrInt = maxClr.getInt(1);
        StatsInt statsClr = new StatsInt(playerClr, maxClrInt);

        Row maxErr = row.getStruct(17);
        List<String> playerErr = JavaConverters.seqAsJavaList(maxErr.getSeq(0));
        int maxErrInt = maxErr.getInt(1);
        StatsInt statsErr = new StatsInt(playerErr, maxErrInt);


        ObjectMapper oMapper = new ObjectMapper();
        Map<String, Object> map = new HashMap<>();
        map.put("Tournament", tournament);
        map.put("Date", date);
        map.put("Home", home);
        map.put("Away", away);
        map.put("Score", score);
        map.put("TacklesTkl", oMapper.convertValue(statsTacklesTkl, Map.class));
        map.put("TacklesAtt", oMapper.convertValue(statsTacklesAtt, Map.class));
        map.put("TacklesDef3rd", oMapper.convertValue(statsTacklesDef3rd, Map.class));
        map.put("TacklesMid", oMapper.convertValue(statsTacklesMid, Map.class));
        map.put("DribblesTkl", oMapper.convertValue(statsDribblesTkl, Map.class));
        map.put("DribblesAtt", oMapper.convertValue(statsDribblesAtt, Map.class));
        map.put("DribblesTkl%", oMapper.convertValue(statsDribblesTklP, Map.class));
        map.put("Blocks", oMapper.convertValue(statsBlocks, Map.class));
        map.put("Int", oMapper.convertValue(statsInt, Map.class));
        map.put("TklInt", oMapper.convertValue(statsTklInt, Map.class));
        map.put("Clr", oMapper.convertValue(statsClr, Map.class));
        map.put("Err", oMapper.convertValue(statsErr, Map.class));


        esStorage.getBulk().add(new IndexRequest()
                .index("max_" + ConfigName.PLAYER_DEFENSIVE_INDEX)
                .id(MatchId).source(map));

    }

    private void saveDefensive(Row row) {
        Map<String, Object> map = new HashMap<>();
        String matchId = row.getString(1);

        try {
            map.put("Tournament", row.getString(0));
            map.put("PlayerID", row.getString(2));
            map.put("Player", row.getString(3));
            map.put("#", row.getInt(4));
            map.put("Nation", row.getString(5));
            map.put("Pos", row.getInt(6));
            map.put("Age", Integer.parseInt(row.getString(7).split("-")[0]));
            map.put("Min", row.getInt(8));
            map.put("TacklesTkl", row.getInt(9));
            map.put("TacklesTklW", row.getInt(10));
            map.put("TacklesDef3nd", row.getInt(11));
            map.put("TacklesMid3nd", row.getInt(12));
            map.put("TacklesAtt3nd", row.getInt(13));
            map.put("DribblesTkl", row.getDouble(14));
            map.put("DribblesAtt", row.getDouble(15));
            map.put("DribblesTkl%", row.getDouble(16));
            map.put("DribblesPast", row.getInt(17));
            map.put("Blocks", row.getInt(18));
            map.put("BlocksSh", row.getInt(19));
            map.put("BlocksPass", row.getInt(20));
            map.put("Int", row.getInt(21));
            map.put("TklInt", row.getInt(22));
            map.put("Clr", row.getInt(23));
            map.put("Err", row.getInt(24));

        } catch (Exception e){
            return;
        }

        esStorage.getBulk().add(new IndexRequest()
                .index(ConfigName.PLAYER_DEFENSIVE)
                .id(matchId).source(map));
    }

    public static void main(String[] args){
        PlayerDefensiveES playerDefensiveEs = new PlayerDefensiveES();
        playerDefensiveEs.writeToEs();
        esStorage.close();
    }
}
