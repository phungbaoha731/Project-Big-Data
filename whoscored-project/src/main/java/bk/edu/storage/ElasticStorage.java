package bk.edu.storage;


import bk.edu.conf.ConfigName;
import bk.edu.model.StatsInt;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.spark.sql.Row;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import scala.collection.JavaConverters;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ElasticStorage {
    private final RestHighLevelClient client;
    private final BulkProcessor bulkProcessor;

    public static RequestOptions COMMON_OPTIONS;

    static {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.setHttpAsyncResponseConsumerFactory(
                new HttpAsyncResponseConsumerFactory
                        .HeapBufferedResponseConsumerFactory(200 * 1024 * 1024));
        COMMON_OPTIONS = builder.build();
    }

    public ElasticStorage() {
        int numHost = ConfigName.ES_HOST.length;
        HttpHost[] httpHosts = new HttpHost[numHost];
        for (int i = 0; i < numHost; i++) {
            httpHosts[i] = new HttpHost(ConfigName.ES_HOST[i], ConfigName.ES_PORT, "http");
        }
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(ConfigName.ES_USER,
                        ConfigName.ES_PASSWORD)
        );

        this.client = new RestHighLevelClient(
                RestClient
                        .builder(httpHosts)
                        .setRequestConfigCallback(
                                builder -> builder
                                        .setConnectTimeout(2 * 60 * 1000)
                                        .setSocketTimeout(5 * 60 * 1000)
                                        .setConnectionRequestTimeout(0)
                        )
                        .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
        );

        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
                System.out.println("batch size: " + bulkRequest.numberOfActions());
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {

            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {

            }
        };

        this.bulkProcessor = BulkProcessor.builder(
                        (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                        listener)
                .setBulkActions(2000)
                .setBulkSize(new ByteSizeValue(10L, ByteSizeUnit.MB))
                .setConcurrentRequests(1)
                .setFlushInterval(TimeValue.timeValueMinutes(1))
                .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueMinutes(5), 3))
                .build();
    }

    public BulkProcessor getBulk() {
        return this.bulkProcessor;
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    public void close() {
        try {
            bulkProcessor.awaitClose(10, TimeUnit.SECONDS);
            client.close();
        } catch (Exception ignored) {
        }
    }

    public void saveMaxGkOverView(Row row){
        String MatchId = row.getString(0);
        String tournament = row.getString(1);
        String dateStr = row.getString(2);
        Long date = null;
        DateFormat dateFormat = new SimpleDateFormat("yyyy--mm-dd");
        try {
            date = dateFormat.parse(dateStr).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        if(date == null){
            return;
        }

        String home = row.getString(3);
        String away = row.getString(4);
        String score = row.getString(5).replace("?", "-");

        Row maxShotStoppingSoTA = row.getStruct(6);
        List<String> playerStoppingSoTa = JavaConverters.seqAsJavaList(maxShotStoppingSoTA.getSeq(0));
        int maxShot = maxShotStoppingSoTA.getInt(1);
        StatsInt statsStoppingSota = new StatsInt(playerStoppingSoTa, maxShot);

        Row maxLaunchedCmp = row.getStruct(11);
        List<String> playerLaunchedCmp = JavaConverters.seqAsJavaList(maxLaunchedCmp.getSeq(0));
        int maxLaunchedCmpInt = maxLaunchedCmp.getInt(1);
        StatsInt statsLaunchedCmp = new StatsInt(playerLaunchedCmp, maxLaunchedCmpInt);

        Row maxLaunchedAtt = row.getStruct(12);
        List<String> playerLaunchedAtt = JavaConverters.seqAsJavaList(maxLaunchedAtt.getSeq(0));
        int maxLaunchedAttInt = (int) maxLaunchedAtt.getDouble(1);
        StatsInt statsLaunchedAtt = new StatsInt(playerLaunchedAtt, maxLaunchedAttInt);

        Row maxPassesAtt = row.getStruct(14);
        List<String> playerPassesAtt = JavaConverters.seqAsJavaList(maxPassesAtt.getSeq(0));
        int maxPassesAttInt = maxPassesAtt.getInt(1);
        StatsInt statsPassesAtt = new StatsInt(playerPassesAtt, maxPassesAttInt);

        Row maxGoalKicksAtt = row.getStruct(18);
        List<String> playerGoalKicksAtt = JavaConverters.seqAsJavaList(maxGoalKicksAtt.getSeq(0));
        int maxGoalKicksAttInt = (int)maxGoalKicksAtt.getDouble(1);
        StatsInt statsGoalKicksAtt = new StatsInt(playerGoalKicksAtt, maxGoalKicksAttInt);

        Row maxCrossedOppAtt = row.getStruct(20);
        List<String> playerCrossedOppAtt = JavaConverters.seqAsJavaList(maxCrossedOppAtt.getSeq(0));
        int maxCrossedOppAttInt = maxCrossedOppAtt.getInt(1);
        StatsInt statsCrossedOppAtt = new StatsInt(playerCrossedOppAtt, maxCrossedOppAttInt);

        Map<String, Object> map = new HashMap<>();
        map.put("Tournament", tournament);
        map.put("Date", date);
        map.put("Home", home);
        map.put("Away", away);
        map.put("Score", score);
        map.put("ShotStoppingSoTA", statsStoppingSota);
        map.put("LaunchedCmp", statsLaunchedCmp);
        map.put("LaunchedAtt", statsLaunchedAtt);
        map.put("GoalKicksAtt", statsGoalKicksAtt);
        map.put("CrossesOpp", statsCrossedOppAtt);

        getBulk().add(new UpdateRequest()
                .index(ConfigName.GK_MAX_LOG_INDEX)
                .id(MatchId)
                .doc(map)
                .docAsUpsert(true));

    }

}
