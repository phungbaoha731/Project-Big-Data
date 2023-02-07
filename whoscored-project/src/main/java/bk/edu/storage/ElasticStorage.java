package bk.edu.storage;


import bk.edu.conf.ConfigName;
import bk.edu.etl.ETLDataGkOverview;
import bk.edu.model.StatsInt;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import scala.Serializable;

public class ElasticStorage implements Serializable{
    private static RestHighLevelClient client;
    private static BulkProcessor bulkProcessor;

    public static RequestOptions COMMON_OPTIONS;
    public ElasticStorage() {
        RequestOptions.Builder builder1 = RequestOptions.DEFAULT.toBuilder();
        builder1.setHttpAsyncResponseConsumerFactory(
                new HttpAsyncResponseConsumerFactory
                        .HeapBufferedResponseConsumerFactory(200 * 1024 * 1024));
        COMMON_OPTIONS = builder1.build();
        int numHost = ConfigName.ES_HOST.length;
        HttpHost[] httpHosts = new HttpHost[numHost];
        for (int i = 0; i < numHost; i++) {
            httpHosts[i] = new HttpHost(ConfigName.ES_HOST[i], ConfigName.ES_PORT, "http");
        }
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("",
                        "")
        );

        client = new RestHighLevelClient(
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
        System.out.println("Client:" + client.getLowLevelClient().toString());

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

        bulkProcessor = BulkProcessor.builder(
                        (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                        listener)
                .setBulkActions(2000)
                .setBulkSize(new ByteSizeValue(10L, ByteSizeUnit.MB))
                .setConcurrentRequests(1)
                .setFlushInterval(TimeValue.timeValueMinutes(1))
                .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueMinutes(5), 3))
                .build();

        System.out.println("bulk" + bulkProcessor.toString());
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
            System.out.println("timeout");
            client.close();
        } catch (Exception ignored) {
        }
    }

    public static void main(String[] args){
        ElasticStorage es = new ElasticStorage();
        Map<String, Object> map = new HashMap<>();
        ObjectMapper oMapper = new ObjectMapper();
        map.put("hello", oMapper.convertValue(new StatsInt(null, 3), Map.class));
        for(int i = 0; i < 1; i++){
            es.getBulk().add(new IndexRequest("ggg").id(i + "ll").source(map));
            System.out.println("insert");
            try {
                boolean a = es.getClient().ping(RequestOptions.DEFAULT);
                if(a){
                    System.out.println("hellooo");
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        es.close();
    }

}
