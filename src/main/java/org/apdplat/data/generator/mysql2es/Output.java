package org.apdplat.data.generator.mysql2es;

import com.alibaba.fastjson.JSON;
import org.apdplat.data.generator.mysql.MySQLUtils;
import org.apdplat.data.generator.utils.Config;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ysc on 20/04/2018.
 */
public abstract class Output {
    private static final Logger LOGGER = LoggerFactory.getLogger(Output.class);

    private static final String HOST = (Config.getStringValue("es.host") == null ? "192.168.252.193" : Config.getStringValue("es.host"));
    private static final String PORT = (Config.getStringValue("es.port") == null ? "9200" : Config.getStringValue("es.port"));
    private static final String MODE = (Config.getStringValue("es.mode") == null ? "es" : Config.getStringValue("es.mode"));
    protected static final int BATCH_SIZE = Config.getIntValue("es.batchSize") == -1 ? 5 : Config.getIntValue("es.batchSize");
    private static final int MYSQL_PAGE_SIZE = Config.getIntValue("mysql.pageSize") == -1 ? 1000 : Config.getIntValue("mysql.pageSize");
    protected static final int START_PAGE = Config.getIntValue("output.start.page") == -1 ? 0 : Config.getIntValue("output.start.page");
    protected static final String ASYNC_OUTPUT = Config.getStringValue("output.async") == null ? "true" : Config.getStringValue("output.async");
    private static final int THREAD_COUNT = Config.getIntValue("output.async.thread.count") == -1 ? 1 : Config.getIntValue("output.async.thread.count");
    private static final ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool();
    private static final BlockingQueue<Map<String, Object>> BLOCKING_QUEUE = new LinkedBlockingQueue<>(THREAD_COUNT);
    private static volatile boolean running = true;

    private static final AtomicLong COUNT = new AtomicLong();

    private static final RestHighLevelClient CLIENT = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost(HOST, Integer.parseInt(PORT), "http")));

    static {
        if (!"file".equals(MODE) && "true".equalsIgnoreCase(ASYNC_OUTPUT)) {
            for (int i = 0; i < THREAD_COUNT; i++) {
                EXECUTOR_SERVICE.submit(() -> {
                    while (running) {
                        try {
                            Map<String, Object> map = BLOCKING_QUEUE.take();
                            if (map.get("data") == null) {
                                running = false;
                                break;
                            }
                            List<Map<String, Object>> data = (List<Map<String, Object>>) map.get("data");
                            String index = (String) map.get("index");
                            String type = (String) map.get("type");
                            output(index, type, null, data);
                        } catch (Exception e) {
                            LOGGER.error("获取数据异常", e);
                        }
                    }
                });
            }
        }
    }

    protected void writeBatch(String index, String type, BufferedWriter bufferedWriter, List<Map<String, Object>> list) throws Exception{
        if("true".equalsIgnoreCase(ASYNC_OUTPUT)){
            List<Map<String, Object>> newList = new ArrayList<>(list.size());
            newList.addAll(list);
            Map<String, Object> map = new HashMap<>();
            map.put("data", newList);
            map.put("index", index);
            map.put("type", type);
            BLOCKING_QUEUE.put(map);
            list.clear();
        }else{
            output(index, type, bufferedWriter, list);
        }
    }

    private static void output(String index, String type, BufferedWriter bufferedWriter, List<Map<String, Object>> list) {
        try {
            if ("file".equals(MODE)) {
                writeBatchToFile(index, type, bufferedWriter, list);
            } else {
                writeBatchToES(index, type, list);
            }
            list.forEach(item -> {
                if (item.get("geo_location") != null) {
                    ((Map<String, Float>) item.get("geo_location")).clear();
                }
                item.clear();
            });
            list.clear();
        }catch (Exception e){
            LOGGER.error("数据输出异常", e);
            throw new RuntimeException(e);
        }
    }

    private static void writeBatchToES(String index, String type, List<Map<String, Object>> list) throws Exception{
        if(list.isEmpty()){
            return;
        }
        BulkRequest request = new BulkRequest();
        for(Map<String, Object> data : list) {
            String id = data.get("id").toString();
            request.add(
                    new IndexRequest(index, type, id)
                            .source(data));

        }
        BulkResponse bulkResponse = CLIENT.bulk(request);
        if (bulkResponse.hasFailures()) {
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                if (bulkItemResponse.isFailed()) {
                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                    LOGGER.error("ES索引失败: {}", failure.getMessage());
                }
            }
        }
    }

    private static void writeBatchToFile(String index, String type, BufferedWriter bufferedWriter, List<Map<String, Object>> list) throws Exception{
        StringBuilder batchJson = new StringBuilder();
        for(Map<String, Object> data : list) {
            String json = JSON.toJSONString(data);
            String id = data.get("id").toString();
            batchJson.append("\n{ \"index\":{ \"_id\": \""+id+"\"} }\n").append(json).append("\n");
        }

        String command = "curl -H \"Content-Type: application/json\" -XPUT 'http://"+HOST+":"+PORT+"/"+index+"/"+type+"/_bulk' -d '"+batchJson.toString()+"';";

        bufferedWriter.write(command+"\n");
        bufferedWriter.flush();
    }

    protected void generateCommand(String table, String sql, String index, String type, String shellFileName) {
        Connection con = MySQLUtils.getConnection();
        if(con == null){
            return ;
        }
        PreparedStatement pst = null;
        ResultSet rs = null;
        List<Map<String, Object>> list = new ArrayList<>();
        try(BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(shellFileName)), "utf-8"))) {
            int count = MySQLUtils.getCount(table);
            int page = count / MYSQL_PAGE_SIZE;
            if(count % MYSQL_PAGE_SIZE > 0){
                page++;
            }
            LOGGER.info("表纪录数量: {}, 页面大小: {}, 总页数: {}", count, MYSQL_PAGE_SIZE, page);
            for(int i=START_PAGE; i<page; i++) {
                String join = "";
                if(sql.contains("where")){
                    join = " and ";
                }else{
                    join = " where ";
                }
                String sqlWithPage = sql + join + table + ".id > " + i*MYSQL_PAGE_SIZE + " and " + table +".id <= " + (i*MYSQL_PAGE_SIZE + MYSQL_PAGE_SIZE) +  ";";
                processPage(index, type, sqlWithPage, bufferedWriter, con, pst, rs, list);
            }
            writeBatch(index, type, bufferedWriter, list);
            bufferedWriter.flush();
            if (!"file".equals(MODE)) {
                for (int i = 0; i < THREAD_COUNT; i++) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("data", null);
                    map.put("index", null);
                    map.put("type", null);
                    BLOCKING_QUEUE.put(map);
                }
            }
        } catch (Exception e) {
            LOGGER.error("查询失败", e);
        } finally {
            MySQLUtils.close(con, pst, rs);
        }
    }

    private void processPage(String index, String type, String sql, BufferedWriter bufferedWriter, Connection con, PreparedStatement pst, ResultSet rs, List<Map<String, Object>> list){
        try {
            LOGGER.info("开始查询, SQL: {}", sql);
            pst = con.prepareStatement(sql);
            rs = pst.executeQuery();
            LOGGER.info("查询结束, 开始处理数据");
            while (rs.next()) {
                Map<String, Object> row = getRow(rs);
                list.add(row);
                COUNT.incrementAndGet();
                if(COUNT.get() % 1000 == 0) {
                    LOGGER.info("已写: {}", COUNT.get());
                }
                if(list.size() % BATCH_SIZE == 0) {
                    writeBatch(index, type, bufferedWriter, list);
                }
            }
        }catch (Exception e){
            LOGGER.error("处理页面异常", e);
        }
    }

    protected abstract Map<String, Object> getRow(ResultSet rs);

    public abstract void run();
}
