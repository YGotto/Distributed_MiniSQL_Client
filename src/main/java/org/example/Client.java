package org.example;

import com.alibaba.fastjson2.JSONObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.freva.asciitable.AsciiTable;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Client {
    private ZooKeeper zk;
    private Lock lock;
    private String nodePath;
    private Buffer buffer;

    public Client(String zkHosts) {
        try {
            this.zk = new ZooKeeper(zkHosts,2000,null);
        } catch (IOException e) {
            System.out.println("连接失败");
            throw new RuntimeException(e);
        }
        this.nodePath = "/curator/region_parent";
        // Buffer管理类
        this.buffer = new Buffer(this.zk, this.nodePath);
    }
    public void printBuffer() {
        System.out.println("TABLE NAME -> REGION NAME");
        for (String table : this.buffer.getTableRegionMap().keySet()) {
            System.out.println(table + " -> " + this.buffer.getTableRegionMap().get(table));
        }

        System.out.println("\nREGION NAME -> HOST");
        for (String region : this.buffer.getRegionHostsMap().keySet()) {
            System.out.println(region + " -> " + this.buffer.getRegionHostsMap().get(region));
        }
    }
    public String getTableFromSql(String sql) {
        String result = null;
        try {
            Statement statement = CCJSqlParserUtil.parse(sql);
            if (statement instanceof Select) {
                Select selectStatement = (Select) statement;
                result = ((PlainSelect) selectStatement.getSelectBody()).getFromItem().toString();
                //System.out.println(result);
            } else if (statement instanceof Insert) {
                Insert insertStatement = (Insert) statement;
                result = insertStatement.getTable().getName();
            } else if (statement instanceof Update) {
                Update updateStatement = (Update) statement;
                result = updateStatement.getTable().getName();
            } else if (statement instanceof Delete) {
                Delete deleteStatement = (Delete) statement;
                result = deleteStatement.getTable().getName();
            } else if (statement instanceof Drop) {
                Drop dropStatement = (Drop) statement;
                result = dropStatement.getName().toString();
            }
        } catch (JSQLParserException e) {
            System.out.println("Table parse failed!");
        }
        System.out.println(result);
        return result;
    }
    public boolean isCreateSql(String sql) {
        boolean result = false;
        try {
            Statement statement = CCJSqlParserUtil.parse(sql);
            if (statement instanceof CreateTable) {
                result = true;
            }
        } catch (JSQLParserException e) {
            return false;
        }
        return result;
    }
    private int getMasterTableCount(String data) {
        int count = 0;
        String[] dataList = data.split(",");
        if (dataList.length <= 4) return 0;
        for (int i = 4; i < dataList.length; i++) {
            if (!dataList[i].endsWith("_slave")) count++;
        }
        return count;
    }
    public String getLeastTableRegionUrl() {
        try {
            // 从zk获得region的信息并统计
            List<String> regions = zk.getChildren(nodePath, false);
            Map<String, String[]> regionDataMap = new HashMap<>();
            Map<String, Integer> regionTableCountMap = new HashMap<>();

            for (String region : regions) {
                String data = buffer.getRegionData(region);
                String[] dataList = data.split(",");
                regionDataMap.put(region, dataList);
                regionTableCountMap.put(region, getMasterTableCount(data));
            }

            // 返回最短（即包含table最少）的region的url
            String minRegion = regionTableCountMap.entrySet().stream()
                    .min(Map.Entry.comparingByValue())
                    .orElseThrow(() -> new RuntimeException("No regions found"))
                    .getKey();

            // 返回 hosts:port
            String[] minRegionData = regionDataMap.get(minRegion);
            return minRegionData[0] + ":" + minRegionData[1];
        } catch (Exception e) {
            System.err.println("ERROR: get least table region url Error " + e);
            return null;
        }
    }
    public void exec(String sql) {
        String tableName;
        String url;

        if (isCreateSql(sql)) { // create 命令, 需要根据负载均衡找到所需的 region_url
            tableName = getTableFromSql(sql);
            url = getLeastTableRegionUrl();
        } else { // 非 create 命令, 查找对应的 table 所在的 region
            tableName = getTableFromSql(sql);
            if (tableName == null) {
                System.out.println("No table in sql!");
                return;
            }
            url = buffer.getRegionUrl(tableName);
            // 检查 Buffer 内是否含有该 table
            if (url == null) {
                System.out.println("INFO: Table name not found in buffer!");
                System.out.println("INFO: Buffer will be refreshed!");
                buffer.refreshBuffer();
                url = buffer.getRegionUrl(tableName);
                if (url == null) {
                    System.out.println("WARNING: table name not found in refreshed buffer!");
                    System.out.println("WARNING: Please check your sql!");
                    return;
                }
            }
        }

        System.out.println("Table: " + tableName + "  Region: " + url);

        HttpURLConnection connection = null;
        try {
            System.out.println("sql exec...");
            JSONObject data = new JSONObject();
            data.put("sql", sql);
            String newsql = sql.replace(' ', '@');
            System.out.println("http://" + url + "/exec/"+newsql);
            URL urlObj = new URL("http://" + url + "/exec/"+newsql);
            connection = (HttpURLConnection) urlObj.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setDoOutput(true);
            connection.getOutputStream().write(data.toString().getBytes(StandardCharsets.UTF_8));

            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                String response = new Scanner(connection.getInputStream(), StandardCharsets.UTF_8.name()).useDelimiter("\\A").next();
                JSONObject result = JSONObject.parseObject(response, JSONObject.class);

                if (!"true".equalsIgnoreCase(result.getString("isSuccess"))) {
                    System.out.println("WARNING: sql exec failed!");
                    System.out.println(result.getString("selectResult"));
                } else {
                    System.out.println("Info: sql exec success!");
                    if (!"select".equalsIgnoreCase(result.getString("operation"))) {
                        System.out.println(result.getString("selectResult"));
                    } else {
                        System.out.println(result.getString("selectResult"));
                        selectPrint(result.getString("selectResult"));
                    }
                }
            } else {
                System.out.println("WARNING: response not OK!");
                String response = new Scanner(connection.getErrorStream(), StandardCharsets.UTF_8.name()).useDelimiter("\\A").next();
                System.out.println(new JSONObject(Integer.parseInt(response)));
            }
        } catch (IOException e) {
            System.out.println("ERROR: sql exec timeout!");
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
    public void selectPrint(String msg) {
        System.out.println(msg);
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            JsonNode msgNode = objectMapper.readTree(msg);
            JsonNode fieldsNode = msgNode.get("fields");
            JsonNode dataNode = msgNode.get("data");

            // 打印字段名
            for (JsonNode field : fieldsNode) {
                System.out.print(field.asText() + "\t");
            }
            System.out.println();
            // 打印分隔线
            for (JsonNode field : fieldsNode) {
                System.out.print("--------");
            }
            System.out.println();
            // 打印数据
            for (JsonNode dataEntry : dataNode) {
                for (JsonNode field : fieldsNode) {
                    System.out.print(dataEntry.get(field.asText()).asText() + "\t");
                }
                System.out.println();
            }

        } catch (IOException e) {
            System.err.println("ERROR: Failed to parse JSON message: " + e.getMessage());
        }
    }
    public void close() {
        try {
            zk.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Client closed.");
    }

    public Buffer getBuffer() {
        return this.buffer;
    }
}
