package org.example;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;

public class Buffer {

    // 储存所有的region节点，主要是跟zookeeper进行同步
    private List<String> regionNames;
    // 储存 table_name -> region_name 的映射
    private Map<String, String> tableRegionMap;
    // 储存 region_name -> hosts+port 的映射
    private Map<String, String> regionHostsMap;
    // 线程安全锁
    private Lock lock;
    // zookeeper client
    private ZooKeeper zk;
    private String nodePath;

    // 构造函数
    public Buffer(ZooKeeper zk, String nodePath) {
        this.regionNames = new ArrayList<>();
        this.tableRegionMap = new HashMap<>();
        this.regionHostsMap = new HashMap<>();
        this.zk = zk;
        this.nodePath = nodePath;

        // 初始化Buffer
        System.out.println("Buffer init...");
        refreshBuffer();
    }

    public String getRegionUrl(String tableName) {
        try {
            if (tableRegionMap.containsKey(tableName)) {
                String regionName = tableRegionMap.get(tableName);
                if (!regionHostsMap.containsKey(regionName)) {
                    System.out.println("WARNING: region found in tableRegionMap but not found in regionHostsMap.");
                    System.out.println("INFO: Buffer will be refreshed");
                    refreshBuffer();
                }
                return regionHostsMap.get(regionName);
            } else {
                return null;
            }
        } finally {
        }
    }
    public String getRegionData(String regionName) {
        try {
            String path = nodePath + "/" + regionName;
            byte[] data = zk.getData(path, false, null);
            return new String(data, "UTF-8");
        } catch (KeeperException | InterruptedException | IOException e) {
            System.err.println("ERROR: getRegionData Error: " + e.getMessage());
            return null;
        }
    }
    public void refreshBuffer() {
        try {
            regionNames.clear();
            tableRegionMap.clear();
            regionHostsMap.clear();
            regionNames = zk.getChildren(nodePath, false);
            for (String regionName : regionNames) {
                appendRegion(regionName);
            }

        } catch (KeeperException | InterruptedException e) {
            System.err.println("ERROR: refresh_buffer Error: " + e.getMessage());
        }
    }
    public void appendRegion(String regionName){
        // 返回table_list里面的master_table,即不以_slave结尾的table
        String data = getRegionData(regionName);
        String[] dataArr = data.split(",");
        String hosts = dataArr[0];
        String port = dataArr[1];

        regionHostsMap.put(regionName, hosts + ":" + port);

        if (dataArr.length > 6) {
            List<String> masterTables = getMasterTable(List.of(dataArr).subList(6, dataArr.length));
            for (String masterTable : masterTables) {
                tableRegionMap.put(masterTable, regionName);
            }
        }
    }
    public List<String> getMasterTable(List<String> tableList) {
        List<String> result = new ArrayList<>();
        for (String table : tableList) {
            if (!table.endsWith("_slave")) {
                result.add(table);
            }
        }
        return result;
    }

    public Map<String, String> getTableRegionMap() {
        return this.tableRegionMap;
    }

    public Map<String, String> getRegionHostsMap() {
        return this.regionHostsMap;
    }
}
