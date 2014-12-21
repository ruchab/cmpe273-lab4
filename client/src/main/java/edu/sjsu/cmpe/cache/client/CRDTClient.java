package edu.sjsu.cmpe.cache.client;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;

import java.util.*;
import java.util.concurrent.*;

public class CRDTClient {
    private final List<String> servers;
    private final DistributedCacheService distributedCacheService;

    public CRDTClient(List<String> servers) {
        this.servers = servers;
        this.distributedCacheService = new DistributedCacheService(null, servers);
    }

    public void put(int key, String value) {
        Map<String, Boolean> writeStatus = distributedCacheService.asyncPut(key, value);

        if(!hasSuccessRate(writeStatus)){
            distributedCacheService.rollbackWrite(key,writeStatus);
        }
    }

    private boolean hasSuccessRate(Map<String, Boolean> statusMap) {
        int successWrites = 0;
        for(Map.Entry<String, Boolean> statusEntry:statusMap.entrySet()){
            if(statusEntry.getValue()){
                successWrites+=1;
            }
        }
        if (successWrites >=2)
            return true;
        else return false;
    }

    public void get(int key) {
        final Map<String, String> serverMap = distributedCacheService.asyncGet(key);
        HashMap<String, Integer> valueCount = new HashMap<String, Integer>(3);

        for(Map.Entry<String,String> entry:serverMap.entrySet()){
            System.out.println(" server-key: "+entry.getKey()+" server-value: "+entry.getValue());
        }

        for(Map.Entry<String,String> entry: serverMap.entrySet()){
            String serverUrl = entry.getKey();
            String v = entry.getValue();
            System.out.println("Current entry: "+ serverUrl +" value:"+ v);
            if(valueCount.containsKey(v) && v!=null){
                int value = valueCount.get(v);
                valueCount.put(v, value + 1);
            }else{
                valueCount.put(v, 1);
            }
        }

        for(Map.Entry<String,Integer> entry:valueCount.entrySet()){
            System.out.println("key: "+entry.getKey()+" value: "+entry.getValue());
        }

        Integer max = null;
        if (valueCount.size()>0){
            max = Collections.max(valueCount.values());
            System.out.println("max="+max);
        }
        String majorityValue = null;
        for(Map.Entry entry :valueCount.entrySet()) {
            if(entry.getValue() == max){
                majorityValue = (String) entry.getKey();
            }
        }

        for(Map.Entry<String,String> entry: serverMap.entrySet()){
            if(entry.getValue()==null){
                System.out.println(String.format("Now Read repairing the Key %s for node %s ",key,entry.getKey()));
                String nodeToBeRepaired = entry.getKey();
                DistributedCacheService repairDistributed = new DistributedCacheService(nodeToBeRepaired, null);
                System.out.println("majorityValue is: "+majorityValue);
                repairDistributed.put(key,majorityValue);
            }
        }

    }
}



class PutCall implements Callable<HttpResponse<JsonNode>> {
    private final String serverUrl;
    private final String key;
    private final String value;
    private final Callback callback;

    public PutCall(String serverUrl,String key,String value,Callback callback){
        this.serverUrl = serverUrl;
        this.key = key;
        this.value = value;
        this.callback = callback;
    }

    @Override
    public HttpResponse<JsonNode> call() throws Exception {
        System.out.println(String.format("Trying to put %s => %s in node %s",key,value,this.serverUrl));
        return (HttpResponse<JsonNode>) Unirest.put(this.serverUrl + "/cache/{key}/{value}")
                .header("accept", "application/json")
                .routeParam("key", key)
                .routeParam("value", value)
                .asJsonAsync(callback).get();
    }
}

class PutCallBackImpl implements Callback<JsonNode> {
    private final String serverUrl;
    private final Map<String, Boolean> statusMap;

    public PutCallBackImpl(String serverUrl, Map<String, Boolean> statusMap){
        this.serverUrl = serverUrl;
        this.statusMap = statusMap;
    }

    public void failed(UnirestException e) {
        System.out.println("Request Failed to node with url "+ serverUrl);
        statusMap.put(serverUrl, false);
    }

    public void completed(HttpResponse<JsonNode> response) {
        System.out.println("response status: "+response.getStatus());
        if(response.getStatus()==200){
            System.out.println("Request Succeeded for node "+ serverUrl);
            statusMap.put(serverUrl,true);
        }else{
            statusMap.put(serverUrl,false);
        }
    }

    public void cancelled() {
        System.out.println("The request has been cancelled");
    }
}