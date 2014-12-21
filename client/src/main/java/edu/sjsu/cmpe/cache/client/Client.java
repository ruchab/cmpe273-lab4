package edu.sjsu.cmpe.cache.client;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client...");
        List<String> servers = Arrays.asList("http://localhost:3000", "http://localhost:3001", "http://localhost:3002");

        // Declaration

        System.out.println("Step 1:First HTTP PUT call to store “a” to key 1.");
        final CRDTClient crdtClient = new CRDTClient(servers);

        crdtClient.put(1, "a");

        sleepFor(30000); // sleep for 30 seconds

        System.out.println("Step 2:Second HTTP PUT call to update key 1 value to “b”.");
        final CRDTClient crdtClient1 = new CRDTClient(servers);
        crdtClient1.put(1,"b");

        sleepFor(30000); // sleep for 30 seconds

        System.out.println("Step 3:Final HTTP GET call to retrieve key “1” value. ");
        final CRDTClient crdtClient2 = new CRDTClient(servers);
        crdtClient2.get(1);
        System.out.println("Existing Cache Client...");
    }

    private static void sleepFor(int sleepInMillis) {
        try {
            System.out.println("Sleeping for now");
            Thread.sleep(sleepInMillis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
