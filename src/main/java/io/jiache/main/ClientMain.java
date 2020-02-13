package io.jiache.main;

imp ort io.jiache.core.Address;
import io.jiache.core.Client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by jiacheng on 17-8-28.
 */
public class ClientMain {
    private static Address leader;
    private static List<Address> cluster = new ArrayList<>();
    private static int putNumber;
    private static int getNumber;
    private static String baseValue = "01234567890123456789" +
            "01234567890123456789" +
            "01234567890123456789" +
            "01234567890123456789" +
            "01234567890123456789" +
            "01234567890123456789" +
            "01234567";
    public static void main(String[] args) throws InterruptedException {
        // args is [leaderHost:leaderPort] [putNumber] [host0:port0,host1:port1,...,host:port] [getNumber]
        if(args.length != 4 && args.length != 5) {
            System.out.println("args is [leaderHost:leaderPort] [putNumber] [host0:port0,host1:port1,...,host:port] [getNumber]");
            System.exit(-1);
        }
        if(args.length == 5) {
            baseValue = args[4];
        }
        String[] leaderS = args[0].split(":");
        leader = new Address(leaderS[0], Integer.parseInt(leaderS[1]));
        putNumber = Integer.parseInt(args[1]);
        Arrays.stream(args[2].split(",")).forEach((s)->{
            String[] ss = s.split(":");
            cluster.add(new Address(ss[0], Integer.parseInt(ss[1])));
        });
        getNumber = Integer.parseInt(args[3]);

        // put operation
        Client leaderClient = new Client(leader.getIp(), leader.getPort());
        long begin = System.currentTimeMillis();
        for(int i=0; i<putNumber; ++i) { ]]]
            leaderClient.put(i+"", baseValue+i);
        }
        double putTime = ((double)(System.currentTimeMillis()-begin))/1e3;
        System.out.println("put operation cost "+putTime+" sec");


        // get operation
        List<Client> clusterClients = new ArrayList<>();
        cluster.stream().forEach((address->{
            clusterClients.add(new Client(address.getIp(), address.getPort()));
        }));

//        TimeUnit.SECONDS.sleep(10);

        clusterClients.stream().forEach((client -> {
            new Thread(()->{
                long getBegin = System.currentTimeMillis();
                for(int i=0; i<getNumber; ++i) {
                    String value = client.getJson("0");
//                    System.out.println(value);
                }
                double getTime = ((double)(System.currentTimeMillis()-getBegin))/1e3;
                System.out.println(client.getConnectTo()+"get operation cost "+getTime+" sec");
            }).start();
        }));

    }
}