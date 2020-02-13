package io.jiache.test;

import io.jiache.raft.RaftServer;
import io.jiache.raft.SecretaryServer;
import io.jiache.core.Address;
import io.jiache.core.Client;
import io.jiache.raft.RaftNode;
import io.jiache.raft.SecretaryNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jiacheng on 17-8-1.
 */
public class Benchmark {
    private static int followerNum = 4;
    private static int secretaryNum = 1;
    private static int clientNum = 1;
    private static int benchmarkSize = 5;
    public static void main(String[] args) throws InterruptedException {
        if(args.length>0){
            if(args.length != 4){
                System.err.println("param number must be 'benchmarksize' 'clientnum' 'followernum' 'secretarynum'");
                System.exit(1);
            }
            benchmarkSize = Integer.valueOf(args[0]);
            clientNum = Integer.valueOf(args[1]);
            followerNum = Integer.valueOf(args[2]);
            secretaryNum = Integer.valueOf(args[3]);
        }
        Address leaderAddress = new Address("127.0.0.1", 8900);
        List<Address> followerAddressList = new ArrayList<>();
        for(int i=1; i<=followerNum; ++i) {
            followerAddressList.add(new Address("127.0.0.1", 8900+i));
        }
        List<Address> secretaryAddressList = new ArrayList<>();
        for(int i=1; i<=secretaryNum; ++i) {
            secretaryAddressList.add(new Address("127.0.0.1", 8800+i));
        }

        List<Integer> allocation = allocate(followerNum, secretaryNum);
        List<List<Address>> secretaryFollower = new ArrayList<>();
        for(int i=0, begin = 0; i<secretaryNum; ++i) {
            secretaryFollower.add(followerAddressList.subList(begin,begin+allocation.get(i)));
            begin += allocation.get(i);
        }

        RaftServer leader = new RaftNode();
        List<RaftServer> followerList = new ArrayList<>();
        followerAddressList.forEach((address -> followerList.add(new RaftNode())));
        List<SecretaryServer> secretaryList = new ArrayList<>();
        secretaryAddressList.forEach((address -> secretaryList.add(new SecretaryNode())));

        // 启动leader
        new Thread(()->{
            try {
                leader.start(leaderAddress);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        // 启动followers

        for(int i=0; i<followerList.size(); ++i) {
            int followerIndex = i;
            new Thread(() -> {
                try {
                    followerList.get(followerIndex).start(followerAddressList.get(followerIndex));
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();
        }

        // 启动secretaries
        for(int i=0; i<secretaryNum; ++i) {
            int secretaryIndex = i;
            new Thread(() -> {
                try {
                    secretaryList.get(secretaryIndex)
                            .start(secretaryAddressList.get(secretaryIndex));
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();
        }

        // leader得到stub
        leader.bootstrap(secretaryAddressList, followerAddressList);
        // follower得到stub
        followerList.forEach((follower)->{
            follower.connectLeader(leaderAddress);
        });
        // secretary开始向follower发送AppendEntries RPC
        for(int i=0; i<secretaryNum; ++i) {
            int sectaryIndex = i;
            new Thread(()->{
                secretaryList.get(sectaryIndex)
                        .bootstrap(secretaryFollower.get(sectaryIndex));
            }).start();
        }

        // 创建客户端
        List<Client> clientList = new ArrayList<>();
        for(int i=0; i<clientNum; ++i) {
            clientList.add(new Client(leaderAddress.getIp(), leaderAddress.getPort()));
        }
        for(int i=0; i<clientNum; ++i) {
            int clientIndex = i;
            new Thread(() -> {
                Client client = clientList.get(clientIndex);
                double begin = System.currentTimeMillis();
                for (int j = 0; j < benchmarkSize; ++j) {
                    client.put(clientIndex+"key" + j, "client"+clientIndex+" " + j);
                }
                for (int j = 0; j < benchmarkSize; ++j) {
                    String s = client.get(clientIndex+"key" + j, String.class);
                    System.out.println("client"+clientIndex+"   s: "+s);
                }
                double time = System.currentTimeMillis() - begin;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("client"+clientIndex+" cost time is " + time / 1000 + " sec");
            }).start();
        }
    }

    private static List<Integer> allocate(Integer all, Integer group) {
        int per = all/group;
        List<Integer> result = new ArrayList<>(group);
        for(int i=0; i<group; ++i) {
            result.add(per);
        }
        int remain = all-per*group;
        for(int i=0; i<remain; ++i) {
            result.set(
                    i, result.get(i)+1);
        }
        return result;
    }
}
