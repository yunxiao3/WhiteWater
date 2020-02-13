package io.jiache.test;

import io.jiache.core.Address;
import io.jiache.core.Client;
import io.jiache.raft.RaftNode;
import io.jiache.raft.RaftServer;
import io.jiache.raft.SecretaryNode;
import io.jiache.raft.SecretaryServer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by jiacheng on 17-8-20.
 */
public class MultiClient {
    private static final int nodeNum=4;
    private static int benchmark;

    private static void run(int baseAddr) {
        // 初始化地址 leader为第一个 secretary为最后一个
        List<Address> addressList = IntStream.range(0,nodeNum).boxed()
                .map((x)->{return new Address("localhost", baseAddr+x);})
                .collect(Collectors.toList());
        RaftServer leader = new RaftNode();
        List<RaftServer> followers = addressList.subList(1,nodeNum-1)
                .stream().map(RaftNode::new)
                .collect(Collectors.toList());
        SecretaryServer secretary = new SecretaryNode();

        ExecutorService executor = Executors.newCachedThreadPool();
        executor.execute(() -> {
            try {
                leader.start(addressList.get(0));
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
        executor.execute(()->{
            try {
                secretary.start(addressList.get(nodeNum-1));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        followers.stream().forEach((follower)->{
            executor.execute(() -> {
                try {
                    follower.start();
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
        });
        leader.bootstrap(Arrays.asList(addressList.get(nodeNum-1)), addressList.subList(1,nodeNum-1));
        followers.stream().forEach((follower)->{
            executor.execute(()->follower.connectLeader(addressList.get(0)));
        });
        executor.execute(()->secretary.bootstrap(addressList.subList(1,nodeNum-1)));

        Client client = new Client(addressList.get(0).getIp(), addressList.get(0).getPort());
        long begin = System.currentTimeMillis();
        for(int i=0; i<benchmark; ++i) {
            client.put(""+i, "hello "+i);
        }
        double time = ((double)(System.currentTimeMillis()-begin))/1e3;
        System.out.println("benchmark-"+benchmark+" cost time is "+time);
        executor.shutdown();
    }

    public static void main(String[] args) {
        if(args.length == 0) {
            args = new String[]{"3", "200"};
        }
        if(args.length != 2) {
            System.out.println("please input 'client number' 'benchmark size'");
            System.exit(-1);
        }
        Integer clientNum = Integer.valueOf(args[0]);
        benchmark = Integer.valueOf(args[1]);
        int baseAddr = 10000;

        ExecutorService executor = Executors.newCachedThreadPool();
        for(int i=0; i<clientNum; ++i) {
            int finalBaseAddr = baseAddr;
            executor.execute(() -> {
                run(finalBaseAddr);
            });
            baseAddr += nodeNum;
        }
        executor.shutdown();
    }
}
