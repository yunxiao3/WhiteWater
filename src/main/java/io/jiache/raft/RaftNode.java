package io.jiache.raft;

import com.alibaba.fastjson.JSON;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.jiache.grpc.*;
import io.jiache.core.Address;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jiacheng on 17-7-31.
 */
public class RaftNode implements RaftServer {
    private Address address;
    private Address leaderAddress;
    private List<Address> followersAddress;
    private List<Address> secretariesAddress;
    private StateMachine stateMachine;
    private List<Entry> log;
    private List<Integer> logReplicNum ;
    private Integer lastApplied;
    private Integer allNum;
    private Integer term;
    private List<SecretaryServiceGrpc.SecretaryServiceBlockingStub> blockingStubs;
    private RaftServiceGrpc.RaftServiceBlockingStub raftServiceBlockingStub;

    // rpc server
    private Server raftServer;

    public RaftNode() {
        this.stateMachine = StateMachine.newInstance();
        this.log = new ArrayList<>();
        this.lastApplied = -1;
        this.term = 1;
    }

    public RaftNode(Address address) {
        this.address = address;
    }

    public RaftNode(Address address, Address leaderAddress, List<Address> followersAddress, List<Address> secretariesAddress) {
        this();
        this.address = address;
        this.leaderAddress = leaderAddress;
        this.followersAddress = followersAddress;
        this.secretariesAddress = secretariesAddress;
        this.address = address;

        bootstrap(secretariesAddress,followersAddress);
    }

    @Override
    public void start(Address address) throws IOException, InterruptedException {
        raftServer = ServerBuilder.forPort(address.getPort())
                .addService(new RaftServiceImpl())
                .build()
                .start();
//        Runtime.getRuntime().addShutdownHook(new Thread(()->{
//            System.out.println("JVM shutdown");
//            RaftNode.this.raftServer.shutdown();
//            System.out.println("Server shutdown");
//        }));
        raftServer.awaitTermination();
    }

    @Override
    public void start() throws IOException, InterruptedException {
        if(this.address == null) {
            throw new RuntimeException("address didn't init");
        }
        start(address);
    }

    @Override
    public void initStub() {
        if(address.equals(leaderAddress)) {
            bootstrap(secretariesAddress, followersAddress);
        } else {
            connectLeader(leaderAddress);
        }
    }

    @Override
    public void bootstrap(List<Address> secretaryAddresses, List<Address> followerAddresses) {
        blockingStubs = new ArrayList<>();
        allNum = followerAddresses.size()+1;
        logReplicNum = new ArrayList<>();
        for(int i=0; i<secretaryAddresses.size(); ++i) {
            Address address = secretaryAddresses.get(i);
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(address.getIp(), address.getPort())
                    .usePlaintext(true)
                    .build();
            blockingStubs.add(SecretaryServiceGrpc.newBlockingStub(channel));
        }
    }

    @Override
    public void connectLeader(Address address) {
        leaderAddress = address;
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(address.getIp(), address.getPort())
                .usePlaintext(true)
                .build();
        raftServiceBlockingStub = RaftServiceGrpc.newBlockingStub(channel);
    }

    private class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase{
        @Override
        public void callBack(CallBackRequest request, StreamObserver<CallBackResponce> responseObserver) {
            increaseLogReplicNum(request.getReplicLogIndex());
            CallBackResponce responce = CallBackResponce.newBuilder()
                    .setSuccess(true)
                    .build();
            responseObserver.onNext(responce);
            responseObserver.onCompleted();
        }

        @Override
        public void put(PutRequest request, StreamObserver<PutResponce> responseObserver) {
            // 加到log中，复制到secretary中，最后等到大部分的follower复制完成则返回
            // 把entry添加到leader的log中，并给出唯一的logIndex

           // System.out.println("bug! yunxiao \n\n");


            Integer logIndex = addLog(request.getKey(), request.getValueJson());
            addToSecretary(logIndex);
//            while(logReplicNum.get(logIndex)<=allNum/2) {
//                Thread.interrupted();
//            }
            String result = commit(log.get(logIndex));
            PutResponce responce = PutResponce.newBuilder()
                    .setSucess(true)
                    .build();
            responseObserver.onNext(responce);
            responseObserver.onCompleted();
        }

        @Override
        public void get(GetRequest request, StreamObserver<GetResponce> responseObserver) {
            // 加到log中，复制到secretary中，最后等到大部分的follower复制完成则返回
//            Integer logIndex = addLog(request.getKey(), null);
//            addToSecretary(logIndex);
//            while(logReplicNum.get(logIndex)<=allNum/2) {
//                Thread.interrupted();
//            }
            String result = commit(new Entry(request.getKey(), null, 0, 0));
            GetResponce responce = GetResponce.newBuilder()
                    .setValue(result)
                    .build();
            responseObserver.onNext(responce);
            responseObserver.onCompleted();
        }

        @Override
        public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponce> responseObserver) {
            Entry entry = JSON.parseObject(request.getEntryJson(), Entry.class);
            Integer committedIndex = request.getCommittedIndex();
            // 添加entry
            if(entry!=null) {
                appendLog(entry);
//                raftServiceBlockingStub.callBack(CallBackRequest
//                        .newBuilder()
//                        .setReplicLogIndex(entry.getLogIndex())
//                        .build());
            }
            // 根据committedIndex进行commit
            int i;
            for(i=lastApplied+1; i<=committedIndex&&i<log.size(); ++i) {
                commit(log.get(i));
            }
//            lastApplied = i-1;
            responseObserver.onNext(AppendEntriesResponce.newBuilder()
                    .setSuccess(true)
                    .build());
            responseObserver.onCompleted();
        }
    }

    // leader中 在follower callback时, 对记录log已经完成复制
    private synchronized void increaseLogReplicNum(Integer replicLogIndex) {
        if(replicLogIndex == null || replicLogIndex>=logReplicNum.size()) {
            throw new RuntimeException("callback error");
        }
        logReplicNum.set(replicLogIndex, logReplicNum.get(replicLogIndex)+1);
    }

    // leader中 在put的request传来key,value后，添加日志到leader
    private synchronized Integer addLog(String key, String value) {
        Integer index = log.size();
        Entry entry = new Entry(key,value,index,term);
        System.out.println("bug! yunxiao \n\n");

        log.add(entry);
        logReplicNum.add(1);
        return index;
    }
    // 添加entry到secretary中
    private void addToSecretary(Integer logIndex) {
        Entry entry = log.get(logIndex);
        for(int i=0; i<blockingStubs.size(); ++i) {
            blockingStubs.get(i).addEntries(AddEntriesRequest
                    .newBuilder()
                    .setCommittedIndex(lastApplied)
                    .setEntryJson(JSON.toJSONString(entry))
                    .build());
        }
    }

    private synchronized String commit(Entry entry) {
        lastApplied = entry.getLogIndex();
        return (String) stateMachine.commit(entry);
    }

    // follower中 向log中添加entry
    private synchronized void appendLog(Entry entry) {
        while(log.size() != entry.getLogIndex()) {
            Thread.interrupted();
        }
        log.add(entry);
    }

    public Address getAddress() {
        return address;
    }

    public Address getLeaderAddress() {
        return leaderAddress;
    }

    public StateMachine getStateMachine() {
        return stateMachine;
    }

    public List<Entry> getLog() {
        return log;
    }

    public List<Integer> getLogReplicNum() {
        return logReplicNum;
    }

    public Integer getLastApplied() {
        return lastApplied;
    }

    public Integer getAllNum() {
        return allNum;
    }

    public Integer getTerm() {
        return term;
    }

    public List<SecretaryServiceGrpc.SecretaryServiceBlockingStub> getBlockingStubs() {
        return blockingStubs;
    }

    public RaftServiceGrpc.RaftServiceBlockingStub getRaftServiceBlockingStub() {
        return raftServiceBlockingStub;
    }

    public Server getRaftServer() {
        return raftServer;
    }
}
