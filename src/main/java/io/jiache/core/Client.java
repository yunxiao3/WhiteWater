package io.jiache.core;

import com.alibaba.fastjson.JSON;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.jiache.grpc.*;

/**
 * Created by jiacheng on 17-7-29.
 */
public class Client{
    private RaftServiceGrpc.RaftServiceBlockingStub blockingStub;

    private Address connectTo;

    public Client(String leaderIp, int leaderPort) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(leaderIp, leaderPort)
                .usePlaintext(true)
                .build();
        blockingStub = RaftServiceGrpc.newBlockingStub(channel);
        connectTo = new Address(leaderIp, leaderPort);
    }


    public void put(String key, Object object){
        PutResponce responce = blockingStub.put(PutRequest.newBuilder()
                .setKey(key)
                .setValueJson(JSON.toJSONString(object))
                .build());
        if(!responce.getSucess()) {
            throw new RuntimeException("put error");
        }
    }

    public String getJson(String key){
        GetResponce responce = blockingStub.get(GetRequest.newBuilder()
                .setKey(key)
                .build());
        return responce.getValue();
    }

    public <T> T get(String key, Class<T> clazz){
        String json = getJson(key);
        return JSON.parseObject(json,clazz);
    }

    public Address getConnectTo() {
        return connectTo;
    }
}
