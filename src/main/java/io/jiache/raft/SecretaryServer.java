package io.jiache.raft;

import io.jiache.core.Address;

import java.io.IOException;
import java.util.List;

/**
 * Created by jiacheng on 17-7-31.
 */
public interface SecretaryServer {
    void start(Address address) throws IOException, InterruptedException;
    void start() throws IOException, InterruptedException;
    void bootstrap(List<Address> followerAddresses);
    void bootstrap();
}
