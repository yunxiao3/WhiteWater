package io.jiache.raft;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jiacheng on 17-7-28.
 */
public class StateMachine {
    private Map<String, Object> storage;

    private StateMachine(Map storage) {
        this.storage = storage;
    }

    public static StateMachine newInstance(){
        return new StateMachine(new HashMap());
    }

    Object commit(Entry entry){
        if(entry.getValue() == null) {
            return storage.get(entry.getKey());
        }
        storage.put(entry.getKey(), entry.getValue());
        return null;
    }
}
