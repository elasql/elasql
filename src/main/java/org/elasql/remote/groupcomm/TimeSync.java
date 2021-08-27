package org.elasql.remote.groupcomm;

import java.io.Serializable;

import org.elasql.server.Elasql;


public class TimeSync implements Serializable {
    private long timeStamp;
    private int nodeId;
    private boolean request;
    
    public TimeSync() {
        this.timeStamp = System.nanoTime()/1000;
        this.nodeId = Elasql.serverId();
        this.request = false;
    }

    public TimeSync(long time) {
        this.timeStamp = time;
        this.nodeId = Elasql.serverId();
        this.request = false;
    }

    public TimeSync(long time, int ID, boolean back) {
        this.timeStamp = time;
        this.nodeId = ID;
        this.request = back;
    }

    public long getTime(){
        return this.timeStamp;
    }

    public int getServerID(){
        return this.nodeId;
    }

    public boolean isRequest(){
        return this.request;
    }
    
}
