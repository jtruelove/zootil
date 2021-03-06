package com.zootil.sample;

import com.zootil.election.ElectionWatcher;
import com.zootil.election.IElectable;
import com.zootil.util.NodeHelper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * Simple service that listens for leader changes
 */
public class SimpleElection implements IElectable
{
    public static void main(String [] args)
            throws IOException, InterruptedException, KeeperException
    {
        // this is just a simple server that assumes you are running zookeeper locally on the default port
        ElectionWatcher watcher = new ElectionWatcher("SimpleServer", "0.0.0.0:2181,0.0.0.0:2182,0.0.0.0:2183", new SimpleElection());

        while (true)
        {
            Thread.sleep(1000);
        }
    }

    @Override
    public void leadershipChanged(boolean amILeader)
    {
        if ( amILeader ) {
            System.out.println("Yea, I'm the leader people must follow my rules!!!");
        }
        else {
            System.out.println("I'm no longer the leader why don't people like me :(.");
        }
    }

    @Override
    public void terminatingEventOcurred(Exception e)
    {
        System.err.println("Terminating event occurred related to leader election and registration with the cluster");
        e.printStackTrace();
        System.exit(-1);
    }
}
