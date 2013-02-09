package com.zootil.sample;

import com.zootil.election.ElectionWatcher;
import com.zootil.election.IElectable;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * Simple service that listens for leader changes
 */
public class SimpleElection implements IElectable
{
    public static void main(String [] args)
            throws IOException, InterruptedException, KeeperException
    {
        SimpleElection server = new SimpleElection();

        // this is just a simple server that assumes you are running zookeeper locally on the default port
        ElectionWatcher watcher = new ElectionWatcher("SimpleServer", java.net.InetAddress.getLocalHost().getHostName() + ":2181", server);

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
