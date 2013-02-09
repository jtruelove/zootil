package com.zootil.election;


import com.zootil.util.NodeHelper;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;

interface INodeIdentifierGenerator
{
    String getIdentifier();
}

/**
 * Class to assist in enabling processes to take part in leader elections and propagate those election events down through
 *  the stack so services can become reactive to election events.
 *
 *  Based on the recipe on the ZK site: http://zookeeper.apache.org/doc/trunk/recipes.html#sc_leaderElection
 */
public class ElectionWatcher implements  Watcher
{
    private static final String NODE_SEPARATOR = "/";
    public static final String DEFAULT_ELECTION_NODE = "election";
    public static final String DEFAULT_LEADER_NODE = "currentLeader";

    private final String electionNodePath;
    private final String leaderNodeRootPath;
    private final String leaderNodePath;
    private final String appNodePath;
    private final String pathPrefix;
    private final String hostName;
    private final String processId;
    private ZooKeeper zooKeeperClient;
    private String currentNodePath;
    private int currentNodeId;
    private String watchedNode;
    private IElectable electable;
    private boolean amILeader;
    private final String zooKeeperAddress;
    private boolean firstConnect = true;

    /**
     * You are provided with a hook to plug in your own Id Generator, if you don't provide one the default generator
     *  based on process id will be used.
     */
    public INodeIdentifierGenerator IdentifierGenerator;

    /**
     * A default impl that uses process id
     */
    private final INodeIdentifierGenerator DEFAULT_ID_GENERATOR = () ->
    {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        int index = name.indexOf("@");
        return name.substring(0,index);
    };

    /**
     * Full Constructor
     *
     * @param electionNode the root node where election nodes will be registered
     * @param leaderNode the place where the leader registers itself as leader
     * @param app the app 'node' to build the election structure under in ZK
     * @param zookeeperConnectionString the comma separated list of ZK instances, i.e. server1:
     * @param electable the callback function to your service to notify in with the state of leadership or ZK events
     * @throws InterruptedException
     * @throws IOException
     * @throws KeeperException
     */
    public ElectionWatcher(String electionNode, String leaderNode, String app, String zookeeperConnectionString, IElectable electable)
            throws IOException, KeeperException, InterruptedException
    {
        pathPrefix = NODE_SEPARATOR + app + NODE_SEPARATOR;
        electionNodePath = pathPrefix + electionNode;
        leaderNodeRootPath = pathPrefix + leaderNode;
        hostName = java.net.InetAddress.getLocalHost().getHostName();
        processId = IdentifierGenerator == null ? DEFAULT_ID_GENERATOR.getIdentifier() : IdentifierGenerator.getIdentifier();
        appNodePath = getFullPathToElectionNode( hostName + "_" + processId + "_" );
        leaderNodePath = leaderNodeRootPath + NODE_SEPARATOR + hostName + "_" + processId;
        this.zooKeeperAddress = zookeeperConnectionString;
        amILeader = false;

        zooKeeperClient = new ZooKeeper(zookeeperConnectionString, 3000, this);
        this.electable = electable;

        // do an initial registration and check for leadership
        registerWithCluster();
        determineOrder();
    }

    /**
     * Basic Constructor
     *
     * Election root is assumed: /[app]/election
     * Leader node is assumed: /[app]/currentLeader
     *
     * @param app the app 'node' to build the election structure under in ZK
     * @param zookeeperConnectionString the comma separated list of ZK instances, i.e. server1:port,server2:port etc..
     * @param electable the callback function to your service so it can be notified with leadership changes
     * @throws InterruptedException
     * @throws IOException
     * @throws KeeperException
     */
    public ElectionWatcher(String app, String zookeeperConnectionString, IElectable electable)
                throws InterruptedException, IOException, KeeperException
    {
        this( DEFAULT_ELECTION_NODE, DEFAULT_LEADER_NODE, app, zookeeperConnectionString, electable );
    }

    /**
     * Figure out where you fit in the current leadership structure
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void determineOrder() throws KeeperException, InterruptedException
    {
        determineOrder(0);
    }

    /**
     * Figure out where you fit in the current leadership structure
     *
     * @param attempt the current attempt at determining order you are on
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void determineOrder(int attempt) throws KeeperException, InterruptedException
    {
        // there's no guarantee on the order of the children
        List<String> children = zooKeeperClient.getChildren( electionNodePath, false );
        HashMap<Integer, String> sequenceToPath = new HashMap<>();

        // extract the ZK sequence id and map the paths by that id
        for ( String child : children )
        {
            sequenceToPath.put( getIdFromNode(child), child );
        }

        // put the keys in a tree set for easy ordered access, you could sort and binary search if you wanted etc...
        TreeSet<Integer> set = new TreeSet<>( sequenceToPath.keySet() );

        // you are the leader
        if ( set.first() ==  currentNodeId )
        {
            System.out.println("I am the leader!!, registering leadership");
            registerAsLeader();
        }
        else // watch the node next in line
        {
            int idToWatch = set.lower(currentNodeId);
            watchedNode = getFullPathToElectionNode(sequenceToPath.get(idToWatch));
            Stat stat = zooKeeperClient.exists( watchedNode, true );

            // the node has disapeared since we last got the list and our watch failed
            if ( stat == null ) {
                if ( attempt < 3 ) {
                    watchedNode = null;
                    System.out.println(String.format( "The node I intended to watch has disapeared, %s, trying again...",
                                                   sequenceToPath.get(idToWatch)));
                    determineOrder(attempt++);
                }
                else {
                    String err = String.format("I've attempted 3 times to watch a node under path %s and failed, please check cluster",
                            electionNodePath);
                    System.err.println(err);
                    throw new IllegalStateException(err);
                }
            }
            else {
                System.out.println("Watching node: " + sequenceToPath.get(idToWatch));
            }
        }
    }

    private int getIdFromNode(String path)
    {
        return Integer.parseInt(path.substring( path.lastIndexOf("_") + 1 ));
    }

    private String getFullPathToElectionNode(String nodeName)
    {
        return electionNodePath + NODE_SEPARATOR + nodeName;
    }

    /**
     * Have this process register itself with the ZK cluster under the election node
     *
     * @throws InterruptedException
     * @throws KeeperException
     */
    private void registerWithCluster() throws InterruptedException, KeeperException
    {
        if ( zooKeeperClient != null ) {
            // make sure appropriate root paths are setup, these will be permanent paths by default once created
            NodeHelper.createFullPath(zooKeeperClient, leaderNodeRootPath);
            NodeHelper.createFullPath(zooKeeperClient, electionNodePath);

            try {
                Stat node = zooKeeperClient.exists(appNodePath, false);
                if ( node == null ) {
                    currentNodePath = zooKeeperClient.create( appNodePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL );
                    currentNodeId = getIdFromNode(currentNodePath);
                    System.out.println(String.format("Registered self with cluster at node: %s", currentNodePath));
                }
                else {
                    throw new IllegalStateException("An app instance on this host and process id is already registered on the node");
                }
            }
            catch ( KeeperException ex) {
                System.out.println("Encountered error registering with the ZooKeeper cluster");
                ex.printStackTrace();
                throw ex;
            }
        }
    }

    /**
     * You are the new leader let people know
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void registerAsLeader() throws KeeperException, InterruptedException
    {
        System.out.println(String.format("Removing any old leader node data under: %s", leaderNodeRootPath));
        // based on our rules ZK says I'm leader remove anything else that thinks it is leader
        NodeHelper.deleteChildrenOfPath( zooKeeperClient, leaderNodeRootPath);

        // create a leader node associated to this process that is ephemeral
        String newLeaderNode = NodeHelper.createFullPath( zooKeeperClient, leaderNodePath, CreateMode.EPHEMERAL);
        System.out.println(String.format("Registered as leader at node: %s for election node: %s", newLeaderNode, currentNodePath));

        // no need to watch you are the leader
        watchedNode = null;
        amILeader = true;

        electable.leadershipChanged( amILeader );
    }

    /**
     * Remove oneself from leadership
     */
    private void unregisterAsLeader()
    {
        amILeader = false;
        electable.leadershipChanged( amILeader );

        System.out.println(String.format("I no longer think I am leader at node: %s", currentNodePath));
    }

    public boolean amITheLeader()
    {
        return amILeader;
    }

    /**
     * ZK callback hook
     *
     * @param watchedEvent the event ZK wants to pass on
     */
    @Override
    public void process(WatchedEvent watchedEvent)
    {
        System.out.println("Received new event - " + watchedEvent);
        switch (watchedEvent.getState())
        {
           case SyncConnected:
               try
               {
                   if ( ! firstConnect ) {
                       // this is a reconnect after disconnect, it isn't related to a watched node
                       if ( watchedEvent.getType() == Event.EventType.None ) {
                            determineOrder();
                       }
                       // the node we were watching was deleted we are the new leader
                       else if ( watchedEvent.getType() == Event.EventType.NodeDeleted && watchedEvent.getPath().equals( watchedNode ) ) {
                           registerAsLeader();
                           System.out.println(NodeHelper.getTreeAsString(zooKeeperClient, pathPrefix));
                       }
                   }
                   else {
                       firstConnect = false;
                   }
               }
               catch (InterruptedException | KeeperException  e)
               {
                   System.out.println(String.format("Got an exception while determing order ex: %s", e));
                   electable.terminatingEventOcurred(e);
               }
               break;
           case Disconnected:
               if ( amILeader ) {
                  // we don't know the state of the world, lets stop the service from doing any more damage
                  unregisterAsLeader();
               }
               break;
           case Expired:
               try {
                   // on the expired event your client is toast and needs recreating, ephemeral nodes are gone
                   zooKeeperClient = new ZooKeeper(zooKeeperAddress, 3000, this);
                   registerWithCluster();
                   determineOrder();
               }
               catch (InterruptedException | KeeperException | IOException e) {
                   System.out.println(String.format("Session was expired, got an exception while creating new client and registering ex: %s", e));
                   // the environment is totally jacked let the election watcher take shutdown steps
                   electable.terminatingEventOcurred(e);
               }
               break;
           case AuthFailed:
               String err = "Athenticating to the ZK cluster failed cannot start up properly";
               System.out.println(err);
               electable.terminatingEventOcurred(new IllegalStateException(err));
           default:
               throw new IllegalStateException(String.format("Got a state type I have no mapping for State: %s", watchedEvent.getState()));
        }
    }
}
