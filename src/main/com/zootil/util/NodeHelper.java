package com.zootil.util;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * General purpose helper functions for interacting with Zookeeper.
 */
public class NodeHelper
{
    /**
     * Handles creating multi depth paths in one call.
     *
     * @param zk your zookeeper connection
     * @param path the full path you want to create
     * @return the full path created
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static String createFullPath( ZooKeeper zk, String path )
                throws KeeperException, InterruptedException
    {
        return createFullPath(zk, path, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * Handles creating multi depth paths in one call.
     *
     * @param zk your zookeeper connection
     * @param path the full path you want to create
     * @param mode what type of nodes are you creating, note some types can't support children
     * @return the full path created
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static String createFullPath( ZooKeeper zk, String path, CreateMode mode )
                    throws KeeperException, InterruptedException
    {
        return createFullPath(zk, path, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
    }

    /**
     * Handles creating multi depth paths in one call.
     *
     * @param zk your zookeeper connection
     * @param path the full path you want to create
     * @param acl the ACL details to associate to the path
     * @param mode what type of nodes are you creating, note some types can't support children
     * @return the full path created
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static String createFullPath( ZooKeeper zk, String path, List<ACL> acl, CreateMode mode )
            throws KeeperException, InterruptedException
    {
        String [] nodes = path.split("/");

        String currentPath = "/";


        for ( int i = 1; i < nodes.length; i++ )
        {
            currentPath += nodes[i];
            Stat stat = zk.exists(currentPath, false);
            if ( stat == null )
            {
                zk.create( currentPath, new byte[0], acl, mode );
            }

            currentPath += "/";
        }

        return currentPath.substring( 0, currentPath.length() -1 );
    }

    /**
     * Delete a path and any children it has
     *
     * @param zk your zookeeper connection
     * @param path the path to delete along with that paths children
     * @return true if all deletes suceed false otherwise
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static boolean deletePathAndChildren( ZooKeeper zk, String path )
                    throws KeeperException, InterruptedException
    {
        return deletePath(zk, path, true);
    }

    /**
     * Deletes a path's children
     *
     * @param zk your zookeeper connection
     * @param path the path to delete the children of
     * @return true if all deletes suceed false otherwise
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static boolean deleteChildrenOfPath( ZooKeeper zk, String path )
                        throws KeeperException, InterruptedException
    {
        return deletePath(zk, path, false);
    }

    private static boolean deletePath( ZooKeeper zk, String path, boolean deleteParentWithChildren )
                throws KeeperException, InterruptedException
    {

        Stat stat = zk.exists( path, false );
        if ( stat != null )
        {
            List<String> children = zk.getChildren(path, false);
            for ( String child : children )
            {
                deletePath(zk, path + "/" + child, true);
            }

            if ( deleteParentWithChildren )
            {
                // delete whatever version you find
                zk.delete( path, -1);
            }
            return true;
        }

        return false;
    }

    /**
     * Helper function for printing trees of nodes
     *
     * @param zk your zookeeper connection
     * @param path the path root path to print and the tree beneath it
     * @return the string representing root passed in and its children
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static String getTreeAsString( ZooKeeper zk, String path ) throws KeeperException, InterruptedException
    {
        StringBuilder builder = new StringBuilder();

        if ( zk.exists(path, false) != null )
        {
           builder.append( path ).append("\n");
           List<String> children = zk.getChildren( path, false);

           for ( String child : children )
           {
               // in the case of the root of all the zookeeper tree don't appen a '/'
               String parentPath = path.equals("/") ? path : path + "/";
               builder.append( getTreeAsString(zk, parentPath + child) );
           }
        }

        return builder.toString();
    }
}
