#Zootil

Some utility classes for interacting with the Zookeeper framework. The ElectionWatcher class implements the basic algorithm defined in the Zookeeper [recipe](http://zookeeper.apache.org/doc/trunk/recipes.html#sc_leaderElection).

<b>Note:</b> this project uses java 1.8 which hasn't been released in a stable version yet

###Installation

To use the SimpleElection sample class you need to have a Zookeeper instance running on your localhost on port 2181. 

On Mac OsX with Homebrew:
    
    brew install zookeeper

then move the sample config over

    mv /usr/local/etc/zookeeper/zoo_sample.cfg /usr/local/etc/zookeeper/zoo.cfg 
    
start Zookeeper (your version might be different)
    
    /usr/local/Cellar/zookeeper/3.4.5/bin/zkServer start
    
Once zookeeper is installed and running just run multiple instances of the jar and try randomly killing them. Also try stop Zookeeper to see what happens.

###Example Usage

     java -jar zootil.jar
