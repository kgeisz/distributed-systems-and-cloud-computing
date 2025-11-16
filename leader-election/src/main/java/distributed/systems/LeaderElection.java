package distributed.systems;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {
  private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
  private static final int SESSION_TIMEOUT = 3000;
  private static final String ELECTION_NAMESPACE = "/election";

  private ZooKeeper zooKeeper;
  private String currentZnodeName;

  public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
    LeaderElection leaderElection = new LeaderElection();
    leaderElection.connectToZookeeper();
    leaderElection.volunteerForLeadership();
    leaderElection.reelectLeader();
    leaderElection.run();
    leaderElection.close();
    System.out.println("Disconnected from Zookeeper, exiting application");
  }

  public void volunteerForLeadership() throws InterruptedException, KeeperException {
    // "c" stands for "candidate"
    // A sequence number is appended to this prefix depending on the order of elected znodes
    // The name of the znode is appeneded to this prefix in the order of election with the parent znode
    String znodePrefix = ELECTION_NAMESPACE + "/c_";

    // The create method takes:
    //   1. znode prefix
    //   2. data we want to put inside znode (empty byte srray)
    //   3. Access Control List (ACL) - we don't care about restricted access, so we use open/unsafe
    //   4. Creation mode is ephemeral, which means if we disconnect from Zookeeper, then the znode is deleted
    String znodeFullPath = zooKeeper.create(
            znodePrefix, new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    System.out.println("The name of the newly created znode: " + znodeFullPath);

    // Get just the name of the znode (remove the parent path)
    this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
  }

  public void reelectLeader() throws InterruptedException, KeeperException {
    String predecessorZnodeName = "";
    Stat predecessorStat = null;

    // This while loop prevents a race condition that can occur when we get the child znode, but it goes down
    // before we can call exists() on it to establish a watcher
    while (predecessorStat == null) {
      // Get the children znode of the elected znode
      // Returns a list of znodes names that are children with this znode
      List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);

      // Now, we want to find which znode has the smallest number, so we sort the list in ascending order
      Collections.sort(children);
      String smallestChild = children.get(0);
      if (smallestChild.equals(this.currentZnodeName)) {
        System.out.println("I am " + this.currentZnodeName + ", and I am the leader");
        return;
      } else {
        // Find the predecessor znode in the hierarchy to figure out what znode we need to watch for failures
        // Use a fast binary search to find our index in the hierarchy, and then use it to get the predecessor's name
        // We're not the leader in this 'if' block, so we are guaranteed to have at least one predecessor.
        int predecessorIndex = Collections.binarySearch(children, this.currentZnodeName) - 1;
        predecessorZnodeName = children.get(predecessorIndex);
        // Call exists() on the full path of the znode we want to watch
        // By calling exists(), this particular znode will get the watcher notification if the predecessor znode is deleted
        predecessorStat = this.zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, this);
        System.out.println(
                "I am " + this.currentZnodeName + ", and I am NOT the leader. The current leader is: " + smallestChild);
      }
    }
    System.out.println("I am " + this.currentZnodeName + ", and I am watching " + predecessorZnodeName);
    System.out.println();
  }

  public void connectToZookeeper() throws IOException {
    this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
  }

  // Put the main thread into a wait state when it calls this method
  // This prevents the main thread from ending the application before we see anything from the Zookeeper server
  public void run() throws InterruptedException {
    synchronized (zooKeeper) {
      zooKeeper.wait();
    }
  }

  // Gracefully close all resources within the Zookeeper object
  public void close() throws InterruptedException {
    zooKeeper.close();
  }

  @Override
  public void process(WatchedEvent event) {
    switch (event.getType()) {
      // General Zookeeper connection events don't have a type, so we check the state when we get a None type
      case None:
        if (event.getState() == Event.KeeperState.SyncConnected) {
          System.out.println("Successfully connected to Zookeeper");
        } else {
          synchronized (zooKeeper) {
            System.out.println("Received an event to disconnect from Zookeeper");
            // Wake up the main thread so the application can exit
            zooKeeper.notifyAll();
          }
        }
        break;
      case NodeDeleted:
        System.out.println("Attempting to reelect a leader");
        try {
          reelectLeader();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        break;
      default:
        System.out.println("Came across unhandled event type: " + event.getType().toString());
    }
  }
}
