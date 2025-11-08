package distributed.systems;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class LeaderElection implements Watcher {
  private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
  private static final int SESSION_TIMEOUT = 3000;
  private ZooKeeper zooKeeper;

  public static void main(String[] args) throws IOException, InterruptedException {
    LeaderElection leaderElection = new LeaderElection();
    leaderElection.connectToZookeeper();
    leaderElection.run();
    leaderElection.close();
    System.out.println("Disconnected from Zookeeper, exiting application");
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
    }
  }
}
