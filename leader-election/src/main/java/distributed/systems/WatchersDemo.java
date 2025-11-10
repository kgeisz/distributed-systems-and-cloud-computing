package distributed.systems;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

public class WatchersDemo implements Watcher {
  private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
  private static final int SESSION_TIMEOUT = 3000;
  private static final String TARGET_ZNODE = "/target_znode";

  private ZooKeeper zooKeeper;

  public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
    WatchersDemo watchersDemo = new WatchersDemo();
    watchersDemo.connectToZookeeper();
    watchersDemo.watchTargetZnode();
    watchersDemo.run();
    watchersDemo.close();
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

  public void watchTargetZnode() throws InterruptedException, KeeperException {
    // Stat give us the metadata of the znode (creation time, number of children, etc.)
    Stat stat = zooKeeper.exists(TARGET_ZNODE, this);
    if (stat == null) {
      System.out.println("stat is null. The target znode does not exist: " + TARGET_ZNODE);
      return;
    }

    // Get the current data of the znode and register a watcher on any changes in this data
    byte[] data = zooKeeper.getData(TARGET_ZNODE, this, stat);
    // Get the znode's children and register a watcher on these children
    List<String> children = zooKeeper.getChildren(TARGET_ZNODE, this);

    System.out.println("Data: '" + new String(data) + "'; children: " + children);
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
        System.out.println(TARGET_ZNODE + " was deleted");
        break;
      case NodeCreated:
        System.out.println(TARGET_ZNODE + " was created");
        break;
      case NodeDataChanged:
        System.out.println(TARGET_ZNODE + " had its data changed");
        break;
      case NodeChildrenChanged:
        System.out.println(TARGET_ZNODE + " had changes to its children");
        break;
      default:
        System.out.println("Came across unhandled event type: " + event.getType().toString());
    }
    try {
      // Get all the up-to-date changes and display it
      // Also re-register the watchers for future events
      this.watchTargetZnode();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    }
  }
}
