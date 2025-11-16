/*
 *  MIT License
 *
 *  Copyright (c) 2019 Michael Pogrebinsky - Distributed Systems & Cloud Computing with Java
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

import org.apache.zookeeper.*;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class Autohealer implements Watcher {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;

    // Parent Znode where each worker stores an ephemeral child to indicate it is alive
    private static final String AUTOHEALER_ZNODES_PATH = "/workers";

    // Path to the worker jar
    private final String pathToProgram;

    // The number of worker instances we need to maintain at all times
    private final int numberOfWorkers;
    private ZooKeeper zooKeeper;

    public Autohealer(int numberOfWorkers, String pathToProgram) {
        this.numberOfWorkers = numberOfWorkers;
        this.pathToProgram = pathToProgram;
    }

    public void startWatchingWorkers() throws KeeperException, InterruptedException, IOException {
        if (zooKeeper.exists(AUTOHEALER_ZNODES_PATH, false) == null) {
            zooKeeper.create(AUTOHEALER_ZNODES_PATH, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        launchWorkersIfNecessary();
    }

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent event) {
//        String path = event.getPath();
//        try {
//            List<String> children = this.zooKeeper.getChildren(event.getPath(), false);
//        } catch (InterruptedException | KeeperException e) {
//            throw new RuntimeException(e);
//        }
      switch (event.getType()) {
            case None:
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Auto-healer: Successfully connected to Zookeeper");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Auto-healer: Disconnected from Zookeeper event");
                        zooKeeper.notifyAll();
                    }
                }
                break;
            /**
             * Add states code here to respond to the relevant events
             */
            case NodeDeleted:
                System.out.println("Auto-healer: A node was deleted");
                break;
            case NodeCreated:
                System.out.println("Auto-healer: A node was created");
                break;
            case NodeDataChanged:
                System.out.println("Auto-healer: A node had its data changed");
                break;
            case NodeChildrenChanged:
                System.out.println("Auto-healer: A node had changes to its children");
                launchWorkersIfNecessary();
                break;
            default:
                System.out.println("Auto-healer: Came across unhandled event type: " + event.getType().toString());
        }
    }

    private void launchWorkersIfNecessary() {
        /**
         * Implement this method to watch and launch new workers if necessary
         */
        try {
            List<String> children = this.zooKeeper.getChildren(AUTOHEALER_ZNODES_PATH, this);
            System.out.println("The current number of children is: " + children.size());
            if (children.size() < this.numberOfWorkers) {
                startNewWorker();
            }
//            while (children.size() < this.numberOfWorkers) {
//                startNewWorker();
//                children = this.zooKeeper.getChildren(AUTOHEALER_ZNODES_PATH, false);
//            }
        } catch (InterruptedException | KeeperException | IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Helper method to start a single worker
     * @throws IOException
     */
    private void startNewWorker() throws IOException {
        System.out.println("Starting a new worker");
        File flakyWorkerJarFile = new File(pathToProgram);
        ProcessBuilder processBuilder =
                new ProcessBuilder("java","-jar",flakyWorkerJarFile.getCanonicalPath());

        File log = new File("log.txt");
        processBuilder.redirectErrorStream(true);
        processBuilder.redirectOutput(ProcessBuilder.Redirect.appendTo(log));
        System.out.println("Launching new worker instance");
        processBuilder.start();

    }
}
