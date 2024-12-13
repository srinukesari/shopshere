package distributed.systems;

import org.apache.zookeeper.*;

import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {
    private static final String ZOOKEEPER_ADDR = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String ELECTION_NAMESPACE = "/election/order_management";

    private ZooKeeper zooKeeper;
    private String currentZnodeName;

    public void connectToZooKeeper() throws Exception {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDR, SESSION_TIMEOUT, this);
    }

    public void run() throws  InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait(40000);
        }
    }

    public void close() throws InterruptedException {
        try {
            this.zooKeeper.close();
        } catch (InterruptedException e) {
            System.out.println("unable to close ZooKeeper");
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to ZooKeeper");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected from ZooKeeper event");
                        zooKeeper.notify();
                    }
                }
        }
    }

    public void volunteerForLeaderElection() throws KeeperException, InterruptedException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_" + System.currentTimeMillis();
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{},
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        System.out.println("znode name " + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace("/election/order_management/", "");
    }

    public void electLeader() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
        Collections.sort(children);
        String smallestChild = children.get(0);
        System.out.println("list of znode -> " + children);

        if (smallestChild.equals(currentZnodeName)) {
            System.out.println("I am the Leader, " + currentZnodeName);
        }else {
            System.out.println("I am not the Leader, " + smallestChild + " is the elected leader");
        }
    }
}
