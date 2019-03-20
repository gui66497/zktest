package com.zzjz.zktest.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author 房桂堂
 * @description DistributeServer
 * @date 2019/3/20 14:07
 */
public class DistributeServer {
    private static final String CONNECT_URL = "vm1:2181,vm2:2181,vm3:2181";

    //超过2秒就认为挂了
    private static final int SESSION_TIME_OUT = 2000;
    //父节点
    private static final String PARENT_NODE = "/servers";

    private ZooKeeper zk = null;

    //latch就相当于一个对象,当latch.await()方法执行时,线程就会等待
    //当latch的count减为0的时候,将会唤醒等待的线程
    //让主线程阻塞掉
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    //创建到zk客户端的连接
    private void getConnect() throws IOException, InterruptedException {
        //一new完就往下走,但是这时候客户端还没完成连接,所以我们要等他创建好
        //一旦成功握手这边的process就会回调一次
        zk = new ZooKeeper(CONNECT_URL, SESSION_TIME_OUT, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                //SyncConnected同步连接
                //回调了并且事件等于连接成功
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    //把计数减少,然后主线程就可以往下走了
                    countDownLatch.countDown();
                }
                //收到事件通知后的回调函数(应该是我们自己的事件处理逻辑)
                System.out.println(watchedEvent.getType() + "---" + watchedEvent.getPath());

                try {
                    //再次注册监听,监听/下面的
                    zk.getChildren("/", true);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });
        countDownLatch.await();
    }

    //向zk集群注册服务信息
    private void registerServer(String hostname) throws KeeperException, InterruptedException {
        //判断这个父节点是否存在
        Stat exists = zk.exists(PARENT_NODE, false);
        /*
         * Ids.OPEN_ACL_UNSAFE 创建开放节点，允许任意操作
         * Ids.READ_ACL_UNSAFE 创建只读节点
         * Ids.CREATOR_ALL_AC  /创建者有全部权限
         * */
        if (exists == null) {
            //如果这个父节点/servers不存在就把他创建出来,父节点需要持久的,其他注册过来的就用短暂的
            zk.create(PARENT_NODE, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        //创建子节点,并且这个子节点后面是跟序号的,在子节点的名称后面加上一串数字后缀,避免冲突,而且子节点是短暂的
        String path = zk.create(PARENT_NODE + "/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        //打印下表示这台服务器上线了
        System.out.println(hostname + "is online..." + path);
    }

    //业务功能
    public void handleBussiness(String hostname) throws InterruptedException {
        //这边业务就是打印一句话,那个ip开始工作了
        System.out.println(hostname + "start working ...");

        //让主线程sleep
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        DistributeServer server = new DistributeServer();
        //获取zk连接
        server.getConnect();

        //利用zk连接注册服务器信息(主机名),这边参数可能用args[0]来替代
        server.registerServer("192.168.3.31");

        //启动业务功能
        server.handleBussiness("192.168.3.31");
    }


}

