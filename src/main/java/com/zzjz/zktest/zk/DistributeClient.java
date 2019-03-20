package com.zzjz.zktest.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author 房桂堂
 * @description DistributeClient
 * @date 2019/3/20 14:10
 */
public class DistributeClient {
    private static final String CONNECT_URL = "vm1:2181,vm2:2181,vm3:2181";

    //超过2秒就认为挂了
    private static final int sessionTimeOut = 2000;
    //父节点
    private static final String parentNode = "/servers";
    //提供给各业务线程使用的服务器列表
    private volatile List<String> serverList;
    private ZooKeeper zk = null;

    //latch就相当于一个对象,当latch.await()方法执行时,线程就会等待
    //当latch的count减为0的时候,将会唤醒等待的线程
    //让主线程阻塞掉
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    //创建到zk的客户端连接
    public void getConnect() throws IOException, InterruptedException {
        //一new完就往下走,但是这时候客户端还没完成连接,所以我们要等他创建好
        //一旦成功握手这边的process就会回调一次
        zk = new ZooKeeper(CONNECT_URL, sessionTimeOut, new Watcher() {
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
                    //重新更新服务器列表，并且注册了监听
                    getServerList();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });
        countDownLatch.await();
    }

    //获取服务器信息列表
    private void getServerList() throws Exception {
        //获取服务器子节点信息,并且对父节点进行监听
        List<String> children = zk.getChildren(parentNode, true);

        //先创建一个局部的list来存服务器信息
        List<String> servers = new ArrayList<String>();
        for (String child : children) {
            //child只是子节点的节点名,我们获取数据要给全路径
            //结果是服务器的名字
            byte[] data = zk.getData(parentNode + "/" + child, false, null);
            servers.add(new String(data));
        }

        //把servers赋值给成员变量serverList,已提供给各业务线程使用
        //避免别的线程正在看的时候,这边正在写,所以弄个赋值
        serverList = servers;

        //打印服务器列表
        System.out.println(serverList);
    }

    //业务线程
    public void handleBussiness() throws InterruptedException {
        //这边业务就是打印一句话,那个ip开始工作了
        System.out.println("client start working ...");

        //让主线程sleep
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {
        //获取zk连接
        DistributeClient client = new DistributeClient();
        client.getConnect();
        //获取servers的子节点信息(并监听),从中获取服务器信息列表
        client.getServerList();

        //业务线程启动
        client.handleBussiness();
    }

}
