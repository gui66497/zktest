package com.zzjz.zktest;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ZktestApplicationTests {

    private static final String CONNECT_URL = "vm1:2181,vm2:2181,vm3:2181";

    private static final int SESSION_TIME_OUT = 2000;

    ZooKeeper zkCli = null;

    @Before
    public void init() throws Exception{
        zkCli = new ZooKeeper(CONNECT_URL, SESSION_TIME_OUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event.getType()+"-----------"+event.getPath());
                try{
                    zkCli.getChildren("/", true);
                }catch (Exception e){

                }

            }
        });
    }

    /**
     * @Description 添加节点数据
     * @Author 刘俊重
     */
    @Test
    public void create() throws Exception{
        // 参数1：要创建的节点的路径 参数2：节点大数据 参数3：节点的权限 参数4：节点的类型。上传的数据可以是任何类型，但都要转成byte[]
        String s = zkCli.create("/zk", "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * @Description 判断节点是否存在
     * @Author 刘俊重
     */
    @Test
    public void isExist() throws Exception{
        Stat exists = zkCli.exists("/servers", false);
        System.out.println(null==exists?"不存在":"存在");
    }

    /**
     * @Description 获取节点数据
     * @Author 刘俊重
     */
    @Test
    public void getData() throws Exception{
        byte[] data = zkCli.getData("/zk", false, null);
        System.out.println("节点数据："+new String(data));
    }

    /**
     * @Description 遍历节点数据
     * @Author 刘俊重
     */
    @Test
    public void getChildren() throws  Exception{
        List<String> children = zkCli.getChildren("/", false);
        for(String s : children){
            System.out.println("节点名称："+s);
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * @Description 删除节点数据
     * @Author 刘俊重
     */
    @Test
    public void delete() throws Exception{
        //参数2：指定要删除的版本，-1表示删除所有版本
        zkCli.delete("/zk",-1);
        this.isExist();
    }

    /**
     * @Description 更新节点数据
     * @Author 刘俊重
     */
    @Test
    public void update() throws Exception{
        Stat stat = zkCli.setData("/zk", "newtest".getBytes(), -1);
        this.getData();
    }

    @Test
    public void contextLoads() {
    }

}
