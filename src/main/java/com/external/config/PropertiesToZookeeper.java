package com.external.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.ExponentialBackoffRetry;

/*
 * Program to load a properties file into zk
 */
public class PropertiesToZookeeper {

	final static String nameSpace = "MyApp/config";
	final static String zkhost = "localhost:2181";
	static CuratorFramework zkclient = null;

	public PropertiesToZookeeper() {
		RetryPolicy rp = new ExponentialBackoffRetry(1000, 3);
		Builder builder = CuratorFrameworkFactory.builder()
				.connectString(zkhost).connectionTimeoutMs(5000)
				.sessionTimeoutMs(5000).retryPolicy(rp);
		builder.namespace(nameSpace);
		zkclient = builder.build();
		zkclient.newNamespaceAwareEnsurePath(nameSpace);
		zkclient.start();
	}

	public void createrOrUpdate(String node, String content) throws Exception {
		if (zkclient.checkExists().forPath(node) == null) {
			zkclient.create().forPath(node, content.getBytes());
			System.out.println("Added successfully!!!");
		} else if (!zkclient.getData().forPath(node).equals(content)) {
			zkclient.setData().forPath(node, content.getBytes());
			System.out.println("node exists...updating!!!");
		}
	}

	public void delete(String path) throws Exception {
		zkclient.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
		System.out.println("Successfully deleted!");
	}

	public boolean checkExist(String path) throws Exception {
		if (zkclient.checkExists().forPath(path) == null)
			return false;
		else
			return true;
	}

	public String read(String path) throws Exception {
		String data = new String(zkclient.getData().forPath(path), "null");
		return data;
	}

	public List<String> getListChildren(String path) throws Exception {
		List<String> paths = zkclient.getChildren().forPath(path);
		for (String p : paths) {
			System.out.println(p);
		}
		return paths;
	}

	public void upload(Properties properties) throws Exception {
		Enumeration<?> e = properties.propertyNames();
		while (e.hasMoreElements()) {
			String key = (String) e.nextElement();
			createrOrUpdate(key, properties.getProperty(key));
		}
	}

	public static void main(String[] args) throws Exception {
		PropertiesToZookeeper zkUpload = new PropertiesToZookeeper();
		InputStream in = new FileInputStream(new File(args[0]));
		Properties properties = new Properties();
		if (in != null) {
			properties.load(in);
			in.close();
		}
		zkUpload.upload(properties);
	}
}