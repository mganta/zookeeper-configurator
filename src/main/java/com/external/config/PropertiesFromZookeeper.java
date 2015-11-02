package com.external.config;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.netflix.config.DynamicWatchedConfiguration;
import com.netflix.config.jmx.ConfigJMXManager;
import com.netflix.config.source.ZooKeeperConfigurationSource;

public class PropertiesFromZookeeper extends Thread {
	private static CuratorFramework client;
	private static ZooKeeperConfigurationSource zkConfigSource;
	private static String zkConfigRootPath = "/MyApp/config";
	private static String zkConnectionString = "localhost:2181";

	/*
	 * A simple looping program to read the properties from ZK
	 */
	public PropertiesFromZookeeper() throws Exception {
		client = CuratorFrameworkFactory.newClient(zkConnectionString,
				new ExponentialBackoffRetry(1000, 3));
		client.start();
		zkConfigSource = new ZooKeeperConfigurationSource(client,
				zkConfigRootPath);
		zkConfigSource.start();

		DynamicWatchedConfiguration zkDynamicConfig = new DynamicWatchedConfiguration(
				zkConfigSource);
		ConfigurationManager.install(zkDynamicConfig);
		registerMBean(zkDynamicConfig);
	}

	private void registerMBean(DynamicWatchedConfiguration zkDynamicConfig) {
		setDaemon(false);
		ConfigJMXManager.registerConfigMbean(zkDynamicConfig);
	}

	public String getStringProperty(String key, String defaultValue) {
		final DynamicStringProperty property = DynamicPropertyFactory
				.getInstance().getStringProperty(key, defaultValue);
		return property.get();
	}

	@Override
	public void run() {
		while (true) {
			try {
				sleep(100);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public static void main(String args[]) throws Exception {
		PropertiesFromZookeeper configurator = new PropertiesFromZookeeper();
		configurator.start();

		while (true) {
			try {
				System.out.println(configurator.getStringProperty(
						"consumer_stream_name", "default message"));
				System.out.println(configurator.getStringProperty(
						"key.deserializer", "default message"));
				System.out.println(configurator.getStringProperty(
						"consumer.id", "default message"));
				sleep(3000);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
