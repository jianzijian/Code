package sharedsentinel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Hashing;
import redis.clients.util.Pool;

public class ShardedSentinelPool extends Pool<ShardedJedis> {

	private final List<JedisShardInfo> jedisShardInfos;
	private final GenericObjectPoolConfig poolConfig;
	private final List<ShardSentinel> shardSentinels = new ArrayList<>();

	public ShardedSentinelPool(List<ShardSentinelInfo> sentinelInfos, List<BaseJedisShardInfo> jedisShardInfos,
			GenericObjectPoolConfig poolConfig) {
		for (int index = 0; index < sentinelInfos.size(); index++) {
			ShardSentinel shardSentinel = new ShardSentinel(index, sentinelInfos.get(index));
			shardSentinels.add(shardSentinel);
			jedisShardInfos.get(index).setHostAndPort(shardSentinel.currentMaster);
		}
		this.jedisShardInfos = new ArrayList<>(jedisShardInfos);
		this.poolConfig = poolConfig;
		this.initPool();
	}

	private void initPool() {
		initPool(poolConfig, new SharedSentinelFactory());
	}

	private void onMasterSwitch(int index, HostAndPort currentHostAndPort) {
		BaseJedisShardInfo baseJedisShardInfo = (BaseJedisShardInfo) jedisShardInfos.get(index);
		// 初始化阶段切换节点？还是处理一下
		if (baseJedisShardInfo == null) {
			return;
		}
		HostAndPort oldHostAndPort = baseJedisShardInfo.getHostAndPort();
		if (!oldHostAndPort.equals(currentHostAndPort)) {
			baseJedisShardInfo.setHostAndPort(currentHostAndPort);
			// clear idle instances,but the borrowed instances still need to be
			// check while getSource
			internalPool.clear();
		}
	}

	@SuppressWarnings("deprecation")
	@Override
	public ShardedJedis getResource() {
		while (true) {
			ShardedJedis shardedJedis = super.getResource();
			Set<HostAndPort> currentHostAndPorts = new HashSet<>();
			for (ShardSentinel shardSentinel : shardSentinels) {
				currentHostAndPorts.add(shardSentinel.currentMaster);
			}
			Set<HostAndPort> tmpHostAndPorts = new HashSet<>();
			for (Jedis jedis : shardedJedis.getAllShards()) {
				HostAndPort connection = new HostAndPort(jedis.getClient().getHost(), jedis.getClient().getPort());
				tmpHostAndPorts.add(connection);
			}
			// if cannot match all hostAndPort,abandon the instance
			if (currentHostAndPorts.containsAll(tmpHostAndPorts)) {
				// connected to the correct master
				shardedJedis.setDataSource(this);
				return shardedJedis;
			} else {
				returnBrokenResource(shardedJedis);
			}
		}
	}

	@Override
	public void destroy() {
		for (ShardSentinel shardSentinel : shardSentinels) {
			shardSentinel.shutdown();
		}

		super.destroy();
	}

	private class ShardSentinel {

		private final int index;
		private final Set<MasterListener> masterListeners = new HashSet<MasterListener>();
		private volatile HostAndPort currentMaster;

		private ShardSentinel(int index, ShardSentinelInfo sentinelInfo) {
			this.index = index;
			this.currentMaster = this.initSentinels(sentinelInfo.ipAndPorts, sentinelInfo.masterName);
		}

		private HostAndPort initSentinels(Set<String> sentinels, final String masterName) {
			HostAndPort master = null;
			boolean sentinelAvailable = false;
			for (String sentinel : sentinels) {
				final HostAndPort hap = HostAndPort.parseString(sentinel);
				Jedis jedis = null;
				try {
					jedis = new Jedis(hap.getHost(), hap.getPort());
					List<String> masterAddr = jedis.sentinelGetMasterAddrByName(masterName);
					sentinelAvailable = true;
					if (masterAddr == null || masterAddr.size() != 2) {
						continue;
					}
					master = toHostAndPort(masterAddr);
					break;
				} catch (JedisException e) {
				} finally {
					if (jedis != null) {
						jedis.close();
					}
				}
			}
			if (master == null) {
				if (sentinelAvailable) {
					throw new JedisException(
							"Can connect to sentinel, but " + masterName + " seems to be not monitored...");
				} else {
					throw new JedisConnectionException(
							"All sentinels down, cannot determine where is " + masterName + " master is running...");
				}
			}
			for (String sentinel : sentinels) {
				final HostAndPort hap = HostAndPort.parseString(sentinel);
				MasterListener masterListener = new MasterListener(masterName, hap.getHost(), hap.getPort());
				masterListener.setDaemon(true);
				masterListeners.add(masterListener);
				masterListener.start();
			}
			return master;
		}

		private HostAndPort toHostAndPort(List<String> getMasterAddrByNameResult) {
			String host = getMasterAddrByNameResult.get(0);
			int port = Integer.parseInt(getMasterAddrByNameResult.get(1));
			return new HostAndPort(host, port);
		}

		private void shutdown() {
			for (MasterListener listener : masterListeners) {
				listener.shutdown();
			}
		}

		private class MasterListener extends Thread {

			private String masterName;
			private String host;
			private int port;
			private long subscribeRetryWaitTimeMillis = 5000;
			private volatile Jedis j;
			private AtomicBoolean running = new AtomicBoolean(false);

			private MasterListener(String masterName, String host, int port) {
				super(String.format("MasterListener-%s-[%s:%d]", masterName, host, port));
				this.masterName = masterName;
				this.host = host;
				this.port = port;
			}

			@Override
			public void run() {
				running.set(true);
				while (running.get()) {
					j = new Jedis(host, port);
					try {
						if (!running.get()) {
							break;
						}
						j.subscribe(new JedisPubSub() {
							@Override
							public void onMessage(String channel, String message) {
								String[] switchMasterMsg = message.split(" ");
								if (switchMasterMsg.length > 3) {
									if (masterName.equals(switchMasterMsg[0])) {
										// master node change
										currentMaster = toHostAndPort(
												Arrays.asList(switchMasterMsg[3], switchMasterMsg[4]));
										onMasterSwitch(index, currentMaster);
									} else {

									}
								} else {

								}
							}
						}, "+switch-master");
					} catch (JedisConnectionException e) {
						if (running.get()) {
							try {
								Thread.sleep(subscribeRetryWaitTimeMillis);
							} catch (InterruptedException e1) {

							}
						} else {

						}
					} finally {
						j.close();
					}
				}
			}

			public void shutdown() {
				try {
					running.set(false);
					// This isn't good, the Jedis object is not thread safe
					if (j != null) {
						j.disconnect();
					}
				} catch (Exception e) {
				}
			}

		}
	}

	private class SharedSentinelFactory implements PooledObjectFactory<ShardedJedis> {

		@Override
		public void activateObject(PooledObject<ShardedJedis> pooledShardedJedis) throws Exception {
			// TODO Auto-generated method stub
		}

		@Override
		public void destroyObject(PooledObject<ShardedJedis> pooledShardedJedis) throws Exception {
			ShardedJedis shardedJedis = pooledShardedJedis.getObject();
			for (Jedis jedis : shardedJedis.getAllShards()) {
				try {
					try {
						jedis.quit();
					} catch (Exception e) {

					}
					jedis.disconnect();
				} catch (Exception e) {

				}
			}
		}

		@Override
		public PooledObject<ShardedJedis> makeObject() throws Exception {
			ShardedJedis jedis = new ShardedJedis(jedisShardInfos, Hashing.MURMUR_HASH);
			return new DefaultPooledObject<ShardedJedis>(jedis);
		}

		@Override
		public void passivateObject(PooledObject<ShardedJedis> pooledShardedJedis) throws Exception {
			// TODO Auto-generated method stub

		}

		@Override
		public boolean validateObject(PooledObject<ShardedJedis> pooledShardedJedis) {
			try {
				ShardedJedis jedis = pooledShardedJedis.getObject();
				for (Jedis shard : jedis.getAllShards()) {
					if (!shard.ping().equals("PONG")) {
						return false;
					}
				}
				return true;
			} catch (Exception ex) {
				return false;
			}
		}

	}

}
