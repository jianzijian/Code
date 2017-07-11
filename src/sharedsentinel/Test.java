package sharedsentinel;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.ShardedJedis;

public class Test {

	public static void main(String[] args) {
		List<ShardSentinelInfo> sentinelInfos = new ArrayList<>();
		Set<String> ipAndPorts = new HashSet<>();
		ipAndPorts.add("10.18.8.10:26379");
		sentinelInfos.add(new ShardSentinelInfo("firstnode", ipAndPorts));
		sentinelInfos.add(new ShardSentinelInfo("secondnode", ipAndPorts));

		List<BaseJedisShardInfo> jedisShardInfos = new ArrayList<>();
		jedisShardInfos.add(new BaseJedisShardInfo());
		jedisShardInfos.add(new BaseJedisShardInfo());

		JedisPoolConfig poolConfig = new JedisPoolConfig();

		ShardedSentinelPool pool = new ShardedSentinelPool(sentinelInfos, jedisShardInfos, poolConfig);

		for (int index = 0; index < 100; index++) {
			ShardedJedis shardedJedis = pool.getResource();
			shardedJedis.set(String.valueOf(index), String.valueOf(index));
			shardedJedis.close();
		}

		while (true) {
			ShardedJedis shardedJedis = pool.getResource();
			try {
				System.out.println(shardedJedis.get("58"));
			} catch (Exception e) {
				System.err.println("get key 58 failed!");
			}

			try {
				System.out.println(shardedJedis.get("62"));
			} catch (Exception e) {
				System.err.println("get key 62 failed!");
			}

			try {
				System.out.println();
				Thread.sleep(1000);
			} catch (Exception e) {
				// eat the exception
			} finally {
				shardedJedis.close();
			}
		}

	}

}
