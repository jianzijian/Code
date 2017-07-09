package sharedsentinel;

import java.util.concurrent.atomic.AtomicReference;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisShardInfo;

public class BaseJedisShardInfo extends JedisShardInfo {

	private final AtomicReference<HostAndPort> hostAndPort = new AtomicReference<HostAndPort>();

	public BaseJedisShardInfo() {
		super("");
	}

	public void setHostAndPort(HostAndPort hostAndPort) {
		this.hostAndPort.set(hostAndPort);
	}

	public HostAndPort getHostAndPort() {
		return hostAndPort.get();
	}

}
