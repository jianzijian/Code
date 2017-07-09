package sharedsentinel;

import java.util.Set;

public class ShardSentinelInfo {

	public final String masterName;
	public final Set<String> ipAndPorts;

	public ShardSentinelInfo(String masterName, Set<String> ipAndPorts) {
		super();
		this.masterName = masterName;
		this.ipAndPorts = ipAndPorts;
	}

}
