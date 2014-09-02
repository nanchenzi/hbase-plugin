package org.apache.hadoop.hbase.master.balancer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.hbase.master.HMaster;

/**
 * 
 * @author ly
 * 
 *         <p>
 *         自定义LoadBalancer策略，目的在于达到不同应用场景之间的表级regionserver隔离
 *         </p>
 * 
 *         <ul>
 *         <li>hbase-loadbalancer.xml添加配置属性</li>
 *         <li>loadbalancer.regionserver.group   regionServer的分组名称</li>
 *         <li>loadbalancer.regionserver.group.mapping.组名称  该组内包含的regionserver </li>
 *         <li>loadbalancer.table.group.mapping.组名称  该组内拥有哪些表</li>
 *         <li>loadbalancer.table.regionserver.default  未分配的组的表默认存放的rs</li>
 *         </ul>
 * 
 *         <ul>
 *         <li>一个组内至少拥有三个rs，当组内没有有效的regionserver 则不进行loadbalancer，当rs下线后默认会把region random到所有节点上</li>
 *         <li>一张表仅能属于一个组，未分配组的表默认迁移到default的regionserver上</li>
 *         <li>当有region在分配的组之外或者未分配组的region不在default region上，则先将超出的范围的region以随机的方式迁回所在的组或者default regionserver上</li>
 *         <li>当所有的region在自己的组内或者default regionserver上运行，则分别对组内的server采用stochasticLoadBalancer进行load balancer</li>
 *         </ul>
 * 
 *         <ul>
 *         <li><name>hbase.master.loadbalancer.class</name> 自定义loadbalancer接口参数</li>
 *         <li><name>hbase.balancer.period</name> balancer运行周期时间 改为半个小时进行一次 1800000</li>
 *         <li><name>hbase.master.loadbalance.bytable</name>  误将该参数开启，不采用表级的loadbalancer</li>
 *         </ul>
 * 
 */

public class TableIsolationLoadBalancer extends BaseLoadBalancer {

	private static final Log LOG = LogFactory
			.getLog(TableIsolationLoadBalancer.class);

	private static final String GROUP_NAME = "loadbalancer.regionserver.group";
	private static final String RS_GROUP_MAPPING_PREFIX_NAME = "loadbalancer.regionserver.group.mapping.";
	private static final String TABLE_GROUP_MAPPING_PREFIX_NAME = "loadbalancer.table.group.mapping.";
	private static final String TABLE_TO_REGIONSERVER_DEFAULT = "loadbalancer.table.regionserver.default";

	private static final Random RANDOM_FUNC = new Random(
			EnvironmentEdgeManager.currentTimeMillis());
	private Configuration config = null;
	private StochasticLoadBalancer stochasticLoadBalancer = null;

	private Map<String, List<String>> groupMappingRS; // rs组对应的regionserver名称的映射关系
	private Map<String, List<ServerName>> groupMappingServerName; // rs组对应的ServerName的映射关系
	private Map<String, String> tableToGroupMapping; // table分配至rsGroup的映射
	private Map<String, List<String>> groupToTableMapping; // group至分配的table的映射关系
	private Set<ServerName> assignRegionServer; // have assign regionserver

	private List<String> unAssignTables; // 未分配Group的表
	private List<String> unKnowRegionServer; // 集群中不存在的regionserveer
	private List<String> unConfigRegionSever; // 配置文件中不存在的regionserver
	private List<String> unKnowTable; // 未知的table表集合
	private ServerName defaultServerName; // backup的regionserver用于存储未分配的表

	private ServerName backUpRegionServer;

	@Override
	public List<RegionPlan> balanceCluster(
			Map<ServerName, List<HRegionInfo>> clusterState)
			throws HBaseIOException {
		// TODO Auto-generated method stub

		if (stochasticLoadBalancer == null) {
			stochasticLoadBalancer = ReflectionUtils.newInstance(
					StochasticLoadBalancer.class, this.getConf());
			stochasticLoadBalancer.setClusterStatus(((HMaster) this.services)
					.getClusterStatus());
			stochasticLoadBalancer.setMasterServices(this.services);
			try {
				this.stochasticLoadBalancer.initialize();
			} catch (HBaseIOException e) {
				// TODO Auto-generated catch block
				LOG.error("init stochasticLoadBalancer error!");
				LOG.error(e.getMessage());
			}

		}

		long startTime = EnvironmentEdgeManager.currentTimeMillis();
		reloadConfigCollections();

		// 加载config配置
		try {
			config = loadbalancerConfig();
		} catch (Exception e) {
			LOG.warn(e.getMessage() + "\n  loadbalancerConfig should have {"
					+ GROUP_NAME + "," + RS_GROUP_MAPPING_PREFIX_NAME + ","
					+ TABLE_GROUP_MAPPING_PREFIX_NAME + ","
					+ TABLE_TO_REGIONSERVER_DEFAULT + "} config property");
			return null;
		}

		// 添加regionserver分组名称
		for (String groupName : config.get(GROUP_NAME).split(",")) {
			groupMappingRS.put(groupName, new ArrayList<String>());
		}
		if (groupMappingRS.isEmpty()) {
			LOG.warn("loadbalancerConfig rsGroup is empty!");
			return null;
		}

		// 添加regionserver至分组中，每组至少有三台机器
		for (String groupName : groupMappingRS.keySet()) {
			String rsToGroupMappingKey = RS_GROUP_MAPPING_PREFIX_NAME
					+ groupName;
			if (config.get(rsToGroupMappingKey) == null) {
				LOG.warn("loadbalancerConfig group : " + rsToGroupMappingKey
						+ " config is missing!");
				return null;
			}
			for (String rsName : config.get(rsToGroupMappingKey).split(",")) {
				groupMappingRS.get(groupName).add(rsName);
			}
			if (groupMappingRS.get(groupName).size() < 3) {
				LOG.warn("loadbalancerConfig  " + groupName
						+ "group must have at least three regionserver");
				return null;
			}

		}

		// 添加group to serverName mapping
		Map<String, ServerName> hostNameToServerNameMapping = getHostNameToServerNameMapping(this.services
				.getServerManager().getOnlineServers());
		for (String groupName : groupMappingRS.keySet()) {
			List<ServerName> serverNameList = new ArrayList<ServerName>();
			for (String rsName : groupMappingRS.get(groupName)) {
				ServerName serverName = hostNameToServerNameMapping.get(rsName);
				if (serverName == null) {
					unKnowRegionServer.add(rsName);
					continue;
				}
				if (assignRegionServer.contains(serverName)) {
					LOG.warn("loadbalancerConfig  " + serverName.getHostname()
							+ " have assign to a group, can't assign again!");
					return null;
				}
				assignRegionServer.add(serverName);
				serverNameList.add(serverName);
			}
			if (serverNameList.size() == 0) {
				LOG.warn("loadbalancerConfig  "
						+ groupName
						+ " group have no live regionServer, refused to do loadBalancer!");
				return null;

			}
			groupMappingServerName.put(groupName, serverNameList);
		}

		// 添加表对应的rs组的mapping
		for (String groupName : groupMappingRS.keySet()) {
			String groupContainsTableKey = TABLE_GROUP_MAPPING_PREFIX_NAME
					+ groupName;
			if (config.get(groupContainsTableKey) == null) {
				LOG.warn("loadbalancerConfig tableMappingGroup : "
						+ groupContainsTableKey + " config is missing!");
				return null;
			}
			List<String> tableNameList = new ArrayList<String>();
			for (String tableName : config.get(groupContainsTableKey)
					.split(",")) {
				tableNameList.add(tableName);
				if (tableToGroupMapping.get(tableName) != null) {
					LOG.warn("loadbalancerConfig  " + tableName
							+ " have been assign to a group ,check the config!");
					return null;
				}
				tableToGroupMapping.put(tableName, groupName);
			}
			groupToTableMapping.put(groupName, tableNameList);
		}

		// 添加defaultServerName
		if (config.get(TABLE_TO_REGIONSERVER_DEFAULT) == null) {
			LOG.warn("loadbalancerConfig should have { "
					+ TABLE_TO_REGIONSERVER_DEFAULT + "} config property");
		}
		for (String defaultRegionServerName : config.get(
				TABLE_TO_REGIONSERVER_DEFAULT).split(",")) {
			defaultServerName = hostNameToServerNameMapping
					.get(defaultRegionServerName);
			if (defaultServerName != null) {
				break;
			}
		}
		if (defaultServerName == null) {
			LOG.warn("defaultServerName have no  right ServerName or no online serverName, refused to load balancer!");
			return null;
		}

		List<RegionPlan> plans = new LinkedList<RegionPlan>();

		// 验证loadbalancer是否outofbound
		boolean haveTableOutOfBound = false;
		for (ServerName localServerName : clusterState.keySet()) {
			List<HRegionInfo> listRegion = clusterState.get(localServerName);
			if (!assignRegionServer.contains(localServerName)) {
				unConfigRegionSever.add(localServerName.getServerName());
			}
			for (HRegionInfo region : listRegion) {
				String tableName = region.getTable().getNameAsString();
				String groupName = tableToGroupMapping.get(tableName);

				if (groupName == null || "".equals(groupName)) {
					// 属于未被分配groupName的表,判断是否在default regionserver上
					if (localServerName.equals(defaultServerName))
						continue;
					haveTableOutOfBound = true;
					unAssignTables.add(tableName);
					addRegionPlan(plans, region, localServerName,
							defaultServerName);
					continue;
				}

				List<ServerName> groupHaveServerNameList = groupMappingServerName
						.get(groupName);
				boolean justInGroupServerNameList = false; // 表是否在分配的组内
				for (ServerName groupPeerServerName : groupHaveServerNameList) {
					if (groupPeerServerName.equals(localServerName))
						justInGroupServerNameList = true;
				}
				if (!justInGroupServerNameList) {
					haveTableOutOfBound = true;
					addRegionPlan(plans, region, localServerName,
							randomPickRegionServer(groupHaveServerNameList));
				}

			}
		}

		if (haveTableOutOfBound) {
			long endTime = EnvironmentEdgeManager.currentTimeMillis();
			printLoadBalancerHelpMessage();
			LOG.info("Do a TableOutOfBound loadBalancer! Calculated a load balance in "
					+ (endTime - startTime)
					+ "ms. "
					+ "Moving "
					+ plans.size()
					+ " region ! ");

			return plans;
		}

		// do group StochasticLoadBalancer
		Map<ServerName, List<HRegionInfo>> groupClusterState = null;

		for (String groupName : groupMappingServerName.keySet()) {
			groupClusterState = new HashMap<ServerName, List<HRegionInfo>>();
			List<ServerName> serverNameList = groupMappingServerName
					.get(groupName);
			for (ServerName serverNameKey : serverNameList) {
				groupClusterState.put(serverNameKey,
						clusterState.get(serverNameKey));
			}
			List<RegionPlan> stochasticPlan = stochasticLoadBalancer
					.balanceCluster(groupClusterState);
			if (stochasticPlan == null || stochasticPlan.size() == 1) {
				LOG.warn("stochasticLoadBalancer has no RegionPlan to move!");
			} else {
				plans.addAll(stochasticPlan);
			}

		}

		long endTime = EnvironmentEdgeManager.currentTimeMillis();
		printLoadBalancerHelpMessage();
		LOG.info("Do a groupStochastic loadBalancer! Calculated a load balance in "
				+ (endTime - startTime)
				+ "ms. "
				+ "Moving "
				+ plans.size()
				+ " region ! ");

		return plans;
	}

	/**
	 * Creates a Configuration with hbase-loadbalancer.xml resources
	 * 
	 * @return a Configuration with hbase-loadbalancer.xml resources
	 */
	private static Configuration loadbalancerConfig() throws Exception {
		Configuration conf = new Configuration();
		conf.addResource("hbase-loadbalancer.xml");

		if (conf.get(GROUP_NAME) == null
				|| conf.get(TABLE_TO_REGIONSERVER_DEFAULT) == null)
			throw new Exception(
					"loadbalancerConfig is missing some config property.");
		return conf;
	}

	/**
	 * get hostname to serverName mapping map
	 * 
	 * @param onlineServers
	 * @return
	 */
	private static Map<String, ServerName> getHostNameToServerNameMapping(
			Map<ServerName, ServerLoad> onlineServers) {
		Map<String, ServerName> mapping = new HashMap<String, ServerName>();
		for (ServerName serverName : onlineServers.keySet()) {
			mapping.put(serverName.getHostname(), serverName);
		}
		return mapping;
	}

	private void reloadConfigCollections() {
		groupMappingRS = new HashMap<String, List<String>>();
		groupMappingServerName = new HashMap<String, List<ServerName>>();
		assignRegionServer = new HashSet<ServerName>();
		tableToGroupMapping = new HashMap<String, String>();
		groupToTableMapping = new HashMap<String, List<String>>();
		defaultServerName = null;

		unKnowRegionServer = new ArrayList<String>();
		unConfigRegionSever = new ArrayList<String>();
		unAssignTables = new ArrayList<String>();
		unKnowTable = new ArrayList<String>();
		backUpRegionServer = null;
	}

	private List<RegionPlan> addRegionPlan(List<RegionPlan> plans,
			HRegionInfo regionInfo, ServerName sourceServerName,
			ServerName destServerName) {
		RegionPlan rp = new RegionPlan(regionInfo, sourceServerName,
				destServerName);
		plans.add(rp);
		return plans;
	}

	private ServerName randomPickRegionServer(List<ServerName> listRS) {
		if (listRS.size() == 1)
			return listRS.get(0);
		return listRS.get(Math.abs(RANDOM_FUNC.nextInt()) % listRS.size());
	}

	private void printLoadBalancerHelpMessage() {
		StringBuilder builder = new StringBuilder();
		builder.append("unKnowRegionServer size is ").append(
				unKnowRegionServer.size());
		if (unKnowRegionServer.size() > 0)
			builder.append(Arrays.toString(unKnowRegionServer.toArray()));
		builder.append("; unConfigRegionSever size is ").append(
				unConfigRegionSever.size());
		if (unConfigRegionSever.size() > 0)
			builder.append(Arrays.toString(unConfigRegionSever.toArray()));
		builder.append("; unAssignTables size is ").append(
				unAssignTables.size());
		if (unAssignTables.size() > 0)
			builder.append(Arrays.toString(unAssignTables.toArray()));

		LOG.info(builder.toString());
	}

}
