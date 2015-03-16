package com.epfl.tkvs;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;


public class AppMaster implements AMRMClientAsync.CallbackHandler{

	public static void main(String[] args) throws Exception {
		new AppMaster().run();
	}
	
	public void run() throws Exception {
		Configuration conf = new Configuration();
		
		AMRMClientAsync<ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(1000, this);
		rmClient.init(conf);
		rmClient.start();
		
		rmClient.registerApplicationMaster(NetUtils.getHostname(), 0, "");
		
		Thread.sleep(1000);
		
		rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
	}

	@Override
	public float getProgress() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void onContainersAllocated(List<Container> arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onContainersCompleted(List<ContainerStatus> arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onError(Throwable arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onNodesUpdated(List<NodeReport> arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onShutdownRequest() {
		// TODO Auto-generated method stub
		
	}
}
