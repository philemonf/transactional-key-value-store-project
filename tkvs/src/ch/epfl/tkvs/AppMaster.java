package ch.epfl.tkvs;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

public class AppMaster implements AMRMClientAsync.CallbackHandler {

	private int containerCount;
	private Configuration conf;
	private NMClient nmClient;

	public AppMaster(int containerCount) {
		this.containerCount = containerCount;
		conf = new YarnConfiguration();

		nmClient = NMClient.createNMClient();
		nmClient.init(conf);
		nmClient.start();
	}

	public boolean containersFinished() {
		return containerCount == 0;
	}

	public static void main(String[] args) throws Exception {
		new AppMaster(3).runMainLoop();
	}

	public void runMainLoop() throws Exception {
		AMRMClientAsync<ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(1000, this);
		rmClient.init(conf);
		rmClient.start();
		rmClient.registerApplicationMaster("", 0, "");
		System.out.println("TM APP MASTER: Alive!");

		Priority priority = Records.newRecord(Priority.class);
		priority.setPriority(0);

		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(128);
		capability.setVirtualCores(1);

		for (int i = 0; i < 3; ++i) {
			ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
			System.out.println("TM APP MASTER: Requesting Container " + i);
			rmClient.addContainerRequest(containerAsk);
		}
		while (!containersFinished()) {
			Thread.sleep(100);
		}

		rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
	}

	@Override
	public void onContainersAllocated(List<Container> containers) {
		for (Container container : containers) {
			try {
				System.out.println("TM APP MASTER: Launching container " + container.getId());

				ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
				ctx.setCommands(Collections.singletonList("echo GG"));
				nmClient.startContainer(container, ctx);

			} catch (Exception ex) {
				System.out.println("TM APP MASTER: Error launching container " + container.getId());
			}
		}

	}

	@Override
	public void onContainersCompleted(List<ContainerStatus> statuses) {
		for (ContainerStatus status : statuses) {
			System.out.println("TM APP MASTER: Completed container " + status.getContainerId());

			synchronized (this) {
				containerCount--;
			}
		}
	}

	@Override
	public float getProgress() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void onError(Throwable ex) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onNodesUpdated(List<NodeReport> report) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onShutdownRequest() {
		// TODO Auto-generated method stub

	}

}
