import java.util.List;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * The TransactionMaster is the ApplicationMaster of this application.
 * It is started by the ResourceManager on client's request.
 * Then, it receives command directly from the client.
 */
public class TransactionMaster implements AMRMClientAsync.CallbackHandler {
	YarnConfiguration configuration;
    String command;

    public TransactionMaster(String command) {
        this.command = command;
        this.configuration = new YarnConfiguration();
    }
    
    public YarnConfiguration getConfiguration() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {
        final String command = args[0];

        TransactionMaster master = new TransactionMaster(command);
        master.runMainLoop();
    }

    public void runMainLoop() throws Exception {
    }

	@Override
	public void onContainersAllocated(List<Container> containers) {
		
	}

	@Override
	public void onContainersCompleted(List<ContainerStatus> statusOfContainers) {
		
	}

	@Override
	public void onNodesUpdated(List<NodeReport> nodeReports) {
		
	}

	@Override
	public float getProgress() {
		return 0;
	}

	@Override
	public void onError(Throwable err) {
		
	}

	@Override
	public void onShutdownRequest() {
		
	}
}
