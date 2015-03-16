import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

/**
 * The TransactionMaster is the ApplicationMaster of this application.
 * It is started by the ResourceManager on client's request.
 * Then, it receives command directly from the client.
 */
public class ApplicationMaster implements AMRMClientAsync.CallbackHandler {
	Configuration configuration;
	AMRMClient<ContainerRequest> rmClient;
	String[] hosts;
    
    public Configuration getConfiguration() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {
        ApplicationMaster master = new ApplicationMaster();
        System.out.println(args);
        master.runMainLoop();
    }

    public void runMainLoop() throws Exception {
    	configuration = new Configuration();
    	
    	// Initialize 
    	rmClient = AMRMClient.createAMRMClient();
    	rmClient.init(configuration);
    	rmClient.start();
    	
    	System.out.println("hey");
    	
    	//Priority priority = Priority.newInstance(0); // intra-application priority
    	//Resource capability = Resource.newInstance(128, 1);
    	
    }

	@Override
	public void onContainersAllocated(List<Container> containers) {
		
	}

	@Override
	public void onContainersCompleted(List<ContainerStatus> statusOfContainers) {
		
	}

	@Override
	public void onNodesUpdated(List<NodeReport> nodeReports) {
		System.out.println(nodeReports);
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
