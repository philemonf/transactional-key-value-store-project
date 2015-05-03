package ch.epfl.tkvs.yarn.appmaster;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;

import ch.epfl.tkvs.yarn.RoutingTable;
import ch.epfl.tkvs.yarn.Utils;


public class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

    private static Logger log = Logger.getLogger(RMCallbackHandler.class.getName());
    private final NMClientAsync nmClient;
    private final YarnConfiguration conf;

    private RoutingTable routing;
    
    private List<Container> registeredContainers = Collections.synchronizedList(new LinkedList<Container>());

    public RMCallbackHandler(NMClientAsync nmClient, YarnConfiguration conf) {
        this.nmClient = nmClient;
        this.conf = conf;
    }

    public RoutingTable getRoutingTable() {
        return routing;
    }

    public void setRoutingTable(RoutingTable routing) {
        this.routing = routing;
    }

    public int getContainerCount() {
        return registeredContainers.size();
    }

    private ContainerLaunchContext initContainer() throws Exception {
        // Create Container Context
        ContainerLaunchContext cCLC = Records.newRecord(ContainerLaunchContext.class);
        cCLC.setCommands(Collections.singletonList("$HADOOP_HOME/bin/hadoop jar TKVS.jar ch.epfl.tkvs.transactionmanager.TransactionManager " + "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));

        // Set Container jar
        LocalResource jar = Records.newRecord(LocalResource.class);
        Utils.setUpLocalResource(Utils.TKVS_JAR_PATH, jar, conf);
        cCLC.setLocalResources(Collections.singletonMap(Utils.TKVS_JAR_NAME, jar));

        // Set Container CLASSPATH
        Map<String, String> env = new HashMap<>();
        Utils.setUpEnv(env, conf);
        env.put("AM_IP", routing.getAMIp());
        env.put("AM_PORT", String.valueOf(routing.getAMPort()));
        cCLC.setEnvironment(env);

        return cCLC;
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        for (Container container : containers) {
            if (!routing.contains(Utils.extractIP(container.getNodeHttpAddress()))) {
                try {
                	registeredContainers.add(container);
                    nmClient.startContainerAsync(container, initContainer());
                    log.info("Container launched " + container.getId());
                } catch (Exception ex) {
                    log.error("Container not launched " + container.getId(), ex);
                }
            }
        }
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> contStatus) {
        for (ContainerStatus status : contStatus) {
            log.info("Container finished " + status.getContainerId());
            Container containerToRemove = null;
            for (Container container : registeredContainers) {
            	if (container != null && status != null && container.getId().equals(status.getContainerId())) {
            		containerToRemove = container;
            	}
            }
            
            if (containerToRemove != null) {
            	registeredContainers.remove(containerToRemove);
            }
            
        }
    }
    
    public void closeAllRegisteredContainers() {
    	for (Container container : registeredContainers) {
    		nmClient.stopContainerAsync(container.getId(), container.getNodeId());
    	}
    }

    @Override
    public void onError(Throwable e) {
        log.error("onError", e);
    }

    @Override
    public void onNodesUpdated(List<NodeReport> nr) {
    }

    @Override
    public void onShutdownRequest() {
        log.warn("onShutdownRequest");
    }

    @Override
    public float getProgress() {
        return 0;
    }
}
