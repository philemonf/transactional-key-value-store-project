package ch.epfl.tkvs.yarn.appmaster;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.log4j.Logger;


/**
 * The YARN Node Manager Asynchronous Client, used to log container information.
 * @see ch.epfl.tkvs.yarn.appmaster.AppMaster
 * @see ch.epfl.tkvs.yarn.appmaster.RMCallbackHandler
 */
public class NMCallbackHandler implements NMClientAsync.CallbackHandler {

    private final static Logger log = Logger.getLogger(NMCallbackHandler.class.getName());

    public NMCallbackHandler() {
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
        log.info("Succeeded to stop Container " + containerId);
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
        log.info("Container Status: id=" + containerId + ", status=" + containerStatus);
    }

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
        log.info("Succeeded to start Container " + containerId);
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
        log.error("Failed to start Container " + containerId);
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
        log.error("Failed to query the status of Container " + containerId);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
        log.error("Failed to stop Container " + containerId);
    }
}