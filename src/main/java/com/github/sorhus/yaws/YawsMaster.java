package com.github.sorhus.yaws;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static spark.Spark.*;

public class YawsMaster {

    public static void main(String[] args) throws InterruptedException, IOException, YarnException {

        // Initialize clients to ResourceManager and NodeManagers
        Configuration conf = new YarnConfiguration();

        AMRMClient<AMRMClient.ContainerRequest> rmClient = AMRMClient.createAMRMClient();
        rmClient.init(conf);
        rmClient.start();

        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();

        // Register with ResourceManager
        System.out.println("registerApplicationMaster 0");
        rmClient.registerApplicationMaster("", 0, "");

        System.out.println("Setting up get foobar");
        get("/pipe/:class/:name/start", (req, res) -> {

            System.out.println(req.params());
            String jarPath = "hdfs:///user/anton/yaws.jar"; //req.params(":jar");
            String pipeClass = req.params(":class");
            String pipeName = req.params(":name");

            // Priority for worker containers - priorities are intra-application
            Priority priority = Records.newRecord(Priority.class);
            priority.setPriority(0);

            // Resource requirements for worker containers
            Resource capability = Records.newRecord(Resource.class);
            capability.setMemory(128);
            capability.setVirtualCores(1);

            AMRMClient.ContainerRequest containerAsk = new AMRMClient.ContainerRequest(capability, null, null, priority);
            System.out.println("Making res-req");
            rmClient.addContainerRequest(containerAsk);

            // Obtain allocated containers, launch and check for responses
            int responseId = 0;
            int completedContainers = 0;
            AllocateResponse response = rmClient.allocate(responseId++);
            for (Container container : response.getAllocatedContainers()) {
                // Launch container by create ContainerLaunchContext
                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

                LocalResource appMasterJar = Records.newRecord(LocalResource.class);
                setupAppMasterJar(new Path(jarPath), appMasterJar, conf);
                ctx.setLocalResources(Collections.singletonMap("jar", appMasterJar));

                // Setup CLASSPATH for ApplicationMaster
                Map<String, String> appMasterEnv = new HashMap<String, String>();
                setupAppMasterEnv(appMasterEnv, conf);
                ctx.setEnvironment(appMasterEnv);

                ctx.setCommands(
                        Collections.singletonList(
                                "$JAVA_HOME/bin/java com.github.sorhus.yaws.YawsPipeRunner " + pipeClass + " " + pipeName + " param1 param2" +
                                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                        ));
                System.out.println("Launching container " + container.getId());
                nmClient.startContainer(container, ctx);
            }
            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                if(status.getExitStatus() == 0) {
                    ++completedContainers;
                    System.out.println("Completed container " + status.getContainerId() + " with status " + status.getExitStatus());
                } else {
                    System.out.println("Container " + status.getContainerId() + " failed");
                }
            }
            return "Triggered pipe " + pipeClass + " " + pipeName;
        });


        get("/shutdown", (req, res) -> {
            System.out.println("Shutting down");
            System.exit(0);
            return null;
        });


        // Un-register with ResourceManager
        rmClient.unregisterApplicationMaster(
                FinalApplicationStatus.SUCCEEDED, "", "");
    }

    private static void setupAppMasterJar(Path jarPath, LocalResource appMasterJar, Configuration conf) throws IOException {
        FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        appMasterJar.setSize(jarStat.getLen());
        appMasterJar.setTimestamp(jarStat.getModificationTime());
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
    }

    private static void setupAppMasterEnv(Map<String, String> appMasterEnv, Configuration conf) {
        String[] strings = conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH);
        for (String c : strings) {
            Apps.addToEnvironment(appMasterEnv, ApplicationConstants.Environment.CLASSPATH.name(), c.trim(), File.pathSeparator);
        }
        Apps.addToEnvironment(appMasterEnv, ApplicationConstants.Environment.CLASSPATH.name(), ApplicationConstants.Environment.PWD.$() + File.separator + "*", File.pathSeparator);
    }

}
