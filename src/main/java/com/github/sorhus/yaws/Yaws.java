package com.github.sorhus.yaws;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class Yaws {

    public static void main(String[] args) throws IOException, YarnException, InterruptedException {
        Configuration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        System.out.println("Creating application");
        YarnClientApplication app = yarnClient.createApplication();

        // Set up the container launch context for the application master
        System.out.println("Creating container");
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
        String command = "$JAVA_HOME/bin/java com.github.sorhus.yaws.YawsMaster "
                + "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout "
                + "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
        amContainer.setCommands(ImmutableList.of(command));

        System.out.println("Creating resource");
        LocalResource appMasterJar = Records.newRecord(LocalResource.class);
        setupAppMasterJar(new Path("hdfs:///user/anton/yaws.jar"), appMasterJar, conf);
        amContainer.setLocalResources(
                Collections.singletonMap("yaws.jar", appMasterJar));

        System.out.println("Creating classpath");
        // Setup CLASSPATH for ApplicationMaster
        Map<String, String> appMasterEnv = new HashMap<String, String>();
        setupAppMasterEnv(appMasterEnv, conf);
        amContainer.setEnvironment(appMasterEnv);

        // Set up resource type requirements for ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(256);
        capability.setVirtualCores(1);

        // Finally, set-up ApplicationSubmissionContext for the application
        System.out.println("Creating app context");
        ApplicationSubmissionContext appContext =
                app.getApplicationSubmissionContext();
        appContext.setApplicationName("YawsMaster"); // application name
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        appContext.setQueue("default"); // queue

        // Submit application
        System.out.println("Submitting app");
        ApplicationId appId = appContext.getApplicationId();
        System.out.println("Submitting application " + appId);
        yarnClient.submitApplication(appContext);

        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();
        while (appState != YarnApplicationState.FINISHED &&
                appState != YarnApplicationState.KILLED &&
                appState != YarnApplicationState.FAILED) {
            Thread.sleep(1000);
            appState = yarnClient.getApplicationReport(appId).getYarnApplicationState();
            System.out.println("Application state is " + appState);
        }
        System.out.println("Application " + appId + " finished with state " + appState +" at " + appReport.getFinishTime());
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
            Apps.addToEnvironment(appMasterEnv, ApplicationConstants.Environment.CLASSPATH.name(),c.trim(),File.pathSeparator);
        }
        Apps.addToEnvironment(appMasterEnv,ApplicationConstants.Environment.CLASSPATH.name(),ApplicationConstants.Environment.PWD.$() + File.separator + "*",File.pathSeparator);
    }
}
