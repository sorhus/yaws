package com.github.sorhus.yaws.plumbing

import java.io.{File, IOException}
import java.util
import java.util.Collections
import com.google.common.collect.ImmutableList
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileStatus, Path}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{YarnClientApplication, YarnClient}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{Apps, ConverterUtils, Records}

object PipeClient extends App {

  println(s"Client user: ${System.getProperty("user.name")}")

  val conf: Configuration = new YarnConfiguration
  val yarnClient: YarnClient = YarnClient.createYarnClient
  yarnClient.init(conf)
  yarnClient.start

  System.out.println("Creating application")
  val app: YarnClientApplication = yarnClient.createApplication
  // Set up the container launch context for the application master
  System.out.println("Creating container")
  val amContainer: ContainerLaunchContext = Records.newRecord(classOf[ContainerLaunchContext])
  val command: String = "$JAVA_HOME/bin/java com.github.sorhus.yaws.plumbing.PipeRunner com.github.sorhus.yaws.MyPipe " + "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout " + "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
  amContainer.setCommands(ImmutableList.of(command))

  System.out.println("Creating resource")
  val appMasterJar: LocalResource = Records.newRecord(classOf[LocalResource])
//  setupAppMasterJar(new Path("hdfs:///user/anton/yaws.jar"), appMasterJar, conf)
  setupAppMasterJar(new Path("hdfs:///user/anton/yaws.jar"), appMasterJar, conf)
  amContainer.setLocalResources(Collections.singletonMap("yaws.jar", appMasterJar))

  System.out.println("Creating classpath")
  // Setup CLASSPATH for ApplicationMaster
  val appMasterEnv: util.Map[String, String] = new util.HashMap[String, String]()
  setupAppMasterEnv(appMasterEnv, conf)
  amContainer.setEnvironment(appMasterEnv)
  // Set up resource type requirements for ApplicationMaster
  val capability: Resource = Records.newRecord(classOf[Resource])
  capability.setMemory(256)
  capability.setVirtualCores(1)

  // Finally, set-up ApplicationSubmissionContext for the application
  System.out.println("Creating app context")
  val appContext: ApplicationSubmissionContext = app.getApplicationSubmissionContext
  appContext.setApplicationName("YawsMaster")
  appContext.setAMContainerSpec(amContainer)
  appContext.setResource(capability)
  appContext.setQueue("default")

  // Submit application
  System.out.println("Submitting app")
  val appId: ApplicationId = appContext.getApplicationId
  System.out.println("Submitting application " + appId)
  yarnClient.submitApplication(appContext)
  val appReport: ApplicationReport = yarnClient.getApplicationReport(appId)
  val appState: YarnApplicationState = appReport.getYarnApplicationState


  @throws(classOf[IOException])
  private def setupAppMasterJar(jarPath: Path, appMasterJar: LocalResource, conf: Configuration) {
    val jarStat: FileStatus = FileSystem.get(conf).getFileStatus(jarPath)
    appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath))
    appMasterJar.setSize(jarStat.getLen)
    appMasterJar.setTimestamp(jarStat.getModificationTime)
    appMasterJar.setType(LocalResourceType.FILE)
    appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC)
  }

  private def setupAppMasterEnv(appMasterEnv: util.Map[String, String], conf: Configuration) {
    conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH: _*).foreach{s =>
      Apps.addToEnvironment(appMasterEnv, ApplicationConstants.Environment.CLASSPATH.name, s.trim, File.pathSeparator)
    }
    Apps.addToEnvironment(appMasterEnv, ApplicationConstants.Environment.CLASSPATH.name, ApplicationConstants.Environment.PWD.$ + File.separator + "*", File.pathSeparator)
  }
}
