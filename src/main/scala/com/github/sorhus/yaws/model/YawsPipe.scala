package com.github.sorhus.yaws.model

import java.io.{File, IOException}
import java.util
import java.util.Collections

import com.github.sorhus.yaws.model.task.{YarnTask, Task}
import com.twitter.scalding.Args
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileStatus, Path}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.{NMClient, AMRMClient}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{Apps, ConverterUtils, Records}
import org.slf4j.{LoggerFactory, Logger}
import collection.JavaConverters._
import scala.collection.mutable

abstract class YawsPipe(val name: String, args: Args) {

  val log: Logger = LoggerFactory.getLogger(getClass.getCanonicalName)

  val conf: YarnConfiguration = new YarnConfiguration()
  val rmClient: AMRMClient[ContainerRequest] = AMRMClient.createAMRMClient[ContainerRequest]
  rmClient.init(conf)
  rmClient.start()
  rmClient.registerApplicationMaster("", 0, "")
  val nmClient: NMClient = NMClient.createNMClient
  nmClient.init(conf)
  nmClient.start()

  def topLevelTasks: List[Task]

  val tasks: List[Task] = {
    var size = 0
    var all: List[Task] = topLevelTasks
    while (size < all.size) {
      size = all.size
      all = (all ::: all.flatMap(_.dependencies)).distinct
    }
    all
  }

    private def getCandidates: List[Task] = {
      println(s"All tasks are $tasks")
      val candidates = tasks.filter(t => !t.isDone && t.isCandidate)
//      log.info("Candidate tasks are {}", candidates)
      println(s"Candidate tasks are $candidates")
      candidates
    }

  def runYarn(task: YarnTask): (YarnTask, AllocateResponse) = {
    println(s"Setting up container for $task")

    val jarPath = "hdfs:///user/anton/yaws.jar"

    // Priority for worker containers - priorities are intra-application
    val priority = Records.newRecord(classOf[Priority])
    priority.setPriority(0)

    // Resource requirements for worker containers
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(128)
    capability.setVirtualCores(1)

    val containerAsk = new ContainerRequest(capability, null, null, priority)
    rmClient.addContainerRequest(containerAsk)
    val response = rmClient.allocate(0)
    val containers: List[Container] = response.getAllocatedContainers.asScala.toList
    containers.foreach{container =>
      val context = Records.newRecord(classOf[ContainerLaunchContext])
      val appMasterJar: LocalResource = Records.newRecord(classOf[LocalResource])
      setupAppMasterJar(new Path(jarPath), appMasterJar, conf)
      context.setLocalResources(Collections.singletonMap("jar", appMasterJar))

      // Setup CLASSPATH for ApplicationMaster
      val appMasterEnv: util.Map[String, String] = new util.HashMap[String, String]
      setupAppMasterEnv(appMasterEnv, conf)
      context.setEnvironment(appMasterEnv)

      context.setCommands(Collections.singletonList(
        s"${task.command} 1>${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stdout 2>${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stderr"
      ))
      nmClient.startContainer(container, context)
      println(s"$task started: $response")
      task.running = true
    }
    (task, response)
  }

  def run() {
    println(s"YawsPipe user: ${System.getProperty("user.name")}")

    val responses = mutable.Map[YarnTask, AllocateResponse]()

    while (!topLevelTasks.forall(_.isDone)) {

      println("---------- NEW ROUND ----------")

      val running: List[Either[YarnTask, Task]] = getCandidates.map{
          case yarnTask: YarnTask => Left(yarnTask)
          case task: Task => Right(task)
        } ++ responses.keys
          .map(Left.apply)
          .map(_.asInstanceOf[Either[YarnTask, Task]])

      println("---------- FOUND RUNNING TASKS ----------")
      println(running)

      val status: List[Boolean] = running.map{
        case Left(yarnTask) =>
          println(s"$yarnTask is running: ${yarnTask.running}")
          if(!yarnTask.running) {
            val (task, response) = runYarn(yarnTask)
            responses.put(task, response)
          }
          val s = responses(yarnTask).getCompletedContainersStatuses.asScala
          println(s"Container statuses are")
          s.foreach(println)
          if(s.nonEmpty) {
            yarnTask.running = false
            yarnTask.done = true
            true
          } else {
            false
          }
        case Right(task) =>
          task.run
      }

      println("---------- RETRIEVED STATUS ----------")
      println(status)

      if((false :: status).reduce(_ || _)) {
        println("At least one task successful")
      } else {
        println("No successful candidate attempts")
        println("Sleeping")
        Thread.sleep(5000)
      }
    }
  }

  override def toString = s"$name $topLevelTasks"

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

