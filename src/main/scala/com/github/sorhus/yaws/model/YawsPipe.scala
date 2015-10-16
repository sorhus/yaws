package com.github.sorhus.yaws.model

import java.util

import com.github.sorhus.yaws.model.task.{YarnTask, Task}
import com.twitter.scalding.Args
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse
import org.apache.hadoop.yarn.api.records.{ContainerLaunchContext, Container, Priority, Resource}
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.{NMClient, AMRMClient}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records
import org.slf4j.{LoggerFactory, Logger}
import collection.JavaConverters._

abstract class YawsPipe(val name: String, args: Args) {

  val log: Logger = LoggerFactory.getLogger(getClass.getCanonicalName)

  val conf: YarnConfiguration = new YarnConfiguration()
  val rmClient: AMRMClient[ContainerRequest] = AMRMClient.createAMRMClient[ContainerRequest]
  rmClient.init(conf)
  rmClient.start()
  val nmClient: NMClient = NMClient.createNMClient
  nmClient.init(conf)
  nmClient.start()

  def getTopLevelTasks: List[Task]

    val tasks: List[Task] = {
      var size = 0
      var all: List[Task] = getTopLevelTasks
      while (size < all.size) {
        size = all.size
        all = (all ::: all.flatMap(_.dependencies)).distinct
      }
      all
    }

    private def getCandidates: List[Task] = {
      val candidates = tasks.filter(t => !t.isDone && t.isCandidate)
//      log.info("Candidate tasks are {}", candidates)
      println(s"Candidate tasks are $candidates")
      candidates
    }

  def runYarn(task: YarnTask): (YarnTask, AllocateResponse) = {
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
      context.setCommands((task.command :: Nil).asJava)
      nmClient.startContainer(container, context)
      task.running = true
    }
    (task, response)
  }

  def run() {
    while (!tasks.forall(_.isDone)) {
      val running: List[Either[YarnTask, Task]] = getCandidates.map{
        case yarnTask: YarnTask => Left(yarnTask)
        case task: Task => Right(task)
      }
      val status: List[Either[(YarnTask, AllocateResponse), Boolean]] = running.map{
        case Left(yarnTask) => Left(runYarn(yarnTask))
        case Right(task) => Right(task.run)
      }

      status.map{
        case Left((task, response)) => if(response.getCompletedContainersStatuses.asScala.nonEmpty) {
          task.running = false
          task.done = true
          true
        } else {
          false
        }
        case Right(success) => success
      }

      if (getCandidates.map(_.run).reduce(_ || _)) {
        println("At least one task successful")
      } else {
        println("No successful candidate attempts")
        println("Sleeping")
        Thread.sleep(5000)
      }
    }
  }

  override def toString = name

}
