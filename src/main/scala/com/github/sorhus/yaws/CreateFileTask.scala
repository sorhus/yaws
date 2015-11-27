package com.github.sorhus.yaws

import java.io.File
import java.net.URI
import java.security.PrivilegedAction

import com.github.sorhus.yaws.model.target.{URITarget, Target}
import com.github.sorhus.yaws.model.task.Task
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.yarn.api.ApplicationConstants

class CreateFileTask(path: String, val dependencies: List[Task] = Nil) extends Task {

  val name = path.split("/").last

  def run(): Boolean = {
    println(s"$path running as ${System.getProperty("user.name")}")
    println(s"ugi: ${UserGroupInformation.getCurrentUser}")
    println(s"ugi: ${UserGroupInformation.createRemoteUser("anton")}")
    println(s"env user: ${System.getenv(ApplicationConstants.Environment.USER.name())}")
    val appSubmitterUserName = System.getenv(ApplicationConstants.Environment.USER.name())
    val appSubmitterUgi = UserGroupInformation.createRemoteUser(appSubmitterUserName)
    val credentials: Credentials = UserGroupInformation.getCurrentUser().getCredentials()
    val c = Credentials
    appSubmitterUgi.addCredentials(credentials)
    val user = appSubmitterUgi
    println(s"user: $user")
    user.doAs(
      new PrivilegedAction[Boolean] {
        override def run(): Boolean = {
          new File(new URI(path)).createNewFile()
        }
      }
    )
  }

  val targets: List[Target] = URITarget(path) :: Nil
}

