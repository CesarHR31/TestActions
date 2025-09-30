package sat.diot.comunes

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.command.PullImageResultCallback
import com.github.dockerjava.api.exception.NotFoundException
import com.github.dockerjava.api.model.{ExposedPort, HostConfig, Ports, PullResponseItem}
import com.github.dockerjava.core.{DefaultDockerClientConfig, DockerClientImpl}
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient
import org.apache.commons.lang3.SystemUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Waiters.{interval, timeout}
import org.scalatest.funsuite.AnyFunSuite

import java.net.ServerSocket
import java.sql.{Connection, DriverManager}
import java.util.Properties
import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt
import scala.util.control.NonFatal

trait DatabaseOnDocker {
  val imageName: String
  val env: Map[String, String]
  val jdbcPort: Int
  val containerName: String
  def getJdcbUrl(ip: String, port: Int): String
  def getJdbcProperties: Properties = new Properties()
  val useSameJdbcPort: Boolean      = false
}
trait DockerJDBCSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var docker: DockerClient = _
  private var containerId: String = _
  private var jdbcUrl: String = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    createContainer()
  }

  override protected def afterAll(): Unit = {
    cleanup()
    super.afterAll()
  }

  val db: DatabaseOnDocker
  val dockerHost: String = if (SystemUtils.IS_OS_WINDOWS) {
    "tcp://localhost:2375"
  } else if (SystemUtils.IS_OS_LINUX) {
    "unix:///var/run/docker.sock"
  } else {
    throw new Exception("Not running on Windows or Linux")
  }

  lazy val externalPort: Int = {
    if (db.useSameJdbcPort) {
      db.jdbcPort
    } else {
      val sock = new ServerSocket(0)
      val port = sock.getLocalPort
      sock.close()
      port
    }
  }

  def cleanup(): Unit = {
    try {
      docker.stopContainerCmd(containerId).exec()
      docker.removeContainerCmd(containerId).withRemoveVolumes(true).exec()
    } catch {
      case NonFatal(e) => println(e)
    }
  }

  def getConnection: Connection = {
    DriverManager.getConnection(jdbcUrl, db.getJdbcProperties)
  }

  def dataPreparation(connection: Connection): Unit

  def createContainer(): Unit = {
    val config = DefaultDockerClientConfig
      .createDefaultConfigBuilder()
      .withDockerHost(dockerHost)
      .withDockerTlsVerify(false)
      .build()

    val httpClient = new ApacheDockerHttpClient.Builder()
      .dockerHost(config.getDockerHost)
      .sslConfig(config.getSSLConfig)
      .build()

    docker = DockerClientImpl.getInstance(config, httpClient)

    try {
      docker.inspectImageCmd(db.imageName).exec()
    } catch {
      case _: NotFoundException =>
        System.err.println(s"${db.imageName} not found. Pulling...")
        docker
          .pullImageCmd(db.imageName)
          .exec(new PullImageResultCallback() {
            override def onNext(item: PullResponseItem): Unit = {
              super.onNext(item)
            }
          })
        eventually(timeout(5.minutes), interval(10.seconds)) {
          docker.inspectImageCmd(db.imageName).exec()
          println(s"${Util.obtenerTimestamp()} Inspecting image ${db.imageName}")
        }
    }

    val exposedPort = ExposedPort.tcp(externalPort)
    val ports = new Ports()
    ports.bind(ExposedPort.tcp(db.jdbcPort), Ports.Binding.bindPort(externalPort))

    val cmd = docker
      .createContainerCmd(db.imageName)
      .withExposedPorts(exposedPort)
      .withHostConfig(new HostConfig().withPortBindings(ports))
      .withEnv(db.env.map { case (k, v) => s"$k=$v" }.toSeq.asJava)
      .withName(db.containerName)
      .exec()

    docker.startContainerCmd(cmd.getId).exec()
    containerId = cmd.getId
    jdbcUrl = db.getJdcbUrl("localhost", exposedPort.getPort)

    var conn: Connection = null

    eventually(timeout(300.second), interval(1.second)) {
      conn = getConnection
    }

    dataPreparation(conn)

    conn.close()
  }

  def getJdbcUrl: String = jdbcUrl
}