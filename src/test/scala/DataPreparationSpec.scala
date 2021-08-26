import mx.cinvestav.commons.balancer.LoadBalancer
import mx.cinvestav.server.Client
import cats.implicits._
import cats.effect._
import java.nio.file.Paths
import java.util.UUID
import com.github.gekomad.scalacompress.Compressors.{lz4Decompress,lz4Compress}
import org.apache.commons.io.FileUtils

class DataPreparationSpec extends munit .CatsEffectSuite {
  final val TARGET = "/home/nacho/Programming/Scala/data-preparation/target"
  final val TEST = s"$TARGET/test"
  test("Delete folder"){
    val folderPath = Paths.get(TARGET)
      .resolve("sink")
      .resolve("data")
      .resolve("dp-0")
    val folderFile = folderPath.toFile
//    println(folderFile.listFiles().mkString(","))
    IO.delay{ FileUtils.deleteDirectory(folderFile)}
//     IO.delay(folderPath.toFile.delete()).flatMap{
//      IO.println
//    }
  }
  test("Uncompress"){
    val source = s"$TEST/07709311-67bf-4717-822d-a70767902010_0.lz4"
    val dest = s"$TEST/x"
//    val result = lz4Compress(source,dest)
    val result = lz4Decompress(source,dest)

//    val source = s"$TARGET/sink/data/dp-1/e033b4db-3a0b-45ce-9811-dcaa89964aa5/compressed/e033b4db-3a0b-45ce-9811-dcaa89964aa5_0.lz4"
//    val dest   = s"$TARGET/sink/data/dp-1/e033b4db-3a0b-45ce-9811-dcaa89964aa5/chunks/"
//    val result = lz4Decompress(source,dest)
    result.toEither match {
      case Left(e) =>  IO.println(e)
      case Right(value) =>IO.println(value)
    }
  }
  test("Basics"){
    val chunkName = UUID.randomUUID().toString
    val data      = chunkName.split('.').toList.lastOption
    println(data )
  }
  test("Download file"){
    Client.downloadFile(
      "http://localhost:7001/download",
      "/home/nacho/Programming/Scala/data-preparation/target/sources/01.pdf",
      Paths.get("/home/nacho/Programming/Scala/data-preparation/target/sink/data/dp-0/FILE_ID/compressed/01.pdf")
    )
  }
  test("Balancer"){
    val lb = LoadBalancer("RB")
    val nodeIds = List("dp-0","dp-1","dp-2")
    val numOfChunks = 23
    val balanced = lb.balanceMulti(nodeIds,numOfChunks,allowRep=true)
    println(balanced)
  }

}
