package mx.cinvestav

import cats.effect.{IO, Ref}
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.utils.v2.{PublisherV2, RabbitMQContext}
import org.typelevel.log4cats.Logger
import mx.cinvestav.commons.balancer.LoadBalancer

object Declarations {
  trait NodeError extends Error
  case class PublisherNotFound() extends NodeError{
    override def getMessage: String = s"Publisher not found"
  }
  case class FileNotFound(id:String) extends NodeError{
    override def getMessage: String = s"$id not found"
  }
  case class VolumeNotFound(volumeName:String) extends NodeError {
    override def getMessage: String = s"Volume[$volumeName] not found"
  }
// _____________________
  case class NodeState(
                        sourceFolders:List[String],
                        publishers:Map[String,PublisherV2],
                        loadBalancer:LoadBalancer
                      )
  case class NodeContext(
                          state:Ref[IO,NodeState],
                          rabbitContext: RabbitMQContext,
                          logger:Logger[IO],
                          config:DefaultConfig
                        )
//  __________________________-
  object payloads {
  case class Compress(
                       sourcePath:String,
                       compressionAlgorithm:String
                     )
  case class Slice(sourcePath:String)
  case class SliceAndCompress(
                               sourcePath:String,
                               chunkSize:Long,
                               compressionAlgorithm:String
                             )
}

}
