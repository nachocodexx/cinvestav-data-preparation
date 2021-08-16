package mx.cinvestav

import cats.effect.{IO, Ref}
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.utils.v2.{PublisherV2, RabbitMQContext}
import org.typelevel.log4cats.Logger
import mx.cinvestav.commons.balancer.LoadBalancer
import mx.cinvestav.commons.fileX.{Extension, Filename}

object Declarations {
  trait NodeError extends Error
  case class MergeError(message:String) extends NodeError{
    override def getMessage: String = s"MERGE_ERROR: $message"
  }
  case class TaskNotFound(taskId:String) extends NodeError {
    override def getMessage: String = s"Task[$taskId] not found"
  }
  case class NoMessageId() extends NodeError {
    override def getMessage: String = "No <MESSAGE_ID> property provided"
  }
  case class NoReplyTo() extends NodeError {
    override def getMessage: String = "No <REPLY_TO> property provided"
  }
  case class PathIsNotDirectory() extends NodeError{
    override def getMessage: String = "NO DIRECTORY"
  }
  case class CompressionError(message:String) extends  NodeError {
    override def getMessage: String = message
  }
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
  trait Task {
    def id:String
    def subTasks:List[SubTask]
    def startedAt:Long
  }
  trait SubTask {
    def taskId:String
    def id:String
    def nodeId:String
  }
  case class CompressionSubTask(id:String,taskId:String,nodeId:String) extends SubTask
  case class CompressionTask(
                              id:String,
                              subTasks: List[CompressionSubTask],
                              startedAt:Long
                            ) extends Task
// ______________________________________________________________________________-
  case class DecompressionSubTask(id:String,taskId:String,nodeId:String) extends SubTask
  case class DecompressionTask(
                                id:String,
                                subTasks: List[DecompressionSubTask],
                                startedAt:Long,
                                filename:Filename,
                                extension:Extension,
                                sourcePath:String
                              ) extends Task

// _____________________
  case class NodeState(
                        sourceFolders:List[String],
                        publishers:Map[String,PublisherV2],
                        loadBalancer:LoadBalancer,
                        pendingTasks:Map[String,Task]
                      )
  case class NodeContext(
                          state:Ref[IO,NodeState],
                          rabbitContext: RabbitMQContext,
                          logger:Logger[IO],
                          config:DefaultConfig
                        )
//  __________________________-
  object payloads {
  case class Decompress(sourcePath:String, compressionAlgorithm:String)
  case class MergeCompleted(url:String)
  case class CompressCompleted(subTaskId:String)
  case class DecompressCompleted(
                                subTaskId:String,
                              )
  case class Compress(
                       sourcePath:String,
                       compressionAlgorithm:String
                     )
  case class Slice(sourcePath:String)
  case class Merge(sourcePath:String,filename:String,extension:String)
  case class MergeAndDecompress(
                               sourcePath:String,
                               compressionAlgorithm:String,
                               filename:String,
                               extension:String
                             )
  case class SliceAndCompress(
                               sourcePath:String,
                               chunkSize:Long,
                               compressionAlgorithm:String
                             )
}

}
