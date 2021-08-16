package mx.cinvestav

import cats.implicits._
import cats.data.EitherT
import cats.effect._
import ch.qos.logback.core.util.FileSize
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.StringVal
import dev.profunktor.fs2rabbit.model.{AmqpEnvelope, AmqpMessage, AmqpProperties}
import io.circe.generic.auto._
import mx.cinvestav.Declarations.{CompressionError, CompressionSubTask, CompressionTask, DecompressionSubTask, DecompressionTask, FileNotFound, MergeError, NoMessageId, NoReplyTo, NodeContext, NodeError, NodeState, PathIsNotDirectory, PublisherNotFound, Task, TaskNotFound, VolumeNotFound, payloads}
import mx.cinvestav.commons.fileX.{ChunkConfig, Extension, FileMetadata, Filename}
import mx.cinvestav.utils.v2.{Acker, PublisherV2, processMessageV2}
import mx.cinvestav.commons.{fileX, liftFF}
import org.typelevel.log4cats.Logger
import fs2.Stream
import io.circe._
import io.circe.syntax._
import io.circe.generic.auto._
import mx.cinvestav.Declarations.payloads.CompressCompleted
import mx.cinvestav.commons.compression

import concurrent.duration._
import language.postfixOps
import java.io.File
import java.util.UUID
import mx.cinvestav.commons.stopwatch.StopWatch._
import mx.cinvestav.utils.v2.encoders._

import java.nio.file.Paths

object CommandHandlers {

  def merge()(implicit ctx:NodeContext, envelope:AmqpEnvelope[String], acker:Acker): IO[Unit] = {
    def successCallback(payload:payloads.Merge) = {
      type E                       = NodeError
      val unit                     = liftFF[Unit,NodeError](IO.unit)
      val maybeCurrentState        = EitherT.liftF[IO,E,NodeState](ctx.state.get)
      implicit val logger          = ctx.logger
      val L                        = Logger.eitherTLogger[IO,E]
      implicit val rabbitMQContext = ctx.rabbitContext
      val maybeReplyTo      = EitherT.fromEither[IO](envelope.properties.replyTo.toRight{NoReplyTo()})
      val app = for {
        timestamp     <- liftFF[Long,NodeError](IO.realTime.map(_.toSeconds))
        currentState  <- maybeCurrentState
        _             <- L.debug(s"MERGE_INIT ${payload.sourcePath}")
//       ______________________________________________________
        replyTo        <- maybeReplyTo
        publisher      <- EitherT.fromOption[IO].apply[NodeError,PublisherV2](currentState.publishers.get(replyTo),PublisherNotFound())
        nodeId         = ctx.config.nodeId
        poolId         = ctx.config.poolId
//      SELECT A VOLUME
        sinkVolume     <- EitherT.fromEither[IO](ctx.config.sinkVolumes.headOption.toRight{VolumeNotFound("")})
        sinkFile       = new File(sinkVolume)
        nodeVolume     = sinkFile.toPath.resolve(nodeId)
//      __________________________________________________________
        sourceFile   = new File(payload.sourcePath)
        filename     = Filename(payload.filename)
        extension    = Extension(payload.extension)
        fileId      <- fileX.mergeFile(filename,extension,sourceFile,chunkFolderName = "chunks").leftMap[NodeError](x=>MergeError(x.getMessage))
      } yield (fileId,publisher)


      app.value.stopwatch.flatMap { result =>
        result.result match {
          case Left(e) =>  ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
          case Right(value) => value match {
            case (fileId,publisher) => for{
                _          <- acker.ack(envelope.deliveryTag)
                _          <- ctx.logger.info(s"MERGE ${result.duration}")
                properties = AmqpProperties(
                  headers = Map("commandId"->StringVal("MERGE_COMPLETED"))
                )
                msgPayload = payloads.MergeCompleted(url = s"http://localhost/download").asJson.noSpaces
                message    = AmqpMessage[String](payload=msgPayload,properties = properties)
              } yield ( )
          }
        }
      }
    }

    processMessageV2[IO,payloads.Merge,NodeContext](
      successCallback =  successCallback,
      errorCallback =  e=>ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag),

    )

  }

  def mergeAndDecompress()(implicit ctx:NodeContext,envelope: AmqpEnvelope[String],acker:Acker):IO[Unit] ={
    def successCallback(payload:payloads.MergeAndDecompress):IO[Unit] = {
      type E                       = NodeError
      val unit                     = liftFF[Unit,NodeError](IO.unit)
      val maybeCurrentState        = EitherT.liftF[IO,E,NodeState](ctx.state.get)
      implicit val logger          = ctx.logger
      val L                        = Logger.eitherTLogger[IO,E]
      implicit val rabbitMQContext = ctx.rabbitContext
      val app = for {
        timestamp           <- liftFF[Long,NodeError](IO.realTime.map(_.toSeconds))
        currentState        <- maybeCurrentState
        nodeId              = ctx.config.nodeId
        decompressTaskId    =  UUID.randomUUID().toString
        sourcePath          = payload.sourcePath
        sourceFile          = new File(sourcePath)
        isDirectory         = Either.cond[NodeError,Unit](sourceFile.isDirectory,(),PathIsNotDirectory())
        _                   <- EitherT.fromEither[IO](isDirectory)
        dataPreparationNodes = ctx.config.dataPreparationNodes
        nodeIds             = dataPreparationNodes.map(_.nodeId).filter(_!=nodeId)
        chunksFile          = sourceFile.listFiles().toList
        chunksMetadata      = chunksFile.map(_.toPath).map(FileMetadata.fromPath)
        totalNumberOfChunks = chunksFile.length
        balancedNodes       = currentState.loadBalancer.balanceMulti(nodes = nodeIds,rounds = totalNumberOfChunks,allowRep = true)
        pubs                <- EitherT.fromEither[IO](balancedNodes.traverse(currentState.publishers.get).toRight{PublisherNotFound()})
        subTasks            = (chunksMetadata zip balancedNodes)
          .map{
            case (chm,nId) =>
              DecompressionSubTask(chm.filename.value,taskId = decompressTaskId,nodeId = nId)
          }
        pubsStream     = Stream.emits(pubs).covary[IO]
        filename       = Filename(payload.filename)
        extension      = Extension(payload.extension)
        decompressTask = DecompressionTask(id=decompressTaskId,subTasks = subTasks,startedAt = timestamp,filename = filename,extension = extension,sourcePath = sourcePath )
        newTask       = (decompressTaskId->  decompressTask)
        newState      <- liftFF[NodeState,NodeError](ctx.state.updateAndGet(s=>s.copy(pendingTasks = s.pendingTasks+newTask)))
        _             <- L.debug(newState.toString)
        _             <- liftFF[Unit,NodeError](
          rabbitMQContext.client.createChannel(conn = rabbitMQContext.connection).use{ implicit channel=>
            Stream
              .emits(chunksFile)
              .zip(pubsStream)
              .covary[IO]
              .evalMap{
                case (chunk, publisher) =>
                  val messagePayload = payloads.Decompress(sourcePath = chunk.toPath.toString,compressionAlgorithm = payload.compressionAlgorithm).asJson.noSpaces
                  val properties = AmqpProperties(
                    headers = Map("commandId"->StringVal("DECOMPRESS")) ,
                    replyTo = nodeId.some,
                    messageId = decompressTaskId.some
                  )
                  val message = AmqpMessage(payload = messagePayload,properties = properties)
                  publisher.publishWithChannel(message) *> ctx.logger.debug(s"${publisher.pubId} $chunk")
              }
              .compile.drain
          }
        )
      } yield ()
//    _______________________________________________________-
      app.value.stopwatch.flatMap{ result =>
        result.result match {
          case Left(e) =>  acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
          case Right(value) =>for {
            _ <- ctx.logger.info(s"MERGE_DECOMPRESS ${result.duration}")
            _ <- acker.ack(envelope.deliveryTag)
          } yield ()
        }
      }
    }
    processMessageV2[IO,payloads.MergeAndDecompress,NodeContext](
      successCallback =  successCallback,
      errorCallback = e=>
        acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
    )
  }


  def sliceAndCompress()(implicit ctx:NodeContext,envelope: AmqpEnvelope[String],acker: Acker): IO[Unit] = {

    def successCallback(payload:payloads.SliceAndCompress) = {
      type E                       = NodeError
      val unit                     = liftFF[Unit,NodeError](IO.unit)
      val maybeCurrentState        = EitherT.liftF[IO,E,NodeState](ctx.state.get)
      implicit val logger          = ctx.logger
      val L                        = Logger.eitherTLogger[IO,E]
      implicit val rabbitMQContext = ctx.rabbitContext
      val app = for {
        timestamp           <- liftFF[Long,NodeError](IO.realTime.map(_.toSeconds))
        currentState        <- maybeCurrentState
        nodeId              = ctx.config.nodeId
        poolId              = ctx.config.poolId
        nodeIds             = ctx.config.dataPreparationNodes.map(_.nodeId).filter(_!=nodeId)
        _                   <- L.debug(currentState.loadBalancer.counter.toString)
        sinkVolume          <- EitherT.fromEither[IO](ctx.config.sinkVolumes.headOption.toRight{VolumeNotFound("")})
        sinkFile            = new File(sinkVolume)
        nodeVolume          = sinkFile.toPath.resolve(nodeId)
        fileId              = UUID.randomUUID()
        compressionTaskId   = UUID.randomUUID().toString
        chunksSink          = nodeVolume.resolve(fileId.toString)
        sourceFile          = new File(payload.sourcePath)
        sourceFileSize      =  sourceFile.length()
        metadata            = FileMetadata.fromPath(path = sourceFile.toPath)
        filename            = metadata.filename
        extension           = metadata.extension
        chunkSize           = 64000000
        proportion          = sourceFileSize.toDouble / chunkSize.toDouble
        intNumberOfChunks   = proportion.toInt
        totalNumberOfChunks = if(sourceFileSize < chunkSize) 1 else math.ceil(proportion).toInt
        chunkSizeDecimal    = proportion-intNumberOfChunks
        chunkConfig    = ChunkConfig(
          outputPath = chunksSink,
          prefix = fileId.toString,
          size = chunkSize
        )
        //       ______________________________________________________
        fileNotFound       = FileNotFound(filename.value)
        sinkVolumeNotFound = VolumeNotFound(sinkFile.toString)
        sourceCondition    = Either.cond[NodeError,Boolean](sourceFile.exists(),right = true,fileNotFound)
//        sinkCondition      = Either.cond[NodeError,Boolean](sinkFile.exists(),right = true,sinkVolumeNotFound)
        _            <- EitherT.fromEither[IO](sourceCondition)
//        _            <- EitherT.fromEither[IO](sinkCondition)
        _            <- L.debug("EXTENSION "+extension)
        _            <- L.debug("FILE_NAME "+filename)
        _            <- L.debug("SOURCE "+sourceFile)
        _            <- L.debug("SINK "+chunksSink)
        _            <- L.debug(s"SINK_EXISTS: ${chunksSink.toFile.exists()}")
        _            <- L.debug(s"ORIGINAL_FILE_SIZE ${sourceFile.length()}")
        _            <- L.debug(s"CHUNK_SIZE $chunkSize")
        _            <- L.debug(s"NUM_CHUNKS $totalNumberOfChunks")
        _            <- L.debug(s"PROPORTION $proportion")
        _            <- L.debug(s"CHUNKS ${intNumberOfChunks}x$chunkSize ${chunkSizeDecimal*chunkSize}x1")
//      ________________________________________________________________________________________________________________
        sliceStream            = fileX.splitFile(sourceFile,chunkConfig =chunkConfig,maxConcurrent = 6 )
        balancedNodes          = currentState.loadBalancer.balanceMulti(nodes = nodeIds,rounds = totalNumberOfChunks,allowRep = true)
        pubs                   <- EitherT.fromEither[IO](balancedNodes.traverse(currentState.publishers.get).toRight{PublisherNotFound()})
        pubsStream             = Stream.emits(pubs).covary[IO]
//      ________________________________________________________________________________________________________________
        compressChunks = rabbitMQContext.client.createChannel(conn = rabbitMQContext.connection)  .use{ implicit channel =>
          sliceStream.zip(pubsStream).evalMap{
            case (chunkInfo, publisher) =>
              val props   = AmqpProperties(
                headers = Map(
                  "commandId" -> StringVal("COMPRESS")
                ),
                replyTo = nodeId.some,
                messageId = compressionTaskId.some
              )
              val compressPayload = payloads.Compress(
                sourcePath = chunkInfo.sourcePath,
                compressionAlgorithm = payload.compressionAlgorithm
              ).asJson.noSpaces
              val message = AmqpMessage(payload = compressPayload,properties = props)
              for {
                _ <- ctx.logger.debug(s"COMPRESS ${chunkInfo.index} ${publisher.pubId} ${chunkInfo.sourcePath}")
                _ <- publisher.publishWithChannel(message)
              } yield chunkInfo

          }.compile.toVector.map(_.toList)
        }
//      ____________________________________________________________
        chunkInfos        <- liftFF(compressChunks)
        subTasks          = (balancedNodes zip chunkInfos).map{
          case (nId, info) =>
            CompressionSubTask(id=s"${fileId}_${info.index}",nodeId = nId,taskId = compressionTaskId)
        }
        pendingCompressionTask = CompressionTask(id = UUID.randomUUID().toString,subTasks = subTasks,startedAt=timestamp)
        newTask = (compressionTaskId->pendingCompressionTask)
        updatedState           <- liftFF[NodeState,NodeError](ctx.state.updateAndGet(s=>s.copy(pendingTasks = s.pendingTasks + newTask  )))
        _ <-L.debug(updatedState.toString)

      } yield ()

      app.value.stopwatch.flatMap{result=>
        result.result match {
          case Left(e) => acker.reject(deliveryTag = envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
          case Right(_) => for {
            _ <- acker.ack(deliveryTag = envelope.deliveryTag)
            _ <- ctx.logger.info(s"SLICE_COMPRESS ${result.duration}")
          } yield ()
        }
      }
    }

    processMessageV2[IO,payloads.SliceAndCompress,NodeContext](
      successCallback = successCallback,
      errorCallback =  e=>ctx.logger.error(e.getMessage) *> acker.reject(deliveryTag = envelope.deliveryTag),
    )

  }



  def slice()(implicit ctx:NodeContext, envelope:AmqpEnvelope[String], acker:Acker): IO[Unit] = {
    def successCallback(payload:payloads.Slice) = {
      type E                       = NodeError
      val unit                     = liftFF[Unit,NodeError](IO.unit)
      val maybeCurrentState        = EitherT.liftF[IO,E,NodeState](ctx.state.get)
      implicit val logger          = ctx.logger
      val L                        = Logger.eitherTLogger[IO,E]
      implicit val rabbitMQContext = ctx.rabbitContext
      val app = for {
        _             <- unit
        //       ______________________________________________________
        nodeId         = ctx.config.nodeId
        poolId         = ctx.config.poolId
        sinkVolume     <- EitherT.fromEither[IO](ctx.config.sinkVolumes.headOption.toRight{VolumeNotFound("")})
        sinkFile       = new File(sinkVolume)
        nodeVolume     = sinkFile.toPath.resolve(nodeId)
        fileId         = UUID.randomUUID()
        chunksSink     = nodeVolume.resolve(fileId.toString)
        sourceFile     = new File(payload.sourcePath)
        sourceFileSize =  sourceFile.length()
        metadata       = FileMetadata.fromPath(path = sourceFile.toPath)
        filename       = metadata.filename
        extension      = metadata.extension
        chunkSize      = 64000000
        proportion     = sourceFileSize.toDouble / chunkSize.toDouble
        intNumberOfChunks = proportion.toInt
        totalNumberOfChunks = if(sourceFileSize < chunkSize) 1 else math.ceil(proportion).toInt
        chunkSizeDecimal   = proportion-intNumberOfChunks
        chunkConfig    = ChunkConfig(
          outputPath = chunksSink,
          prefix = fileId.toString,
          size = chunkSize
        )
        //       ______________________________________________________
        fileNotFound       = FileNotFound(filename.value)
        sinkVolumeNotFound = VolumeNotFound(sinkFile.toString)
        sourceCondition    = Either.cond[NodeError,Boolean](sourceFile.exists(),right = true,fileNotFound)
        sinkCondition      = Either.cond[NodeError,Boolean](sinkFile.exists(),right = true,fileNotFound)
        _            <- EitherT.fromEither[IO](sourceCondition)
        _            <- L.debug("EXTENSION "+extension)
        _            <- L.debug("FILE_NAME "+filename)
        _            <- L.debug("SOURCE "+sourceFile)
        _            <- L.debug("SINK "+chunksSink)
        _            <- L.debug(s"SINK_EXISTS: ${chunksSink.toFile.exists()}")
        _            <- L.debug(s"ORIGINAL_FILE_SIZE ${sourceFile.length()}")
        _            <- L.debug(s"CHUNK_SIZE $chunkSize")
        _            <- L.debug(s"NUM_CHUNKS $totalNumberOfChunks")
        _            <- L.debug(s"PROPORTION $proportion")
        _            <- L.debug(s"CHUNKS ${intNumberOfChunks}x$chunkSize ${chunkSizeDecimal*chunkSize}x1")
        sliceIO = fileX.splitFile(sourceFile,chunkConfig =chunkConfig,maxConcurrent = 6 ).compile.drain
        _ <- liftFF[Unit,NodeError](sliceIO)
      } yield fileId


      app.value.stopwatch.flatMap { result =>
        result.result match {
          case Left(e) =>  ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
          case Right(value) =>
            for{
              _ <- acker.ack(envelope.deliveryTag)
              _ <- ctx.logger.info(s"SLICE ")
            } yield ( )
        }
      }
    }

    processMessageV2[IO,payloads.Slice,NodeContext](
      successCallback =  successCallback,
      errorCallback =  e=>ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag),

    )

  }




  def compressCompleted()(implicit ctx:NodeContext,envelope: AmqpEnvelope[String],acker:Acker):IO[Unit] = {
    def successCallback(payload:payloads.CompressCompleted) = {
      type E                       = NodeError
      val unit                     = liftFF[Unit,NodeError](IO.unit)
      val maybeCurrentState        = EitherT.liftF[IO,E,NodeState](ctx.state.get)
      val maybeMessageId           = EitherT.fromEither[IO](envelope.properties.messageId.toRight{NoMessageId()})
      implicit val logger          = ctx.logger
      val L                        = Logger.eitherTLogger[IO,E]
      implicit val rabbitMQContext = ctx.rabbitContext
      val app = for {
        timestamp    <- liftFF[Long,NodeError](IO.realTime.map(_.toSeconds))
        currentState <- maybeCurrentState
        taskId       <- maybeMessageId
        _            <- L.debug(s"COMPRESS_COMPLETED $taskId ${payload.subTaskId}")
        _            <- L.debug(currentState.pendingTasks.mkString(","))
        task         <-  EitherT.fromEither[IO](currentState.pendingTasks.get(taskId).toRight{TaskNotFound(taskId)})
        compressTask = task.asInstanceOf[CompressionTask]
        updatedTask  = compressTask.copy(subTasks = compressTask.subTasks.filter(_.id!= payload.subTaskId))
        newState     <- liftFF[NodeState,NodeError]{
          ctx.state.updateAndGet(s=>s.copy(pendingTasks =  s.pendingTasks.updated(taskId,updatedTask) ))
        }
        newTask     <- EitherT.fromOption[IO].apply[NodeError,Task](newState.pendingTasks.get(taskId),TaskNotFound(taskId))
        _           <- L.debug(newTask.toString)
        _ <- if(newTask.subTasks.isEmpty)
          L.debug(s"COMPRESSION_TASK_DONE $taskId ${timestamp-newTask.startedAt}")
        else unit
      } yield ()
      app.value.stopwatch.flatMap{ result=>
        result.result match {
          case Left(e) =>  acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
          case Right(_) => for {
            _ <- acker.ack(envelope.deliveryTag)
            _ <- ctx.logger.info(s"COMPRESS_COMPLETED ${result.duration}")
          } yield ()
        }
      }
    }

    processMessageV2[IO,payloads.CompressCompleted,NodeContext](
      successCallback =  successCallback,
      errorCallback =  e=> ctx.logger.error(e.getMessage)*>acker.reject(deliveryTag = envelope.deliveryTag)
    )
  }

  def compress()(implicit ctx:NodeContext,envelope:AmqpEnvelope[String],acker:Acker): IO[Unit] = {
    def successCallback(payload:payloads.Compress) = {
      type E                = NodeError
      val maybeCurrentState = EitherT.liftF[IO,E,NodeState](ctx.state.get)
      val unit              = liftFF[Unit,NodeError](IO.unit)
      val maybeReplyTo      = EitherT.fromEither[IO](envelope.properties.replyTo.toRight{NoReplyTo()})
      val maybeMessageId    = EitherT.fromEither[IO](envelope.properties.messageId.toRight{NoMessageId()})
      implicit val logger   = ctx.logger
      val L                 = Logger.eitherTLogger[IO,E]
      implicit val rabbitMQContext = ctx.rabbitContext

      val app = for {
        currentState         <- maybeCurrentState
        compressionAlgorithm = compression.fromString(payload.compressionAlgorithm)
        source               = payload.sourcePath
        sourcePath           = Paths.get(source)
        replyTo              <- maybeReplyTo
        taskId               <- maybeMessageId
        publisher            <- EitherT.fromOption[IO].apply[NodeError,PublisherV2](currentState.publishers.get(replyTo),PublisherNotFound())
        //       /data
        sourceNameCount      = sourcePath.getNameCount
        //       $FILE_ID
        chunkName            = sourcePath.subpath(sourceNameCount-1,sourceNameCount).toString
        //        /data/$FILE_ID/compressed
        destinationPath      = Paths.get("/").resolve(sourcePath.subpath(0,sourceNameCount-2))
          .resolve("compressed")
        destinationFile      = destinationPath.toFile
        //       ________________________________________________
        res <- liftFF[Boolean,E](IO.delay{destinationFile.mkdir()})
        _ <- L.debug(s"SOURCE $source")
        _ <- L.debug(s"SOURCE_EXISTS ${sourcePath.toFile.exists()}")
        _ <- L.debug(s"CHUNK_NAME $chunkName")
        _ <- L.debug(s"CHUNK_DESTINATION $destinationPath")
        _ <- L.debug(s"CHUNK_DESTINATION_FILE $destinationFile")
        _ <- L.debug(s"CHUNK_DESTINATION_EXISTS ${destinationFile.exists()}")
        _ <- L.debug(s"RES $res")
        //       ___________________________________________________________
        result <- compression
          .compress(compressionAlgorithm,source = source,destination = destinationPath.toString)
          .leftMap(e=>CompressionError(e.getMessage))
        _ <- L.debug(result.toString)
      } yield (publisher,taskId,chunkName,result)
//  _____________________________________________________________________________________
      app.value.stopwatch.flatMap {result=>
        result.result match {
          case Left(e) =>  ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
          case Right(value) => value match{
            case (publisher,taskId,chunkName,compressionStats) =>  for {
              _             <- acker.ack(envelope.deliveryTag)
              currentState  <- ctx.state.get
              sourcePath    = Paths.get(payload.sourcePath)
              chunkMetadata = FileMetadata.fromPath(sourcePath)
              chunk         = sourcePath.toFile
              deleteDelay   = 1200
              properties    = AmqpProperties(
                headers = Map("commandId"->StringVal("COMPRESS_COMPLETED")),
                messageId = taskId.some
              )
              messagePayload = CompressCompleted(chunkName).asJson.noSpaces
              message       = AmqpMessage[String](payload = messagePayload,properties = properties)
              _             <- publisher.publish(message)
              _ <- (IO.sleep(deleteDelay milliseconds)*>IO.delay{chunk.delete()} *> ctx.logger.info(s"DELETE_CHUNK ${chunkMetadata.filename} ${chunkMetadata.size.value.getOrElse(0)}")).start
              _ <- ctx.logger.info(s"COMPRESSION_DATA ${result.duration}")
            } yield ()
          }
        }
      }
    }

    processMessageV2[IO,payloads.Compress,NodeContext](
      successCallback =  successCallback,
      errorCallback =  e=>ctx.logger.error(e.getMessage) *> acker.reject(deliveryTag = envelope.deliveryTag),

    )

  }


  def decompress()(implicit ctx:NodeContext,envelope:AmqpEnvelope[String],acker:Acker): IO[Unit] = {
    def successCallback(payload:payloads.Decompress) = {
      type E                = NodeError
      val maybeCurrentState = EitherT.liftF[IO,E,NodeState](ctx.state.get)
      val maybeReplyTo      = EitherT.fromEither[IO](envelope.properties.replyTo.toRight{NoReplyTo()})
      val maybeMessageId    = EitherT.fromEither[IO](envelope.properties.messageId.toRight{NoMessageId()})
      val unit              = liftFF[Unit,NodeError](IO.unit)
      implicit val logger   = ctx.logger
      val L                 = Logger.eitherTLogger[IO,E]
      implicit val rabbitMQContext = ctx.rabbitContext

      val app = for {
        currentState         <- maybeCurrentState
        replyTo              <- maybeReplyTo
        publishers           = currentState.publishers
        publisher            <- EitherT.fromOption[IO].apply(publishers.get(replyTo),PublisherNotFound())
        taskId               <- maybeMessageId
        compressionAlgorithm = compression.fromString(payload.compressionAlgorithm)
        source               = payload.sourcePath
        sourcePath           = Paths.get(source)
        //       /data
        sourceNameCount      = sourcePath.getNameCount
        //       $FILE_ID
        chunkName            = sourcePath.subpath(sourceNameCount-1,sourceNameCount).toString
        //        /data/$FILE_ID/compressed
        destinationPath      = Paths.get("/").resolve(sourcePath.subpath(0,sourceNameCount-2))
//          .resolve("decompressed")
          .resolve("chunks")
        destinationFile      = destinationPath.toFile
        //       ________________________________________________
        res <- liftFF[Boolean,E](IO.delay{destinationFile.mkdir()})
        _ <- L.debug(s"SOURCE $source")
        _ <- L.debug(s"SOURCE_EXISTS ${sourcePath.toFile.exists()}")
        _ <- L.debug(s"CHUNK_NAME $chunkName")
        _ <- L.debug(s"CHUNK_DESTINATION $destinationPath")
        _ <- L.debug(s"CHUNK_DESTINATION_FILE $destinationFile")
        _ <- L.debug(s"CHUNK_DESTINATION_EXISTS ${destinationFile.exists()}")
        _ <- L.debug(s"DESTINATION_MKDIR_RESULT $res")
        //       ___________________________________________________________
        result <- compression
          .decompress(compressionAlgorithm,source = source,destination = destinationPath.toString)
          .leftMap(e=>CompressionError(e.getMessage))
        _ <- L.debug(result.toString)
      } yield (publisher,result,taskId)

      app.value.stopwatch.flatMap {result=>
        result.result match {
          case Left(e) =>  ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
          case Right(value) => for {
            _ <- acker.ack(envelope.deliveryTag)
            publisher          = value._1
            decompressionStats = value._2
            taskId             = value._3
//          DELETE_COMPRESSED_FILE
            sourcePath    = Paths.get(payload.sourcePath)
            chunkMetadata = FileMetadata.fromPath(sourcePath)
            chunk         = sourcePath.toFile
            deleteDelay    = 1200

            properties  = AmqpProperties(
              headers   = Map("commandId"->StringVal("DECOMPRESS_COMPLETED")),
              replyTo   = publisher.pubId.some,
              messageId = taskId.some
            )
            messagePayload = payloads.DecompressCompleted(subTaskId = chunkMetadata.filename.value).asJson.noSpaces
            message       = AmqpMessage[String](payload= messagePayload,properties = properties)
            _             <- publisher.publish(message)
//          _________________________________________________________________________________________
            _ <- (IO.sleep(deleteDelay milliseconds)*>IO.delay{chunk.delete()} *> ctx.logger.info(s"DELETE_CHUNK ${chunkMetadata.filename} ${chunkMetadata.size.value.getOrElse(0)}")).start
            _ <- ctx.logger.info(s"DECOMPRESSION_DATA ${result.duration}")
//            _ <- ctx.logger.debug(s"REPLY_TO ${}")
          } yield ()
        }
      }
    }

    processMessageV2[IO,payloads.Decompress,NodeContext](
      successCallback =  successCallback,
      errorCallback =  e=>ctx.logger.error(e.getMessage) *> acker.reject(deliveryTag = envelope.deliveryTag),

    )

  }


  def decompressCompleted()(implicit ctx:NodeContext,envelope: AmqpEnvelope[String],acker:Acker):IO[Unit] = {
    def successCallback(payload:payloads.CompressCompleted) = {
      type E                       = NodeError
      val unit                     = liftFF[Unit,NodeError](IO.unit)
      val maybeCurrentState        = EitherT.liftF[IO,E,NodeState](ctx.state.get)
      val maybeMessageId           = EitherT.fromEither[IO](envelope.properties.messageId.toRight{NoMessageId()})
      val maybeReplyTo      = EitherT.fromEither[IO](envelope.properties.replyTo.toRight{NoReplyTo()})
      implicit val logger          = ctx.logger
      val L                        = Logger.eitherTLogger[IO,E]
      implicit val rabbitMQContext = ctx.rabbitContext
      val app = for {
        timestamp    <- liftFF[Long,NodeError](IO.realTime.map(_.toSeconds))
        currentState <- maybeCurrentState
        replyTo      <- maybeReplyTo
        taskId       <- maybeMessageId
        _            <- L.debug(s"DECOMPRESS_COMPLETED $taskId ${payload.subTaskId}")
        _            <- L.debug(currentState.pendingTasks.mkString(","))
        task         <-  EitherT.fromEither[IO](currentState.pendingTasks.get(taskId).toRight{TaskNotFound(taskId)})
        decompressTask = task.asInstanceOf[DecompressionTask]
        updatedTask  = decompressTask.copy(subTasks = decompressTask.subTasks.filter(_.id!= payload.subTaskId))
        newState     <- liftFF[NodeState,NodeError]{
          ctx.state.updateAndGet(s=>s.copy(pendingTasks =  s.pendingTasks.updated(taskId,updatedTask) ))
        }
        newTask     <- EitherT.fromOption[IO].apply[NodeError,Task](newState.pendingTasks.get(taskId),TaskNotFound(taskId))
//        masterNodeId = decompressTask.mn
        mergeMsgProperties = AmqpProperties(
          headers = Map("commandId"->StringVal("MERGE")),
          replyTo = replyTo.some
        )
        _           <- L.debug(newTask.toString)
        _ <- if(newTask.subTasks.isEmpty) for{
//          _            <- L.debug(s"DECOMPRESSION_TASK_DONE $taskId ${timestamp-newTask.startedAt}")
          publisher       <- EitherT.fromOption[IO].apply[NodeError,PublisherV2](currentState.publishers.get(replyTo),PublisherNotFound())
          sourcePath      = decompressTask.sourcePath
          filename        = decompressTask.filename.value
          extension       = decompressTask.extension.value
          mergeMsgPayload = payloads.Merge(sourcePath = sourcePath,filename = filename,extension = extension).asJson.noSpaces
          mergeMessage    = AmqpMessage[String](payload= mergeMsgPayload, properties = mergeMsgProperties)
          _               <- liftFF[Unit,NodeError](publisher.publish(mergeMessage))
//          _ <-
        } yield()
        else unit
      } yield ()
      app.value.stopwatch.flatMap{ result=>
        result.result match {
          case Left(e) =>  acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
          case Right(_) => for {
            _ <- acker.ack(envelope.deliveryTag)
            _ <- ctx.logger.info(s"DECOMPRESS_COMPLETED ${result.duration}")
          } yield ()
        }
      }
    }

    processMessageV2[IO,payloads.CompressCompleted,NodeContext](
      successCallback =  successCallback,
      errorCallback =  e=> ctx.logger.error(e.getMessage)*>acker.reject(deliveryTag = envelope.deliveryTag)
    )
  }

}
