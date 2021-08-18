package mx.cinvestav

import cats.implicits._
import cats.data.EitherT
import cats.effect._
import ch.qos.logback.core.util.FileSize
import com.github.gekomad.scalacompress.DecompressionStats
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.StringVal
import dev.profunktor.fs2rabbit.model.{AmqpEnvelope, AmqpMessage, AmqpProperties}
import io.circe.generic.auto._
import mx.cinvestav.Declarations.{ChunkLocation, CompressionError, CompressionSubTask, CompressionTask, DecompressionSubTask, DecompressionTask, DownloadError, FileNotFound, MergeError, NoMessageId, NoReplyTo, NodeContext, NodeError, NodeState, PathIsNotDirectory, PublisherNotFound, Task, TaskNotFound, VolumeNotFound, payloads}
import mx.cinvestav.commons.fileX.{ChunkConfig, Extension, FileMetadata, Filename}
import mx.cinvestav.utils.v2.{Acker, PublisherV2, fromNodeToPublisher, processMessageV2}
import mx.cinvestav.commons.{fileX, liftFF}
import org.typelevel.log4cats.Logger
import fs2.Stream
import io.circe._
import io.circe.syntax._
import io.circe.generic.auto._
import mx.cinvestav.Declarations.payloads.{CompressCompleted, CompressedChunkLocation}
import mx.cinvestav.commons.compression

import concurrent.duration._
import language.postfixOps
import java.io.File
import java.util.UUID
import mx.cinvestav.commons.stopwatch.StopWatch._
import mx.cinvestav.utils.v2.encoders._
import mx.cinvestav.commons.fileX.downloadFromURL
import mx.cinvestav.commons.nodes.Node
import mx.cinvestav.server.Client

import java.net.URL
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
//      GET TIMESTAMP
        _ <- L.debug("MERGE_INIT")
        timestamp           <- liftFF[Long,NodeError](IO.realTime.map(_.toSeconds))
        //       CURRENT STATE
        currentState        <- maybeCurrentState
        //       NODE_ID
        nodeId              = ctx.config.nodeId
        //       TASK_ID
        decompressTaskId    =  UUID.randomUUID().toString
        compressedChunks         = payload.compressedChunks
        chunksMetadata = compressedChunks
          .flatMap(_.sources)
          .map(Paths.get(_))
          .map(FileMetadata.fromPath)
//        _ <- L.debug(compressedChunksMetadata.mkString(","))
        //  FILE_ID
        fileId              = payload.fileId
        //
        sinkFolder          = s"${ctx.config.sinkFolder}/$nodeId/$fileId/compressed"
        sinkFolderPath      = Paths.get(sinkFolder)
        _                   = sinkFolderPath.toFile.mkdirs()
//        chunksMetadata      <- liftFF[List[FileMetadata],NodeError](
//          Helpers.downloadCompressedChunks(compressedChunks,sinkFolderPath)
//        )
        // AVAILABLE DP nodes
        dataPreparationNodes = ctx.config.dataPreparationNodes
        nodeIds              = dataPreparationNodes.map(_.nodeId).filter(_!=nodeId)
        balancedNodes        = currentState.loadBalancer.balanceMulti(nodes = nodeIds,rounds = compressedChunks.length,allowRep = true)
        pubs                 <- EitherT.fromEither[IO](balancedNodes.traverse(currentState.publishers.get).toRight{PublisherNotFound()})
        pubsStream           = Stream.emits(pubs).covary[IO]
        subTasks             = (chunksMetadata zip balancedNodes)
          .map{
            case (chm,nId) =>
              DecompressionSubTask(id=chm.filename.value,taskId = decompressTaskId,nodeId = nId)
          }
        filename       = Filename(payload.filename)
        extension      = Extension(payload.extension)
        decompressTask = DecompressionTask(
          id         = decompressTaskId,
          subTasks   = subTasks,
          startedAt  = timestamp,
          filename   = filename,
          extension  = extension
//          sourcePath = ""
        )
        newTask       = (decompressTaskId->  decompressTask)
        newState      <- liftFF[NodeState,NodeError](ctx.state.updateAndGet(s=>s.copy(pendingTasks = s.pendingTasks+newTask)))
//        _             <- L.debug(newState.toString)
        _             <- liftFF[Unit,NodeError](
          rabbitMQContext.client.createChannel(conn = rabbitMQContext.connection).use{ implicit channel=>
            val data = pubsStream zip Stream.emits(compressedChunks)
            data.evalMap{
              case (publisher, chunkLocation) =>
                  val messagePayload = payloads.Decompress(
                    compressionAlgorithm = payload.compressionAlgorithm,
                    fileId = fileId,
                    compressedChunksLocations= chunkLocation::Nil
                  ).asJson.noSpaces

                  val properties = AmqpProperties(
                            headers = Map("commandId"->StringVal("DECOMPRESS")) ,
                            replyTo = nodeId.some,
                            messageId = decompressTaskId.some
                  )
                  val message = AmqpMessage(payload = messagePayload,properties = properties)
                  publisher.publishWithChannel(message)
//                *> ctx.logger.debug(s"${publisher.pubId} $chunk")
//                IO.unit
            }.compile.drain
          }
        )
//            Stream
//              .emits(chunksFile)
//              .zip(pubsStream)
//              .covary[IO]
//              .evalMap{
//                case (chunk, publisher) =>
//                  val messagePayload = payloads.Decompress(sourcePath = chunk.toPath.toString,compressionAlgorithm = payload.compressionAlgorithm).asJson.noSpaces
//                  val properties = AmqpProperties(
//                    headers = Map("commandId"->StringVal("DECOMPRESS")) ,
//                    replyTo = nodeId.some,
//                    messageId = decompressTaskId.some
//                  )
//                  val message = AmqpMessage(payload = messagePayload,properties = properties)
//                  publisher.publishWithChannel(message) *> ctx.logger.debug(s"${publisher.pubId} $chunk")
//              }
//              .compile.drain
//          }
//        )
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
//      get current time
        timestamp           <- liftFF[Long,NodeError](IO.realTime.map(_.toSeconds))
//      get current state
        currentState        <- maybeCurrentState
//      URL
        url                 = new URL(payload.url)
        urlPath             = Paths.get(url.getPath)
        urlMetadata         = FileMetadata.fromPath(urlPath)
//       NODE_ID
        nodeId              = ctx.config.nodeId
        nodeIds             = ctx.config.dataPreparationNodes.map(_.nodeId).filter(_!=nodeId)
//      FILE_ID
        fileId              = UUID.randomUUID()
//      TASK_ID
        compressionTaskId   = UUID.randomUUID().toString
//      SINK_FOLDER
        sinkFolder          = ctx.config.sinkFolder
        sinkFile            = new File(sinkFolder)
//      SINK_FOLDER/NODE_ID
        nodeVolume          = sinkFile.toPath.resolve(nodeId)
//      SINK_FOLDER/NODE_ID/FILE_ID
        fileIdWithExtension = fileId+"."+urlMetadata.extension.value
        chunksSink          = nodeVolume
          .resolve(fileId.toString)
          .resolve(fileIdWithExtension)
//      RESOURCE_URL
        resourceURL         = s"http://${currentState.ip}:${ctx.config.port}"
//       OUPUT_PATH
        outputPath          = nodeVolume.resolve(Paths.get(fileId.toString))
//      CREATE OUTPUT_FOLDER
         _                  = outputPath.toFile.mkdirs()
//      DOWNLOAD THE FILE FROM SERVER
        _                   <- EitherT.fromEither[IO](
          downloadFromURL(url,chunksSink.toFile)
            .leftMap(DownloadError)
        )
        //      ________________________________________________________________
//      SINK_FOLDER/NODE_ID/FILE_ID
        sourceFile          =  chunksSink.toFile
        sourceFileSize      =  sourceFile.length()
        //      ________________________________________________________________
//      METADATA
        metadata            = FileMetadata.fromPath(path = sourceFile.toPath)
        filename            = metadata.filename
        extension           = metadata.extension
//      CHUNKS DATAAAAAAA
        chunkSize           = payload.chunkSize
        proportion          = sourceFileSize.toDouble / chunkSize.toDouble
        intNumberOfChunks   = proportion.toInt
        totalNumberOfChunks = if(sourceFileSize < chunkSize) 1 else math.ceil(proportion).toInt
        chunkSizeDecimal    = proportion-intNumberOfChunks
        chunkConfig    = ChunkConfig(
          outputPath = outputPath,
          prefix     = fileId.toString,
          size       = chunkSize.toInt
        )
        //       ______________________________________________________
        fileNotFound       = FileNotFound(filename.value)
        sourceCondition    = Either.cond[NodeError,Boolean](sourceFile.exists(),right = true,fileNotFound)
        _            <- EitherT.fromEither[IO](sourceCondition)
        _            <- L.debug("RESOURCE_URL "+resourceURL)
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
//       SLICES
        sliceStream            = fileX.splitFile(sourceFile,chunkConfig =chunkConfig,maxConcurrent = 6 )
//      LOAD BALANCER
        balancedNodes          = currentState.loadBalancer.balanceMulti(nodes = nodeIds,rounds = totalNumberOfChunks,allowRep = true)
//
        pubs                   <- EitherT.fromEither[IO](balancedNodes.traverse(currentState.publishers.get).toRight{PublisherNotFound()})
        pubsStream             = Stream.emits(pubs).covary[IO]
        //      ________________________________________________________________________________________________________________
        compressChunks = rabbitMQContext.client.createChannel(conn = rabbitMQContext.connection)  .use{ implicit channel =>
          sliceStream.zip(pubsStream).evalMap{
            case (chunkInfo, publisher) =>
//            _____________________________________________________________________
              val props   = AmqpProperties(
                headers = Map(
                  "commandId" -> StringVal("COMPRESS")
                ),
                replyTo = nodeId.some,
                messageId = compressionTaskId.some
              )
//            _____________________________________________________________________
              val compressPayload = payloads.Compress(
                fileId=fileId.toString,
                url = resourceURL,
                compressionAlgorithm = payload.compressionAlgorithm,
                source = chunkInfo.sourcePath
              ).asJson.noSpaces
//            _____________________________________________________________________
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
        pendingCompressionTask = CompressionTask(
          id        = fileId.toString ,
          subTasks  = subTasks,
          startedAt = timestamp,
          filename  = fileIdWithExtension,
          userId    = payload.userId
        )
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
        nodeId       = ctx.config.nodeId
        poolId       = ctx.config.poolId
        loadBalancerId = ctx.config.loadBalancer
        taskId       <- maybeMessageId
        _            <- L.debug(s"COMPRESS_COMPLETED $taskId ${payload.subTaskId} ${payload.source}")
        _            <- L.debug(currentState.pendingTasks.mkString(","))
        task         <-  EitherT.fromEither[IO](currentState.pendingTasks.get(taskId).toRight{TaskNotFound(taskId)})
        compressTask = task.asInstanceOf[CompressionTask]
        newChunkLoc = ChunkLocation(url= payload.url,source = payload.source)
        updatedTask  = compressTask.copy(
          subTasks = compressTask.subTasks.filter(_.id!= payload.subTaskId),
          chunkLocations = compressTask.chunkLocations :+ newChunkLoc
        )
        newState     <- liftFF[NodeState,NodeError]{
          ctx.state.updateAndGet(s=>s.copy(pendingTasks =  s.pendingTasks.updated(taskId,updatedTask) ))
        }
        newTask     <- EitherT.fromOption[IO].apply[NodeError,Task](newState.pendingTasks.get(taskId),TaskNotFound(taskId))
        newCompressTask = newTask.asInstanceOf[CompressionTask]
//       _________________________________________

        sourceMetadata = FileMetadata.fromPath(Paths.get(payload.source))
        oldChunkName   = sourceMetadata.filename.value
        oldChunkPath   = s"${ctx.config.sinkFolder}/$nodeId/${compressTask.id}/chunks/$oldChunkName"
        chunk          = new File(oldChunkPath)
        _              <- liftFF[Unit,NodeError](IO.delay{chunk.delete()} )
        _              <- L.debug(s"DELETE_CHUNK ${payload.subTaskId} $oldChunkPath")
//      ________________________________________________________________________________
        _              <- L.debug(newTask.toString)
        _ <- if(newTask.subTasks.isEmpty) for{
          _ <- L.debug(s"COMPRESSION_TASK_DONE $taskId ${timestamp-newTask.startedAt}")
          _ <- L.debug(s"DELETE_ORIGINAL_FILE ${compressTask.filename}")
          originalFile = new File(s"${ctx.config.sinkFolder}/$nodeId/${compressTask.id}/${compressTask.filename}")
          _     <- liftFF[Unit,NodeError](IO.delay{originalFile.delete()} )
          loadBalancerNode = Node(poolId,loadBalancerId)
          loadBalancerPub  = fromNodeToPublisher(loadBalancerNode)
          properties = AmqpProperties(headers = Map("commandId"->StringVal("BALANCE")))
          msgPayload = Json.obj(
            "fileId"-> compressTask.id.asJson,
                    "userId" -> compressTask.userId.asJson,
                     "chunks" -> newCompressTask.chunkLocations.asJson
          ).noSpaces
          message    = AmqpMessage[String](payload = msgPayload,properties = properties)
          _          <- liftFF[Unit,NodeError](loadBalancerPub.publish(message))
          _ <- L.debug("MESSAGE TO BALANCER")
//          _  <-
        } yield ()
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
//      CURRENT_STATE
        currentState         <- maybeCurrentState
        //      GET COMPRESSION ALGORITTHM
        compressionAlgorithm = compression.fromString(payload.compressionAlgorithm)
        //      NODE_ID
        nodeId               = ctx.config.nodeId
        fileId               = payload.fileId
        chunkSink            = s"${ctx.config.sinkFolder}/$nodeId/$fileId/chunks"
        chunkCompressedSink  = s"${ctx.config.sinkFolder}/$nodeId/$fileId/compressed"
        chunkSinkPath        = Paths.get(chunkSink)
        chunkCompressedSinkPath = Paths.get(chunkCompressedSink)
        _                    = chunkCompressedSinkPath.toFile.mkdirs()
        _                    = chunkSinkPath.toFile.mkdirs()
        //      RESOURCE_URL
        //      http://localhost:6000
        url                  = new URL(payload.url)
        //       /data/dp-0/FILE_ID/FILE_ID_CHUNK-INDEX
        urlSource            = payload.source
        urlSourcePath        = Paths.get(urlSource)
        //      FILE_NAME = FILE_ID_CHUNK-INDEX  / EXTENSION = ""
        urlSourceMetadata    = FileMetadata.fromPath(urlSourcePath)
        chunkName            = urlSourceMetadata.fullname
//       FILE_ID/FILE_ID_CHUNK-INDEX
        chunkSinkDestination = chunkSinkPath.resolve(urlSourceMetadata.fullname)
        chunkSinkDestinationFile = chunkSinkDestination.toFile
        //      DOWNLOAD THE CHUNKS AND SAVE IN /FILE_ID/CHUNKS/FILE_ID_CHUNK-INDEX
        _                    <- Client.downloadFileE(
//        http://localhost:6000
          url         = url.toString,
//        /data/dp-0/FILE_ID/FILE_ID_CHUNK-INDEX
          sourcePath  = urlSource,
//
          destination = chunkSinkDestination
        )
        //      ______________________________________________________________________
        //        sourcePath           = Paths.get(source)
        replyTo              <- maybeReplyTo
        taskId               <- maybeMessageId
        //
        publisher            <- EitherT.fromOption[IO].apply[NodeError,PublisherV2](currentState.publishers.get(replyTo),PublisherNotFound())
        //       ________________________________________________
        //        res <- liftFF[Boolean,E](IO.delay{destinationFile.mkdir()})
        _ <- L.debug(s"URL ${payload.url}")
        _ <- L.debug(s"SOURCE ${payload.source}")
        _ <- L.debug(s"SOURCE_EXISTS ${urlSourcePath.toFile.exists()}")
        _ <- L.debug(s"CHUNK_NAME ${urlSourceMetadata.fullname}")
        _ <- L.debug(s"CHUNK_DESTINATION $chunkSinkDestination")
        _ <- L.debug(s"CHUNK_DESTINATION_EXISTS ${chunkSinkDestination.toFile.exists()}")
        _ <- L.debug(s"CHUNK_COMPRESED_DESTINATION $chunkCompressedSink")
        _ <- L.debug(s"CHUNK_DESTINATION_EXISTS ${chunkSinkDestination.toFile.exists()}")
        //        _ <- L.debug(s"RES $res")
        //       ___________________________________________________________
        result <- compression
          .compress(
            compressionAlgorithm,
            source = chunkSinkDestination.toString,
            destination = chunkCompressedSink
          )
          .leftMap(e=>CompressionError(e.getMessage))
//          .flatMap(x=>)
        //       _____________________________________________________
        _ <- liftFF[Unit,NodeError](IO.delay{chunkSinkDestinationFile.delete()})
        _ <- L.debug(result.toString)
//
      } yield (publisher,taskId,chunkName,result)
//  _____________________________________________________________________________________
      app.value.stopwatch.flatMap {result=>
        result.result match {
          case Left(e) =>  ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
          case Right(value) => value match{
            case (publisher,taskId,chunkName,compressionStats) =>  for {
              _             <- acker.ack(envelope.deliveryTag)
              currentState  <- ctx.state.get
              port          = ctx.config.port
              properties    = AmqpProperties(
                headers = Map("commandId"->StringVal("COMPRESS_COMPLETED")),
                messageId = taskId.some
              )
              messagePayload = CompressCompleted(
                subTaskId = chunkName,
                source = compressionStats.fileOut,
                url = s"http://${currentState.ip}:$port"
              ).asJson.noSpaces
              message       = AmqpMessage[String](payload = messagePayload,properties = properties)
              _             <- publisher.publish(message)
//              _ <- (IO.sleep(deleteDelay milliseconds)*>IO.delay{chunk.delete()} *> ctx.logger.info(s"DELETE_CHUNK ${chunkMetadata.filename} ${chunkMetadata.size.value.getOrElse(0)}")).start
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
//      CURRENT_STATE
        _                    <- L.debug("DECOMPRESS_INIT")
        currentState         <- maybeCurrentState
        nodeId               = ctx.config.nodeId
        fileId               = payload.fileId
//      REPLY_TO
        replyTo              <- maybeReplyTo
//      PUBLISHER
        publishers           = currentState.publishers
        publisher            <- EitherT.fromOption[IO].apply(publishers.get(replyTo),PublisherNotFound())
//      TASK_ID
        taskId               <- maybeMessageId
//      COMPRESSION_ALGORITHM
        compressionAlgorithm = compression.fromString(payload.compressionAlgorithm)
//       SINK_FOLDER
        sinkFolder           = s"${ctx.config.sinkFolder}/$nodeId/$fileId/compressed"
        sinkFolderPath       = Paths.get(sinkFolder)
        _                    = sinkFolderPath.toFile.mkdirs()
        sinkUnCompressed     = s"${ctx.config.sinkFolder}/$nodeId/$fileId/chunks"
        sinkUnCompressedPath = Paths.get(sinkUnCompressed)
//
        compressedChunks     = payload.compressedChunksLocations

//     DOWNLOAD_COMPRESSED_CHUNKS
        chunksMetadata       <- liftFF[List[FileMetadata],NodeError](
          Helpers.downloadCompressedChunks(compressedChunks,sinkFolderPath)
        )
        _ <- L.debug("DOWNLOAD_CHUNKS!")

        chunksNames = chunksMetadata.map(_.fullname)
        sources = chunksNames
          .map(sinkFolderPath.resolve)

        destinations = chunksMetadata.map(_.filename.value).map(_=>sinkUnCompressedPath)
        sourcesAndDestinations = sources zip destinations

        results  <- sourcesAndDestinations.traverse {
            case (source, dest) =>
            compression
              .decompress(ca = compressionAlgorithm,
                source = source.toString,
                destination = dest.toString
              )
              .leftMap(e=>CompressionError(e.getMessage))

          }
//        _ <- EitherT.apply[IO,NodeError,List[DecompressionStats]](results)

        properties  = AmqpProperties(
          headers   = Map("commandId"->StringVal("DECOMPRESS_COMPLETED")),
          replyTo   = publisher.pubId.some,
          messageId = taskId.some
        )
        messagePayloads = chunksMetadata.map(chm=>payloads.DecompressCompleted(subTaskId = chm.filename.value).asJson.noSpaces)
        messages        = messagePayloads.map(mp=>AmqpMessage[String](payload= mp,properties = properties))
        x               <- liftFF[Unit,NodeError](rabbitMQContext.client.createChannel(rabbitMQContext.connection).use{ implicit channel=>
           messages.traverse(m => publisher.publishWithChannel(m)).void
        })
//        _ <- L.debug(results.mkString(","))
      } yield (publisher,taskId)

      app.value.stopwatch.flatMap {result=>
        result.result match {
          case Left(e) =>  ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
          case Right(value) => for {
            _ <- acker.ack(envelope.deliveryTag)
//            publisher          = value._1
//            decompressionStats = value._2
//            taskId             = value._3
//          DELETE_COMPRESSED_FILE
//          _________________________________________________________________________________________
//            _ <- (IO.sleep(deleteDelay milliseconds)*>IO.delay{chunk.delete()} *> ctx.logger.info(s"DELETE_CHUNK ${chunkMetadata.filename} ${chunkMetadata.size.value.getOrElse(0)}")).start
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
//      TIMESTAMP
        timestamp    <- liftFF[Long,NodeError](IO.realTime.map(_.toSeconds))
//      CURRENT STATE
        currentState <- maybeCurrentState
//      REPLY_TO
        replyTo      <- maybeReplyTo
//      TASK_ID
        taskId       <- maybeMessageId
//
        _            <- L.debug(s"DECOMPRESS_COMPLETED $taskId ${payload.subTaskId}")
        _            <- L.debug(currentState.pendingTasks.mkString(","))
//
        task         <-  EitherT.fromEither[IO](currentState.pendingTasks.get(taskId).toRight{TaskNotFound(taskId)})
        decompressTask = task.asInstanceOf[DecompressionTask]
        updatedTask  = decompressTask.copy(subTasks = decompressTask.subTasks.filter(_.id!= payload.subTaskId))
//      NEW_STATE
        newState     <- liftFF[NodeState,NodeError]{
          ctx.state.updateAndGet(s=>s.copy(pendingTasks =  s.pendingTasks.updated(taskId,updatedTask) ))
        }
        newTask     <- EitherT.fromOption[IO].apply[NodeError,Task](newState.pendingTasks.get(taskId),TaskNotFound(taskId))
//        masterNodeId = decompressTask.mn
//        mergeMsgProperties = AmqpProperties(
//          headers = Map("commandId"->StringVal("MERGE")),
//          replyTo = replyTo.some
//        )
        _           <- L.debug(newTask.toString)
        _ <- if(newTask.subTasks.isEmpty) L.debug("FINISH_DECOMPRESSION")
        else unit
////          _            <- L.debug(s"DECOMPRESSION_TASK_DONE $taskId ${timestamp-newTask.startedAt}")
//          publisher       <- EitherT.fromOption[IO].apply[NodeError,PublisherV2](currentState.publishers.get(replyTo),PublisherNotFound())
//          sourcePath      = decompressTask.sourcePath
//          filename        = decompressTask.filename.value
//          extension       = decompressTask.extension.value
//          mergeMsgPayload = payloads.Merge(sourcePath = sourcePath,filename = filename,extension = extension).asJson.noSpaces
//          mergeMessage    = AmqpMessage[String](payload= mergeMsgPayload, properties = mergeMsgProperties)
//          _               <- liftFF[Unit,NodeError](publisher.publish(mergeMessage))
////          _ <-
//        } yield()
//        else unit
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
