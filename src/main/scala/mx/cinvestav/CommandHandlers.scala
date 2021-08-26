package mx.cinvestav

import cats.implicits._
import cats.data.EitherT
import cats.effect._
import dev.profunktor.fs2rabbit.model.{ExchangeName, RoutingKey}
import mx.cinvestav.utils.v2
import mx.cinvestav.utils.v2.PublisherConfig
import org.apache.commons.io.FileUtils
//_________________
import fs2.Stream
//__________________________
import io.circe._
import io.circe.syntax._
import io.circe.generic.auto._
//_______________________
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.StringVal
import dev.profunktor.fs2rabbit.model.{AmqpEnvelope, AmqpMessage, AmqpProperties}
//
import mx.cinvestav.commons.errors._
import mx.cinvestav.commons.types._
//{ChunkMetadata,ChunkLocation,CompressionSubTask,CompressionTask,DecompressionSubTask,DecompressionTask}
import mx.cinvestav.commons.payloads.{v2=>payloads}
import mx.cinvestav.Declarations.{NodeContext, NodeState}

import mx.cinvestav.commons.fileX.{ChunkConfig, Extension, FileMetadata, Filename}
import mx.cinvestav.utils.v2.{Acker, PublisherV2, fromNodeToPublisher, processMessageV2}
import mx.cinvestav.commons.{fileX, liftFF}
import org.typelevel.log4cats.Logger
import mx.cinvestav.commons.compression
//_______________________
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
//  SLICE & COMPRESS
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
        timestamp           <- liftFF[Long,NodeError](IO.realTime.map(_.toMillis))
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
        resourceURL         = s"http://${currentState.ip}:${ctx.config.port}/download"
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
        //      CHUNKS DATA
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
        balancedNodes          = currentState.loadBalancer.balanceMulti(
          nodes = nodeIds,
          rounds = totalNumberOfChunks,
          allowRep = true,
          _counter =  Map.empty[String,Int]
        )
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
                fileId               = fileId.toString,
                url                  = resourceURL,
                compressionAlgorithm = payload.compressionAlgorithm,
                source               = chunkInfo.sourcePath,
                chunkIndex           = chunkInfo.index,
                timestamp            = timestamp
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
          id                   = fileId.toString ,
          subTasks             = subTasks,
          startedAt            = timestamp,
          filename             = fileIdWithExtension,
          userId               = payload.userId,
          compressionAlgorithm = payload.compressionAlgorithm,
          extension            = urlMetadata.extension.value
        )
        newTask = (compressionTaskId->pendingCompressionTask)
        updatedState           <- liftFF[NodeState,NodeError](ctx.state.updateAndGet(s=>s.copy(pendingTasks = s.pendingTasks + newTask  )))
        _ <-L.debug(updatedState.toString)

      } yield (fileId,compressionTaskId)

      app.value.stopwatch.flatMap{result=>
        result.result match {
          case Left(e) => acker.reject(deliveryTag = envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
          case Right(value) => value match {
            case (fileId,compressionTaskId)=> for {
                _ <- acker.ack(deliveryTag = envelope.deliveryTag)
                _ <- ctx.logger.info(s"SLICE_COMPRESS $fileId $compressionTaskId ${result.duration.toMillis}")
              } yield ()
          }
        }
      }
    }
    processMessageV2[IO,payloads.SliceAndCompress,NodeContext](
      successCallback = successCallback,
      errorCallback =  e=>ctx.logger.error(e.getMessage) *> acker.reject(deliveryTag = envelope.deliveryTag),
    )

  }
/// COMPRESS
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
//        TIMESTAMP
        timestamp            <- liftFF[Long,E](IO.realTime.map(_.toMillis))
        //      CURRENT_STATE
        currentState         <- maybeCurrentState
        //      GET COMPRESSION ALGORITTHM
        compressionAlgorithm = compression.fromString(payload.compressionAlgorithm)
        //      NODE_ID
        nodeId                  = ctx.config.nodeId
        fileId                  = payload.fileId
        port                    = ctx.config.port
        chunkSink               = s"${ctx.config.sinkFolder}/$nodeId/$fileId/chunks"
        chunkCompressedSink     = s"${ctx.config.sinkFolder}/$nodeId/$fileId/compressed"
        chunkSinkPath           = Paths.get(chunkSink)
        chunkCompressedSinkPath = Paths.get(chunkCompressedSink)
        _                       = chunkCompressedSinkPath.toFile.mkdirs()
        _                       = chunkSinkPath.toFile.mkdirs()
        //      RESOURCE_URL
        //      http://localhost:6000
        url                  = new URL(payload.url)
        //       /data/dp-0/FILE_ID/chunks/FILE_ID_CHUNK-INDEX
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
        compressionStats <- compression
          .compress(
            compressionAlgorithm,
            source = chunkSinkDestination.toString,
            destination = chunkCompressedSink
          )
          .leftMap(e=>CompressionError(e.getMessage))
        //       ________________________________________________
        //        res <- liftFF[Boolean,E](IO.delay{destinationFile.mkdir()})
        compressedChunkFile = chunkCompressedSinkPath.toFile
        _ <- L.info(s"COMPRESSION_LATENCY $fileId $taskId ${timestamp-payload.timestamp}")
        _ <- L.debug(s"URL ${payload.url}")
        _ <- L.debug(s"SOURCE ${payload.source}")
        _ <- L.debug(s"SOURCE_EXISTS ${urlSourcePath.toFile.exists()}")
        _ <- L.debug(s"CHUNK_NAME ${urlSourceMetadata.fullname}")
        _ <- L.debug(s"CHUNK_INDEX ${payload.chunkIndex}")
        _ <- L.debug(s"CHUNK_SIZE ${compressedChunkFile.length()}")
        _ <- L.debug(s"CHUNK_DESTINATION $chunkSinkDestination")
        //        _ <- L.debug(s"CHUNK_DESTINATION_EXISTS ${chunkSinkDestination.toFile.exists()}")
        _ <- L.debug(s"CHUNK_COMPRESED_DESTINATION $chunkCompressedSink")
        //        _ <- L.debug(s"CHUNK_DESTINATION_EXISTS ${chunkSinkDestination.toFile.exists()}")
        //       ___________________________________________________________
        //        ___________________________
        nodeUrl = s"http://${currentState.ip}:$port/download"
        chunkMetadata = ChunkMetadata(
          chunkId = urlSourceMetadata.filename.value,
          index   = payload.chunkIndex,
          sizeIn  = compressionStats.sizeIn,
          sizeOut = compressionStats.sizeOut,
          compressionAlgorithm = payload.compressionAlgorithm,
          location =  Location(url = nodeUrl,source = compressionStats.fileOut)
        )
        //       _____________________________________________________
        _ <- liftFF[Unit,NodeError](IO.delay{chunkSinkDestinationFile.delete()})
        _ <- L.debug(compressionStats.toString)
        //        _ <- L.debug(chunkMetadata.toString)
        //
      } yield (fileId,publisher,taskId,chunkMetadata,compressionStats)
      //  _____________________________________________________________________________________
      app.value.stopwatch.flatMap {result=>
        result.result match {
          case Left(e) =>  ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
          case Right(value) => value match{
            case (fileId,publisher,taskId,chunkMetadata,compressionStats) =>  for {
              timestamp     <-  IO.realTime.map(_.toMillis)
              _             <- acker.ack(envelope.deliveryTag)
              properties    = AmqpProperties(
                headers = Map("commandId"->StringVal("COMPRESS_COMPLETED")),
                messageId = taskId.some
              )
              messagePayload = payloads.CompressCompleted(
                fileId        = fileId,
                subTaskId     = chunkMetadata.chunkId,
                chunkMetadata = chunkMetadata,
                timestamp     = timestamp
              ).asJson.noSpaces
              message        = AmqpMessage[String](payload = messagePayload,properties = properties)
              _              <- publisher.publish(message)
              originalSize   = compressionStats.sizeIn
              compressedSize = compressionStats.sizeOut
              serviceTime    = result.duration.toMillis
//              COMMAND_ID, FILES_ID, TASK_ID, FILE_SIZE_IN, FILE_SIZE_OUT, SERVICE_TIME
              _ <- ctx.logger.info(s"COMPRESSION $fileId $taskId $originalSize $compressedSize $serviceTime")
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
// COMPRESS_COMPLETED
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
        timestamp      <- liftFF[Long,NodeError](IO.realTime.map(_.toMillis))
        currentState   <- maybeCurrentState
        taskId         <- maybeMessageId
        //      _____________________________________________________________________
        nodeId         = ctx.config.nodeId
        poolId         = ctx.config.poolId
        loadBalancerInfo = ctx.config.loadBalancer
        //        ___________________________________
        fileId         = payload.fileId
        //        userId         = payload.us
        chunkMetadata  = payload.chunkMetadata
        chunkSource    = chunkMetadata.location.source
        chunkUrl       = chunkMetadata.location.url
        _              <- L.info(s"COMPRESSION_LATENCY $fileId $taskId ${timestamp - payload.timestamp}")
        //     _______________________________________________________________________________________________
        task           <-  EitherT.fromEither[IO](currentState.pendingTasks.get(taskId).toRight{TaskNotFound(taskId)})
        compressTask   = task.asInstanceOf[CompressionTask]
        //      ________________________________________________________________
        userId         = compressTask.userId
        //      _______________________________
        updatedTask  = compressTask.copy(
          subTasks       = compressTask.subTasks.filter(_.id!= payload.subTaskId),
          chunksMetadata = compressTask.chunksMetadata :+ chunkMetadata
        )
        chunksMedata = updatedTask.chunksMetadata
        //       COMPRESSION_RATIO
        totalSizeIn  = (chunksMedata.map(_.sizeIn).sum).toDouble
        totalSizeOut = (chunksMedata.map(_.sizeOut).sum).toDouble
        compressionRatio =  totalSizeIn / totalSizeOut
        //
        _            <- L.debug(s"PENDING_SUBTASKS ${updatedTask.subTasks.length}")
        newState     <- liftFF[NodeState,NodeError]{
          ctx.state.updateAndGet(s=>s.copy(pendingTasks =  s.pendingTasks.updated(taskId,updatedTask) ))
        }
        newTask     <- EitherT.fromOption[IO].apply[NodeError,Task](newState.pendingTasks.get(taskId),TaskNotFound(taskId))
        newCompressTask = newTask.asInstanceOf[CompressionTask]
        //       _________________________________________

        sourceMetadata = FileMetadata.fromPath(Paths.get(chunkSource))
        oldChunkName   = sourceMetadata.filename.value
        oldChunkPath   = s"${ctx.config.sinkFolder}/$nodeId/$fileId/chunks/$oldChunkName"
        chunk          = new File(oldChunkPath)
        _              <- liftFF[Unit,NodeError](IO.delay{chunk.delete()} )
        _              <- L.debug(s"DELETE_CHUNK ${payload.subTaskId} $oldChunkPath")
        //      ________________________________________________________________________________
        _              <- L.debug(newTask.toString)
        _ <- if(newTask.subTasks.isEmpty) for{
          _ <- L.info(s"COMPRESSION_TASK_DONE $fileId $taskId $totalSizeIn $totalSizeOut $compressionRatio ${timestamp-newTask.startedAt}")
          _ <- L.debug(s"DELETE_ORIGINAL_FILE ${compressTask.filename}")
          //  ________________________________________________________________
          originalFile = new File(s"${ctx.config.sinkFolder}/$nodeId/$fileId/${compressTask.filename}")
          _     <- liftFF[Unit,NodeError](IO.delay{originalFile.delete()} )
          //  ________________________________________________________________
//          loadBalancerPubCfg = PublisherConfig(
//            exchangeName = ExchangeName(loadBalancerInfo.exchange),
//            routingKey = RoutingKey(loadBalancerInfo.routingKey)
//          )
          loadBalancerPub  = currentState.loadBalancerPublisher
          //  ________________________________________________________________
          properties = AmqpProperties(headers = Map("commandId"->StringVal("BALANCE")))
          msgPayload = payloads.Balance(
            fileId = fileId,
            userId=userId,
            chunks=newCompressTask.chunksMetadata,
            extension = compressTask.extension,
            timestamp=timestamp
          ).asJson.noSpaces
          message    = AmqpMessage[String](payload = msgPayload,properties = properties)
          _          <- liftFF[Unit,NodeError](loadBalancerPub.publish(message))
          //  ________________________________________________________________
        } yield ()
        else unit
      } yield (fileId,taskId)
      app.value.stopwatch.flatMap{ result=>
        result.result match {
          case Left(e) =>  acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
          case Right(value) =>  value match {
            case (fileId,taskId)=> for {
                _ <- acker.ack(envelope.deliveryTag)
                subTaskId = payload.subTaskId
                duration  = result.duration.toMillis
                _ <- ctx.logger.debug(s"COMPRESS_COMPLETED $fileId $taskId $subTaskId $duration")
              } yield ()
          }
        }
      }
    }

    processMessageV2[IO,payloads.CompressCompleted,NodeContext](
      successCallback =  successCallback,
      errorCallback =  e=> ctx.logger.error(e.getMessage)*>acker.reject(deliveryTag = envelope.deliveryTag)
    )
  }

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
//        _ <- L.debug(payload.toString)
        timestamp     <- liftFF[Long,NodeError](IO.realTime.map(_.toMillis))
        currentState  <- maybeCurrentState
//       ______________________________________________________
//        replyTo         <- maybeReplyTo
//        publisher       <- EitherT.fromOption[IO].apply[NodeError,PublisherV2](currentState.publishers.get(replyTo),PublisherNotFound())
        nodeId          = ctx.config.nodeId
        poolId          = ctx.config.poolId
        fileId          = payload.fileId
        _ <- L.info(s"MERGE_LATENCY $fileId ${timestamp - payload.timestamp}")
        chunksMetadata  = payload.chunkMetadata
        chunksLocations = chunksMetadata.map(_.location)
        sinkFolder      = s"${ctx.config.sinkFolder}/$nodeId/$fileId"
        destination     =  Paths.get(sinkFolder).resolve("chunks")
        _ <- L.debug(s"CHUNK_DESTINATION $destination")
//      DOWNLOAD CHUNKS
        _ <- liftFF[List[FileMetadata],E](
          Helpers.downloadUncompressedChunks(chunksMetadata,destination)
        )
        _ <- L.debug(s"DOWNLOAD_UNCOMPRESSED_CHUNKS $fileId")
//      __________________________________________________________
//        sourceFile   = new File(payload.sourcePath)
        filename     = Filename(payload.filename)
        extension    = Extension(payload.extension)
        result      <- fileX.mergeFile(
          filename,
          extension,
          new File(sinkFolder),
          chunkFolderName = "chunks")
          .leftMap[NodeError](x=>MergeError(x.getMessage))
        _ <- liftFF[Unit,E](
          IO.delay{
            destination.toFile.listFiles().map(_.delete())
          }.void
        )
      } yield (result)


      app.value.stopwatch.flatMap { result =>
        result.result match {
          case Left(e) =>  ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
          case Right(value) => value match {
            case (mergeResult) => for{
              currentState <- ctx.state.get
                _          <- acker.ack(envelope.deliveryTag)
                _          <- ctx.logger.info(s"MERGE ${payload.fileId} ${result.duration.toMillis}")
                ip         = currentState.ip
                port       = ctx.config.port
                properties = AmqpProperties(
                  headers = Map("commandId"->StringVal("MERGE_COMPLETED"))
                )
                msgPayload = payloads.MergeCompleted(url = s"http://$ip:$port/download").asJson.noSpaces
                message    = AmqpMessage[String](payload=msgPayload,properties = properties)
                cfg = PublisherConfig(exchangeName = ExchangeName("client"),routingKey = RoutingKey("client"))
                pub  = PublisherV2(cfg)
                _ <- pub.publish(message)
//                _  <- publisher.publish(message)
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
      val maybeReplyTo      = EitherT.fromEither[IO](envelope.properties.replyTo.toRight{NoReplyTo()})
      implicit val logger          = ctx.logger
      val L                        = Logger.eitherTLogger[IO,E]
      implicit val rabbitMQContext = ctx.rabbitContext
      val app = for {
//      GET TIMESTAMP
        timestamp           <- liftFF[Long,NodeError](IO.realTime.map(_.toMillis))
        //       CURRENT STATE
        currentState        <- maybeCurrentState
        //       NODE_ID
        nodeId              = ctx.config.nodeId
//        REPLY_TO
        replyTo             <- maybeReplyTo
        //       TASK_ID
        decompressTaskId    =  UUID.randomUUID().toString
//       COMPRESSECD CHUNKS METADATA
        compressedChunks    = payload.compressedChunks
        compressedSources   = compressedChunks.flatMap(_.sources)
        chunksMetadata = compressedSources
          .map(Paths.get(_))
          .map(FileMetadata.fromPath)
        //  FILE_ID
        fileId              = payload.fileId
        // SINK FOLDER
        sinkFolder          = s"${ctx.config.sinkFolder}/$nodeId/$fileId/compressed"
        sinkFolderPath      = Paths.get(sinkFolder)
        _                   = sinkFolderPath.toFile.mkdirs()
//       DOWNLOAD COMPRESSED CHUNKS
//        chunksMetadata      <- liftFF[List[FileMetadata],NodeError](
//          Helpers.downloadCompressedChunks(compressedChunks,sinkFolderPath)
//        )
        // BALANCE DECOMPRESSION SUB TASKS
        dataPreparationNodes = ctx.config.dataPreparationNodes
        nodeIds              = dataPreparationNodes.map(_.nodeId).filter(_!=nodeId)
        balancedNodes        = currentState.loadBalancer.balanceMulti(nodes = nodeIds,rounds = compressedSources.length,allowRep = true)
        pubs                 <- EitherT.fromEither[IO](balancedNodes.traverse(currentState.publishers.get).toRight{PublisherNotFound()})
        pubsStream           = Stream.emits(pubs).covary[IO]
//       SUB-TASKS
        subTasks             = (chunksMetadata zip balancedNodes)
          .map{
            case (chm,nId) =>
              DecompressionSubTask(id=chm.filename.value,taskId = decompressTaskId,nodeId = nId)
          }
//       FILENMAE
        filename       = Filename(payload.filename)
//       EXTENSION
        extension      = Extension(payload.extension)
//       DECOMPRESSION_TASK
        decompressTask = DecompressionTask(
          id         = decompressTaskId,
          subTasks   = subTasks,
          startedAt  = timestamp,
          filename   = filename,
          extension  = extension,
          replyTo    = replyTo
//          sourcePath = ""
        )

        newTask       = (decompressTaskId->  decompressTask)
        _             <- liftFF[NodeState,NodeError](ctx.state.updateAndGet(s=>s.copy(pendingTasks = s.pendingTasks+newTask)))
        _             <- liftFF[Unit,NodeError](
          rabbitMQContext.client.createChannel(conn = rabbitMQContext.connection).use{ implicit channel=>
            val data = pubsStream zip Stream.emits(compressedChunks)
//            val data = pubsStream zip Stream.emits(chunksMetadata)
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
                  publisher.publishWithChannel(message)*>ctx.logger.debug(s"SEND_COMPRESSED_CHUNK ${publisher.pubId} $decompressTaskId")
//                IO.unit
            }.compile.drain
          }
        )
      } yield (fileId)
//    _______________________________________________________-
      app.value.stopwatch.flatMap{ result =>
        result.result match {
          case Left(e) =>  acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
          case Right(fileId) =>for {
            _ <- ctx.logger.info(s"MERGE_DECOMPRESS $fileId ${result.duration.toMillis}")
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



  def slice()(implicit ctx:NodeContext, envelope:AmqpEnvelope[String], acker:Acker): IO[Unit] = {
    def successCallback(payload:payloads.Slice) = {
      type E                       = NodeError
      val unit                     = liftFF[Unit,NodeError](IO.unit)
      val maybeCurrentState        = EitherT.liftF[IO,E,NodeState](ctx.state.get)
      implicit val logger          = ctx.logger
      val L                        = Logger.eitherTLogger[IO,E]
      implicit val rabbitMQContext = ctx.rabbitContext
      val app = for {
        currentState   <- maybeCurrentState
        //       ______________________________________________________
        nodeId         = ctx.config.nodeId
        poolId         = ctx.config.poolId
        sinkVolume     = ctx.config.sinkFolder
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
        sourceCondition    = Either.cond[NodeError,Boolean](sourceFile.exists(),right = true,fileNotFound)
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






  def decompress()(implicit ctx:NodeContext,envelope:AmqpEnvelope[String],acker:Acker): IO[Unit] = {
    def successCallback(payload:payloads.Decompress) = {
      type E                = NodeError
      val maybeCurrentState = EitherT.liftF[IO,E,NodeState](ctx.state.get)
      val maybeReplyTo      = EitherT.fromEither[IO](envelope.properties.replyTo.toRight{NoReplyTo()})
      val maybeMessageId    = EitherT.fromEither[IO](envelope.properties.messageId.toRight{NoMessageId()})
      val unit              = liftFF[Unit,NodeError](IO.unit)
      implicit val logger: Logger[IO] = ctx.logger
      val L                 = Logger.eitherTLogger[IO,E]
      implicit val rabbitMQContext: v2.RabbitMQContext = ctx.rabbitContext

      val app = for {
//      CURRENT_STATE
        timestamp            <- liftFF[Long,E](IO.realTime.map(_.toMillis))
        currentState         <- maybeCurrentState
        nodeId               = ctx.config.nodeId
        fileId               = payload.fileId
        port                 = ctx.config.port
        ip                   = currentState.ip
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
        _ <- L.debug("DOWNLOAD_CHUNKS_COMPLETED")

        chunksNames = chunksMetadata.map(_.fullname)
        sources = chunksNames
          .map(sinkFolderPath.resolve)

        destinations = chunksMetadata.map(_.filename.value).map(_=>sinkUnCompressedPath)
        fullDestinations = chunksMetadata.map(_.fullname).map(x=>sinkUnCompressedPath.resolve(x))
        sourcesAndDestinations = sources zip destinations

        decompressionStats  <- sourcesAndDestinations.traverse {
            case (source, dest) =>
              println(source)
              println(dest)
              println(compressionAlgorithm)
              compression
              .decompress(ca = compressionAlgorithm,
              source = source.toString,
              destination = dest.toString
              )
              .leftMap(e=>CompressionError(e.getMessage))

          }
        url = s"http://$ip:$port/download"
        uncompressedChunksMetadata = decompressionStats.map{ ds =>
          val source = ds.fileOut.head
          val sourceMetadata = FileMetadata.fromPath(Paths.get(source))
          val chunkId        = sourceMetadata.filename.value
          val chunkIdSplited = chunkId.split("_")
          val location = Location(url= url,source = source)
          ChunkMetadata(
            chunkId = chunkId,
            index   = chunkIdSplited.lastOption.getOrElse("0").toInt,
            sizeOut = ds.sizeOut,
            sizeIn  = ds.sizeIn,
            compressionAlgorithm = payload.compressionAlgorithm,
            location = location
          )
        }
        _ <- L.debug(uncompressedChunksMetadata.toString)
//        _ <- EitherT.apply[IO,NodeError,List[DecompressionStats]](results)

        properties  = AmqpProperties(
          headers   = Map("commandId"->StringVal("DECOMPRESS_COMPLETED")),
          replyTo   = publisher.pubId.some,
//          replyTo   = deco,
          messageId = taskId.some
        )
        messagePayload = payloads.DecompressCompleted(
            fileId = fileId,
            timestamp = timestamp,
            chunksMetadata = uncompressedChunksMetadata
          ).asJson.noSpaces
//        )
        message = AmqpMessage[String](payload= messagePayload,properties = properties)
        //      SEND DECOMPRESS TO NODES
        _ <- liftFF[Unit,E](publisher.publish(message))
        _ <- liftFF[Unit,E](
          IO.delay{sinkFolderPath.toFile.listFiles().map(_.delete())}
        )
//        _ <- L.debug(results.mkString(","))
      } yield (fileId,publisher,taskId)

      app.value.stopwatch.flatMap {result=>
        result.result match {
          case Left(e) =>  ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
          case Right(value) =>  value match {
            case (fileId,publisher,taskId)=> for {
                _ <- acker.ack(envelope.deliveryTag)
                //          DELETE_COMPRESSED_FILE
                //          _________________________________________________________________________________________
                //            _ <- (IO.sleep(deleteDelay milliseconds)*>IO.delay{chunk.delete()} *> ctx.logger.info(s"DELETE_CHUNK ${chunkMetadata.filename} ${chunkMetadata.size.value.getOrElse(0)}")).start
                _ <- ctx.logger.info(s"DECOMPRESSION $fileId $taskId ${result.duration.toMillis}")
                //            _ <- ctx.logger.debug(s"REPLY_TO ${}")
              } yield ()
          }

        }
      }
    }

    processMessageV2[IO,payloads.Decompress,NodeContext](
      successCallback =  successCallback,
      errorCallback =  e=>ctx.logger.error(e.getMessage) *> acker.reject(deliveryTag = envelope.deliveryTag),

    )

  }


  def decompressCompleted()(implicit ctx:NodeContext,envelope: AmqpEnvelope[String],acker:Acker):IO[Unit] = {
    def successCallback(payload:payloads.DecompressCompleted) = {
      type E                       = NodeError
      val unit                     = liftFF[Unit,NodeError](IO.unit)
      val maybeCurrentState        = EitherT.liftF[IO,E,NodeState](ctx.state.get)
      val maybeMessageId           = EitherT.fromEither[IO](envelope.properties.messageId.toRight{NoMessageId()})
      val maybeReplyTo             = EitherT.fromEither[IO](envelope.properties.replyTo.toRight{NoReplyTo()})
      implicit val logger          = ctx.logger
      val L                        = Logger.eitherTLogger[IO,E]
      implicit val rabbitMQContext = ctx.rabbitContext
      val app = for {
//      TIMESTAMP
        timestamp    <- liftFF[Long,NodeError](IO.realTime.map(_.toMillis))
        fileId       = payload.fileId
        _            <- L.info(s"DECOMPRESS_LATENCY $fileId ${timestamp - payload.timestamp}")
//      CURRENT STATE
        currentState <- maybeCurrentState
//      REPLY_TO
        replyTo      <- maybeReplyTo
        publishers   = currentState.publishers
        publisher    <- EitherT.fromOption[IO].apply(publishers.get(replyTo),PublisherNotFound())
//      TASK_ID
        taskId       <- maybeMessageId
//
        _            <- L.debug(currentState.pendingTasks.mkString(","))
//
        task         <-  EitherT.fromEither[IO](currentState.pendingTasks.get(taskId).toRight{TaskNotFound(taskId)})
        decompressTask = task.asInstanceOf[DecompressionTask]
        filename = decompressTask.filename.value
        extension = decompressTask.extension.value
        chunksMetadataId = payload.chunksMetadata.map(_.chunkId)
        updatedSubtasks = decompressTask.subTasks.filter(x=> !chunksMetadataId.contains(x.id) )
//          .diff(payload.chunksMetadata.map(_.chunkId).toSet)
        updatedTask  = decompressTask.copy(subTasks = updatedSubtasks,chunksMetadata = decompressTask.chunksMetadata++payload.chunksMetadata)
//      NEW_STATE
        newState     <- liftFF[NodeState,NodeError]{
          ctx.state.updateAndGet(s=>s.copy(
            pendingTasks =  s.pendingTasks.updated(taskId,updatedTask) ,
          )
          )
        }
        newTask     <- EitherT.fromOption[IO].apply[NodeError,Task](newState.pendingTasks.get(taskId),TaskNotFound(taskId))
//        masterNodeId = decompressTask.mn
        mergeMsgProperties = AmqpProperties(
          headers = Map("commandId"->StringVal("MERGE")),
//          replyTo = replyTo.some
          replyTo = decompressTask.replyTo.some
        )
        _           <- L.debug(newTask.toString)
        _ <- if(newTask.subTasks.isEmpty)  for {
          _            <- L.debug(s"REPLY_TO $replyTo")
          _            <- L.info(s"DECOMPRESS_TASK_COMPLETED ${decompressTask.id} ${timestamp-decompressTask.startedAt}")
          mergePayload = payloads.Merge(
            filename =filename,
            extension = extension,
            chunkMetadata = updatedTask.chunksMetadata,
            timestamp = timestamp,
            fileId = payload.fileId
          ).asJson.noSpaces
          mergeMessage = AmqpMessage[String](payload=mergePayload,properties = mergeMsgProperties)
          _            <- liftFF[Unit,E](publisher.publish(mergeMessage))
        } yield ()
        else unit
      } yield (fileId,taskId,chunksMetadataId)

      app.value.stopwatch.flatMap{ result=>
        result.result match {
          case Left(e) =>  acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
          case Right(value) =>  value match {
            case (fileId,taskId,chunksMetadataId)=> for {
                _ <- acker.ack(envelope.deliveryTag)
                _ <- chunksMetadataId.traverse(subTaskId=>
                  ctx.logger.info(s"DECOMPRESS_COMPLETED $fileId $taskId $subTaskId ${result.duration.toMillis}")
                )
              } yield ()
          }
        }
      }
    }

    processMessageV2[IO,payloads.DecompressCompleted,NodeContext](
      successCallback =  successCallback,
      errorCallback =  e=> ctx.logger.error(e.getMessage)*>acker.reject(deliveryTag = envelope.deliveryTag)
    )
  }


  def deleteFile()(implicit ctx:NodeContext,envelope: AmqpEnvelope[String],acker:Acker)= {
    type E                       = NodeError
    val unit                     = liftFF[Unit,NodeError](IO.unit)
    val maybeCurrentState        = EitherT.liftF[IO,E,NodeState](ctx.state.get)
    implicit val logger          = ctx.logger
    val L                        = Logger.eitherTLogger[IO,E]
    implicit val rabbitMQContext = ctx.rabbitContext
    def successCallback(payload:payloads.DeleteFile) = {
      val app = for {
        _                 <- unit
        nodeId            = ctx.config.nodeId
        fileId            = payload.fileId
        baseFolder        = Paths.get(s"${ctx.config.sinkFolder}/$nodeId/$fileId")
        chunksFolder      = baseFolder.resolve("chunks")
        compressedFolder  = baseFolder.resolve("compressed")
        chunksFolderF     = chunksFolder.toFile
        compressedFolderF = compressedFolder.toFile
        totalFiles        = (chunksFolderF.listFiles().length + compressedFolderF.listFiles().length)
        source            = payload.source
        file              = new File(source)
        _                 <- liftFF[Unit,E](IO.delay{file.delete()  }.void)
        _                 <- if(totalFiles==0)  for {
          _ <- liftFF[Unit,E]( IO.delay{
              FileUtils.deleteDirectory(baseFolder.toFile)
            }.void
          )
          _ <- L.debug("DELETE_BASE_FOLDER_EMPTY")
        } yield ()
        else unit
//        listFiles = new
      } yield()

      app.value.stopwatch.flatMap{ res =>
        res.result match {
          case Left(e) => acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
          case Right(value) =>  for {
            _<- IO.unit
            fileId = payload.fileId
            source = payload.source
            _<- ctx.logger.info(s"DELETE_FILE $fileId $source")
          } yield ()

        }
      }
//      IO.delay{
//       val file =  new File(payload.source)
//        file.delete()
//      }
    }

    processMessageV2[IO,payloads.DeleteFile,NodeContext](
      successCallback =  successCallback,
      errorCallback =  e=> ctx.logger.error(e.getMessage)*>acker.reject(deliveryTag = envelope.deliveryTag)
    )
  }

}
