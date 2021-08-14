package mx.cinvestav

import cats.implicits._
import cats.data.EitherT
import cats.effect._
import ch.qos.logback.core.util.FileSize
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.StringVal
import dev.profunktor.fs2rabbit.model.{AmqpEnvelope, AmqpMessage, AmqpProperties}
import io.circe.generic.auto._
import mx.cinvestav.Declarations.{CompressionError, FileNotFound, NodeContext, NodeError, NodeState, PublisherNotFound, VolumeNotFound, payloads}
import mx.cinvestav.commons.fileX.{ChunkConfig, FileMetadata}
import mx.cinvestav.utils.v2.{Acker, processMessageV2}
import mx.cinvestav.commons.{fileX, liftFF}
import org.typelevel.log4cats.Logger
import fs2.Stream
import io.circe._
import io.circe.syntax._
import io.circe.generic.auto._
import mx.cinvestav.commons.compression

import concurrent.duration._
import language.postfixOps
import java.io.File
import java.util.UUID
import mx.cinvestav.commons.stopwatch.StopWatch._
import mx.cinvestav.utils.v2.encoders._

import java.nio.file.Paths

object CommandHandlers {



  def sliceAndCompress()(implicit ctx:NodeContext,envelope: AmqpEnvelope[String],acker: Acker): IO[Unit] = {

    def successCallback(payload:payloads.SliceAndCompress) = {
      type E                       = NodeError
      val unit                     = liftFF[Unit,NodeError](IO.unit)
      val maybeCurrentState        = EitherT.liftF[IO,E,NodeState](ctx.state.get)
      implicit val logger          = ctx.logger
      val L                        = Logger.eitherTLogger[IO,E]
      implicit val rabbitMQContext = ctx.rabbitContext
      val app = for {
        currentState        <- maybeCurrentState
        nodeId              = ctx.config.nodeId
        poolId              = ctx.config.poolId
        nodeIds             = ctx.config.dataPreparationNodes.map(_.nodeId).filter(_!=nodeId)
        _                   <- L.debug(currentState.loadBalancer.counter.toString)
        sinkVolume          <- EitherT.fromEither[IO](ctx.config.sinkVolumes.headOption.toRight{VolumeNotFound("")})
        sinkFile            = new File(sinkVolume)
        nodeVolume          = sinkFile.toPath.resolve(nodeId)
        fileId              = UUID.randomUUID()
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
        sliceStream   = fileX.splitFile(sourceFile,chunkConfig =chunkConfig,maxConcurrent = 6 )
        balancedNodes = currentState.loadBalancer.balanceMulti(nodes = nodeIds,rounds = totalNumberOfChunks,allowRep = true)
        pubs          <- EitherT.fromEither[IO](balancedNodes.traverse(currentState.publishers.get).toRight{PublisherNotFound()})
        pubsStream    = Stream.emits(pubs).covary[IO]
        _ <- liftFF[Unit,NodeError](
          sliceStream.zip(pubsStream).evalMap{
            case (chunkInfo, publisher) =>
              val props   = AmqpProperties(
                headers = Map("commandId" -> StringVal("COMPRESS")),
                replyTo = nodeId.some
              )
              val compressPayload = payloads.Compress(
                sourcePath = chunkInfo.sourcePath,
                compressionAlgorithm = payload.compressionAlgorithm
              ).asJson.noSpaces
              val message = AmqpMessage(payload = compressPayload,properties = props)
              for {
                _ <- ctx.logger.debug(s"COMPRESS ${chunkInfo.index} ${publisher.pubId} ${chunkInfo.sourcePath}")
                _ <- publisher.publish(message)
//                chunk = new File(chunkInfo.sourcePath)
//                deleteDelay = 1200
//                _ <- (IO.sleep(deleteDelay milliseconds)*>IO.delay{chunk.delete()} *> ctx.logger.info(s"DELETE_CHUNK ${chunkInfo.index} ${chunkInfo.metadata.size.value.getOrElse(0)}")).start
              } yield ()

//              publisher.publish(message) *>
//                ctx.logger.debug(s"COMPRESS ${chunkInfo.index} ${chunkInfo.sourcePath}")
        }.compile.drain
        )
//        publisher = currentState
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




  def compress()(implicit ctx:NodeContext,envelope:AmqpEnvelope[String],acker:Acker): IO[Unit] = {
    def successCallback(payload:payloads.Compress) = {
      type E                = NodeError
      val maybeCurrentState = EitherT.liftF[IO,E,NodeState](ctx.state.get)
      val unit                     = liftFF[Unit,NodeError](IO.unit)
      implicit val logger   = ctx.logger
      val L                 = Logger.eitherTLogger[IO,E]
      implicit val rabbitMQContext = ctx.rabbitContext

      val app = for {
        currentState         <- maybeCurrentState
        compressionAlgorithm = compression.fromString(payload.compressionAlgorithm)
        source               = payload.sourcePath
        sourcePath           = Paths.get(source)
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
      } yield result

      app.value.stopwatch.flatMap {result=>
        result.result match {
          case Left(e) =>  ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
          case Right(value) => for {
             _ <- acker.ack(envelope.deliveryTag)
             sourcePath    = Paths.get(payload.sourcePath)
             chunkMetadata = FileMetadata.fromPath(sourcePath)
             chunk         = sourcePath.toFile
             deleteDelay    = 1200
             _ <- (IO.sleep(deleteDelay milliseconds)*>IO.delay{chunk.delete()} *> ctx.logger.info(s"DELETE_CHUNK ${chunkMetadata.filename} ${chunkMetadata.size.value.getOrElse(0)}")).start
             _ <- ctx.logger.info(s"COMPRESSION_DATA ${result.duration}")
          } yield ()
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
      val unit                     = liftFF[Unit,NodeError](IO.unit)
      implicit val logger   = ctx.logger
      val L                 = Logger.eitherTLogger[IO,E]
      implicit val rabbitMQContext = ctx.rabbitContext

      val app = for {
        currentState         <- maybeCurrentState
        compressionAlgorithm = compression.fromString(payload.compressionAlgorithm)
        source               = payload.sourcePath
        sourcePath           = Paths.get(source)
        //       /data
        sourceNameCount      = sourcePath.getNameCount
        //       $FILE_ID
        chunkName            = sourcePath.subpath(sourceNameCount-1,sourceNameCount).toString
        //        /data/$FILE_ID/compressed
        destinationPath      = Paths.get("/").resolve(sourcePath.subpath(0,sourceNameCount-2))
          .resolve("decompressed")
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
      } yield result

      app.value.stopwatch.flatMap {result=>
        result.result match {
          case Left(e) =>  ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
          case Right(value) => for {
            _ <- acker.ack(envelope.deliveryTag)
//          DELETE_COMPRESSED_FILE
            sourcePath    = Paths.get(payload.sourcePath)
            chunkMetadata = FileMetadata.fromPath(sourcePath)
            chunk         = sourcePath.toFile
            deleteDelay    = 1200
            _ <- (IO.sleep(deleteDelay milliseconds)*>IO.delay{chunk.delete()} *> ctx.logger.info(s"DELETE_CHUNK ${chunkMetadata.filename} ${chunkMetadata.size.value.getOrElse(0)}")).start
            _ <- ctx.logger.info(s"DECOMPRESSION_DATA ${result.duration}")
          } yield ()
        }
      }
    }

    processMessageV2[IO,payloads.Decompress,NodeContext](
      successCallback =  successCallback,
      errorCallback =  e=>ctx.logger.error(e.getMessage) *> acker.reject(deliveryTag = envelope.deliveryTag),

    )

  }


}
