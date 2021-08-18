package mx.cinvestav

import cats.implicits._
import cats.effect._
import mx.cinvestav.Declarations.payloads.CompressedChunkLocation
import mx.cinvestav.commons.fileX.FileMetadata
import mx.cinvestav.server.Client.{downloadFile, downloadFileE}

import java.nio.file.{Path, Paths}

object Helpers {

  def downloadCompressedChunks(
                                compressedChunkLocation: List[CompressedChunkLocation],
                                destination:Path
                              ) = {
    compressedChunkLocation.traverse{ chunksLocation=>
      val url     = chunksLocation.url
      val sources = chunksLocation.sources
      sources.traverse{ source =>
        val metadata = FileMetadata.fromPath(Paths.get(source))
        val chunkName = metadata.fullname
        val chunkDestination = destination.resolve(chunkName)
        downloadFile(url,source,destination= chunkDestination)
          .handleError{ e=>
            IO.println(s"ERROR: ${e.getMessage}")
          }.map(_=>metadata)
      }
//        .flatMap(_=>IO.unit)
    }.map(_.flatten)
//      .flatMap(_=>IO.unit)
//      .flatMap(_.flatten)

  }


}
