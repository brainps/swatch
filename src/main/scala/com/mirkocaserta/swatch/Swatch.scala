package com.mirkocaserta.swatch

import java.nio.file.StandardWatchEventKinds.{ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY}

import akka.actor.ActorRef

import concurrent.{Future, future}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.WatchEvent.Kind

import com.sun.nio.file.SensitivityWatchEventModifier

import language.implicitConversions
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import util.{Failure, Success, Try}

/**
 * A wrapper for a Java 7 [[java.nio.file.WatchService]].
 */
object Swatch {

  val log = LoggerFactory.getLogger(getClass)

  type Listener = (SwatchEvent) ⇒ Unit

  sealed trait EventType

  case object Create extends EventType

  case object Modify extends EventType

  case object Delete extends EventType

  case object Overflow extends EventType

  sealed trait SwatchEvent {
    def path: Path
  }

  case class Create(path: Path) extends SwatchEvent

  case class Modify(path: Path) extends SwatchEvent

  case class Delete(path: Path) extends SwatchEvent

  private[this] implicit def eventType2Kind(et: EventType) = {
    import java.nio.file.StandardWatchEventKinds._

    et match {
      case Create ⇒ ENTRY_CREATE
      case Modify ⇒ ENTRY_MODIFY
      case Delete ⇒ ENTRY_DELETE
      case Overflow ⇒ OVERFLOW
    }
  }

  private[this] implicit def eventType2KindFlat(et: EventType): WatchEvent.Kind[Any] = {
    // Force it
    eventType2Kind(et).asInstanceOf[WatchEvent.Kind[Any]]
  }



  private[this] implicit def kind2EventType(kind: Kind[Path]) = {
    import java.nio.file.StandardWatchEventKinds._

    kind match {
      case ENTRY_CREATE ⇒ Create
      case ENTRY_MODIFY ⇒ Modify
      case ENTRY_DELETE ⇒ Delete
      case _ ⇒ Overflow
    }
  }

  implicit def string2path(path: String): Path = Paths.get(path)

  /**
   * Message class for the SwatchActor.
   *
   * @param path the path to watch
   * @param eventTypes event types to watch for
   * @param recurse should subdirs be watched too?
   * @param listener an optional [[akka.actor.ActorRef]]
   *                 where notifications will get sent to;
   *                 if unspecified, the [[akka.actor.Actor#sender]]
   *                 ref will be used
   */
  case class Watch(path: Path, eventTypes: Seq[EventType], recurse: Boolean = false, listener: Option[ActorRef] = None)



  val watchServices : mutable.Queue[WatchService] = mutable.Queue()

  /**
   * Watch the given path by using a Java 7
   * [[java.nio.file.WatchService]].
   *
   * @param path the path to watch
   * @param eventTypes event types to watch for
   * @param listener events will be sent here
   * @param recurse should subdirs be watched too?
   */
  def watch(path: Path,
            eventTypes: Seq[EventType],
            listener: Listener,
            recurse: Boolean = false) {
    log.debug(s"watch(): entering; path='$path', eventTypes='$eventTypes', listener='$listener', recurse=$recurse")
    val watchService = FileSystems.getDefault.newWatchService
    log.debug(s"watch(): enqueued watchService: ${watchService}  to size : ${watchServices.size+1}")
    watchServices.enqueue(watchService)


    if (recurse) {
      Files.walkFileTree(path, new SimpleFileVisitor[Path] {
        override def preVisitDirectory(path: Path, attrs: BasicFileAttributes) = {
          watch(path, eventTypes, listener)
          FileVisitResult.CONTINUE
        }
      })

    } else {
      // Original Swatch code
      // path.register(watchService, eventTypes map eventType2Kind: _*)

      // MacOSX performance improvement
      val javaEventTypes : Array[Kind[_]] = Array(eventTypes map eventType2KindFlat : _*)
      val watchKey = path.register(watchService, javaEventTypes, SensitivityWatchEventModifier.HIGH)

    }

    import concurrent.ExecutionContext.Implicits.global

    Future {
      import collection.JavaConversions._
      var loop = true

      while (loop) {
        log.debug(s"watch(): looping future; path='$path', eventTypes='$eventTypes', listener='$listener', recurse=$recurse")
        Try(watchService.take) match {
          case Success(key) ⇒
            key.pollEvents map {
              event ⇒
                import java.nio.file.StandardWatchEventKinds.OVERFLOW

                event.kind match {
                  case OVERFLOW ⇒ // weeee
                  case _ ⇒
                    val ev = event.asInstanceOf[WatchEvent[Path]]
                    val tpe = kind2EventType(ev.kind)
                    val notification = tpe match {
                      case Create ⇒ Create(path.resolve(ev.context))
                      case Modify ⇒ Modify(path.resolve(ev.context))
                      case Delete ⇒ Delete(path.resolve(ev.context))
                    }
                    log.debug(s"watch(): notifying listener; notification=$notification")
                    listener(notification)
                    if (!key.reset) {
                      log.debug("watch(): reset unsuccessful, exiting the loop")
                      loop = false
                    }
                }
            }
          case Failure(e) ⇒
            // Behavior Change - Exit on Failure  to complete Future ignore failure, just as IRL
            if ( e.isInstanceOf[ClosedWatchServiceException] ) {
              log.debug(s"watch(): ClosedWatchServiceException, stopping loop; e='${e.getClass.getName}', path='$path', eventTypes='$eventTypes', listener='$listener', recurse=$recurse")
              loop = false
            } else {
              log.error(s"watch(): Unexpected: ${e.getClass.getName}, stopping loop; e='${e.getClass.getName}', path='$path', eventTypes='$eventTypes', listener='$listener', recurse=$recurse")
            }

        }
      }

      log.debug(s"watch(): completed future; path='$path', eventTypes='$eventTypes', listener='$listener', recurse=$recurse")

    }
    log.debug(s"watch(): completed watch; path='$path', eventTypes='$eventTypes', listener='$listener', recurse=$recurse")
  }


  def closeAll(): Unit = {
    log.debug(s"watch(): closing all watchServices of size ${watchServices.size}")

    watchServices.dequeueAll { watchService =>
      log.debug(s"watch(): closing watchService: ${watchService}")
      watchService.close()
      true
    }

    log.debug(s"watch(): closedAll watchServices down to size ${watchServices.size}")

  }
}

