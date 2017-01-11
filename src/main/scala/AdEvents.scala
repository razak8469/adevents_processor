package adevents
import spray.json.DefaultJsonProtocol._

object AdEvents {
  case class AdServe(name: String,
                     timestamp: Long,
                     sessionId: String,
                     adId: String,
                     publisherId: String)

  case class VideoView(name: String,
                       timestamp: Long,
                       sessionId: String,
                       videoId: String,
                       from: Float,
                       to: Float)

  case class UrlOpen(name: String,
                     timestamp: Long,
                     sessionId: String,
                     url: String)

  implicit val adserveFormat = jsonFormat5(AdServe)
  implicit val videoviewFormat = jsonFormat6(VideoView)
  implicit val urlopenFormat = jsonFormat4(UrlOpen)
}
