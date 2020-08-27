import scala.language.postfixOps
import sys.process._
import java.net.URL
import java.io.File
import java.time.YearMonth
import wvlet.log.LogSupport

object Download_events extends LogSupport {

  case class DownloadException (errorMsg : String) extends Exception (errorMsg)

  def main(args: Array[String]): Unit = {

    val year = 2018
    val month = 2
    val yearMonthObject = YearMonth.of(year, month)
    val daysInMonth = yearMonthObject.lengthOfMonth
    val path = "https://data.gharchive.org/" + year.toString + "-" + "%02d".format(month)


    def downloadFile(downloadableFileLink: String, path: String): Unit = {
        val urlObject = new URL(downloadableFileLink)
        val filePath = path + "/" + urlObject.getPath().replaceAll("/", "")
        urlObject #> new File(filePath) !!
    }

    for (d <- 1 to daysInMonth)
    {
      val path1 = path + "-" + "%02d".format(d)
      val path_d = "./files/" + year.toString + "-" + "%02d".format(month) +  "-" + "%02d".format(d)
      val directory = new File(path_d)
      if (!directory.exists) {
        directory.mkdir
      }
      for (h <- 0 to 23)
        {
          val path2 = path1 + "-" + h.toString +".json.gz"
          try {
            downloadFile(path2, path_d)
          }
          catch {
            case DownloadException("") => println("error during downloading")
          }
        }
      info(" data downloaded for  "+ year.toString + "-" + "%02d".format(month) +  "-" + "%02d".format(d))
    }
  }
}

