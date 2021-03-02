import org.apache.http.impl.client.HttpClients
import org.apache.http.client.config.{CookieSpecs, RequestConfig}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.client.methods.HttpGet
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import java.util.Properties
import java.nio.file.Paths
import java.nio.file.Files
import scala.io.Source
import scala.concurrent.Future
import scala.concurrent.blocking
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


object Twitter_Stream {

  var doneInput = ""

  def main(args: Array[String]): Unit = {
    //Start the streaming process
    streamTweets()
  }

  def streamTweets(): Unit = {
    println("Starting the Producer!")

    //Producer Properties
    val producerProps = new Properties()
    producerProps.put("bootstrap.servers", "localhost:9092")
    producerProps.put("acks", "all")
    producerProps.put("retries", 0)
    producerProps.put("linger.ms", 1)
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    //Producer client
    val producer = new KafkaProducer[String, String](producerProps)

    //HTTP client
    val httpClient = HttpClients.custom.setDefaultRequestConfig(
      RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build
    ).build

    //Build the uri
    val uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/sample/stream")
    val httpGet = new HttpGet(uriBuilder.build)
    val bearerToken = System.getenv("BEARER_TOKEN")

    httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken))

    //Execute the http
    val response = httpClient.execute(httpGet)
    val entity = response.getEntity()

    //Setting up the variables
    var lineNumber = 1
    var fileNumber = 1
    var tweetLine = ""

    //Entity is online
    if ( entity != null ) {

      //Tweet reader
      val reader = new BufferedReader(new InputStreamReader(entity.getContent()))
      var lineNumber = 1
      var line = reader.readLine()


      //While you can read in
      while (line != null) {

        //Producer sends the tweets onto topic
        producer.send(new ProducerRecord[String, String]("twitter-stream", s"${lineNumber}", line ) )
        lineNumber += 1
        line = reader.readLine()

      }
    }

    //close the producer
    producer.close()
  }









  def produceTweets(milliseconds: Long): Unit = {
    println("Starting the Producer!")

    val producerProps = new Properties()
    //producerProps.put("bootstrap.servers", "127.0.0.1:9092")
    producerProps.put("bootstrap.servers", "localhost:9092")
    producerProps.put("acks", "all")
    // producerProps.put("retries", 0)
    // producerProps.put("linger.ms", 1)
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    println("Start the producer ---------------------------------")

    val producer = new KafkaProducer[String, String](producerProps)

    println("Producer has been made ---------------------------------")

    var lineNumber = 1
    var fileNumber = 1
    var filePath = Paths.get(s"twitterstream/tweetstream-${milliseconds}-${fileNumber}.json") 

    println("Here ---------------------------------")

    //Producer: Send twitter data
    while( !doneCheck ){
      println("Here")
      filePath = Paths.get(s"twitterstream/tweetstream-1614578376670-1.json") 
      //filePath = Paths.get(s"twitterstream/tweetstream-${milliseconds}-${fileNumber}.json") 

      //Checks if there is a file to 
      if(Files.exists( filePath ) ){

        //tweetstream-1614578376670-1.json
        val source = Source.fromFile("twitterstream/tweetstream-1614578376670-1.json")

        //val source = Source.fromFile(s"twitterstream/tweetstream-${milliseconds}-${fileNumber}.json")
        lineNumber = 1

        for (line <- source.getLines()){
          producer.send(new ProducerRecord[String, String]("twitter-stream", s"${milliseconds}-${fileNumber}-${lineNumber}", line ) )
          lineNumber += 1
        }

        source.close()

        Files.deleteIfExists( filePath )

        println(s"File number: ${fileNumber} sent")
        fileNumber += 1

      } else{
        //Thread.sleep(100)
        println("Here")
      }

    }

    producer.close()
    println("Ending the Producer!")

  }


  def tweetStreamToDir(milliseconds: Long): Unit = {
    println("Starting the twitter stream!")


    val httpClient = HttpClients.custom.setDefaultRequestConfig(
      RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build
    ).build

    val uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/sample/stream")
    val httpGet = new HttpGet(uriBuilder.build)
    val bearerToken = System.getenv("BEARER_TOKEN")

    httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken))

    val response = httpClient.execute(httpGet)
    val entity = response.getEntity()

    //println(response)

    if ( entity != null ) {

      val reader = new BufferedReader(new InputStreamReader(entity.getContent()))
      var line = reader.readLine()

      var fileWriter = new PrintWriter("./tweetstream-temp.json")
      var lineNumber = 1
      var linesPerFile = 1000

      while (line != null) {
        if (lineNumber % linesPerFile == 0) {

          fileWriter.close()

          Files.move(
            Paths.get("tweetstream-temp.json"),
            Paths.get(s"twitterstream/tweetstream-${milliseconds}-${lineNumber/linesPerFile}.json")
          )
            
          fileWriter = new PrintWriter(Paths.get("tweetstream-temp.json").toFile())
          println(s"File written: ${lineNumber/linesPerFile}")

        }

        fileWriter.println(line)
        line = reader.readLine()
        lineNumber += 1

      }

    }

    println("Ending stream early!")
  }

  def doneCheck(): Boolean = {
    //blocking {
      if( doneInput == "quit"){
        true
      }

      false
    //}
  }
}