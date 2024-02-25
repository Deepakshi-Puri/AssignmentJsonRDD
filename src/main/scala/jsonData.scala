import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.{DefaultFormats, JValue}
import org.json4s.jackson.JsonMethods

case class ClientsAddress (street: String, city: String, state: String, postal_code: String)
case class ClientsInfo(client_id: String,
                       client_name: String,
                       contact_person: String,
                       email: String,
                       phone: String,
                       address: ClientsAddress)

object jsonData {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel (Level.ERROR)
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("json Data")
    val spark = new SparkContext(conf)

    val jsonRDD = spark.textFile("src/main/scala/fake_client_data.json")

    val jValueRDD: RDD [JValue] = jsonRDD.map(JsonMethods.parse(_))

    val clientsInfoRDD: RDD[ClientsInfo] = jValueRDD.map(jValue => {
      implicit val formats: DefaultFormats.type = DefaultFormats

      val client_id = (jValue \ "client_id").extract[String]
      val client_name = (jValue \ "client_name").extract[String]

      val contact_person = (jValue \ "contact_person").extract[String]
      val email = (jValue \ "email").extract[String]
      val phone = (jValue \ "phone").extract[String]

      val addressJValue = jValue \ "address"
      val street = (addressJValue \ "street").extract[String]
      val city = (addressJValue \ "city").extract[String]
      val state = (addressJValue \ "state").extract[String]
      val postal_code = (addressJValue \ "postal_code").extract[String]

      val address = ClientsAddress(street, city, state, postal_code)

      ClientsInfo(client_id, client_name, contact_person, email, phone, address)
    })

    // All Clients Information
    println("\nAll Clients:")
    clientsInfoRDD.foreach(println)

    // Filtering by State
    val clientsInCalifornia = clientsInfoRDD.filter(_.address.state == "LA")
    println("\nClients in Los Angeles:")
    clientsInCalifornia.foreach(println)

    // Mapping Client Names
    val clientNamesRDD = clientsInfoRDD.map(_.client_name)
    println("\nClient Names:")
    clientNamesRDD.foreach(println)
  }
}
