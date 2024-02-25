import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._

case class ClientsAddress2 (street: String, city: String, state: String, postal_code: String)
case class ClientsInfo2(client_id: String,
                       client_name: String,
                       contact_person: String,
                       email: String,
                       phone: String,
                       address: ClientsAddress2)

object JsonRDDExample {
  implicit val formats: DefaultFormats.type = DefaultFormats

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JsonRDDExample").setMaster("local")
    val sc = new SparkContext(conf)

    val json_rdd = sc.textFile("src/main/scala/fake_client_data.json")

    val client_data_rdd = json_rdd.map { line =>
      val json = parse(line)
      json.extract[ClientsInfo2]
    }

    // Printing first row
    val first_client_data = client_data_rdd.first()
    println("\nFirst Clients Information:")
    println(first_client_data)

    // Filtering clients by state (keeping only clients from California)
    val clients_in_la = client_data_rdd.filter(_.address.state == "LA")
    println("\nClients in Los Angeles:")
    clients_in_la.foreach(println)

    // Mapping clients to their phone numbers
    val client_phone_numbers = client_data_rdd.map(client => (client.client_name, client.phone))
    println("\nClient Phone Numbers:")
    client_phone_numbers.foreach(println)

    sc.stop()
  }
}