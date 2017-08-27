package com.test.spark.xmlparser

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.{ SparkConf, SparkContext }

import com.databricks.spark.csv
import org.apache.commons.csv
import com.databricks.spark.xml.XmlReader
import com.databricks.spark.xml
import net.liftweb.json.Xml.toJson
import net.liftweb.json.JsonAST._
import org.apache.hadoop.yarn.webapp.ToJSON
import org.apache.spark.sql.functions.{ udf, explode }
import scala.io

object XMLParser {

	def main( args : Array[ String ] ) : Unit = {

		try {
			val sparkConf = new SparkConf().setAppName( "XMLParser" ).setMaster( "local[4]" );

			val sparkContext = new SparkContext( sparkConf );

			val sqlContext = new SQLContext( sparkContext )

			val pat = """<\?xml version(.*)</magnumlog>""".r

			val file = sparkContext.textFile( "file:\\C:\\test1\\POS_APPLICATION_MAGNUM.csv", 4 )

			val splitFile = file.map( x => pat.findAllIn( x ).toList )

			val xmlRDD = splitFile.filter { x => x.length > 0 }.map { x => x( 0 ) }

			val df = new XmlReader().xmlRdd( sqlContext, xmlRDD )

			df.printSchema()
			df.select( df( "case.agencycode" ) ).show()

		} catch {
			case e : Exception =>
				println( "Issue in XML parsing" )
				e.printStackTrace()
				System.exit( 1 )
		}
		println( "Successfully parsed XML file" )
		System.exit( 0 )

		//df.show()

	}

}