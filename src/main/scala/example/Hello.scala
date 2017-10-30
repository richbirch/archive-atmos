package example

import java.io._
import java.nio.file.{Files, Paths}

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.{AmazonS3Client, S3ClientOptions}
import com.amazonaws.util.IOUtils

import scala.collection.JavaConversions._
import scala.io.Source

object Hello extends App {

  val atmosClient = newAtmosClient
  val s3Client = newS3Client
  val atmosBucket = "drtdqprod"
  val s3Bucket = "drt-prod-s3"
  val buckets = List(atmosBucket, atmosBucket + "archived")

  val localPath = "/home/rich/dev/atmos-backup"

  buckets.map(bucketName => {
    val markersLogFileName = s"$localPath/markers-$bucketName.log"
    val markerRecordFile = new FileWriter(markersLogFileName, true)
    val markers = Source.fromFile(markersLogFileName).getLines.toList
    println(s"$markersLogFileName: $markers")
    listFilesInBucket(atmosClient, bucketName, "", markerRecordFile)
  })

  def listFilesInBucket(client: AmazonS3Client, bucketName: String, nextContinuationToken: String = null, markersRecordFile: FileWriter): Set[String] = {
    println(s"Requesting files from marker '$nextContinuationToken'")
    if (nextContinuationToken != null) {
      markersRecordFile.write(s"$nextContinuationToken\n")
      markersRecordFile.flush()
    }

    val listRequest = new ListObjectsRequest(bucketName, "", nextContinuationToken, "", 1000)
    val listing = client.listObjects(listRequest)
    val nct = listing.getNextMarker

    listing.getObjectSummaries.map(o => {
      val fileName = o.getKey
      if (!Files.exists(Paths.get(s"$localPath/$fileName")))
        saveFile(newAtmosClient, bucketName, fileName, localPath)
      else
        println(s"skipping previously downloaded file $localPath/$fileName")
      o.getKey
    }).toSet ++ (if (nct != null) listFilesInBucket(atmosClient, bucketName, nct, markersRecordFile) else Set())
  }

  def saveFile(client: AmazonS3Client, bucketName: String, s3FileName: String, localPath: String): Unit = {
    val fullPath = s"$localPath/$s3FileName"
    val outFile = new BufferedOutputStream(new FileOutputStream(fullPath))
    val bytes = IOUtils.toByteArray(client.getObject(bucketName, s3FileName).getObjectContent)
    outFile.write(bytes)
    println(s"Written $fullPath")
    outFile.close
  }

  def uploadFile(client: AmazonS3Client, bucketName: String, s3FileName: String, localPath: String): Unit = {
    val fullPath = s"$localPath/$s3FileName"
    println(s"Uploading $fullPath to $bucketName")
    val metadata = new ObjectMetadata
    metadata.setSSEAlgorithm("aws:kms")
    val putRequest = new PutObjectRequest(bucketName, s3FileName, new FileInputStream(new File(fullPath)), metadata)
    client.putObject(putRequest)
    println(s"Uploaded $fullPath")
  }

  def newAtmosClient = {
    val configuration: ClientConfiguration = new ClientConfiguration()
    configuration.setSignerOverride("S3SignerType")
    val creds = new ProfileCredentialsProvider("drt-atmos")
    val client = new AmazonS3Client(creds, configuration)
    client.setS3ClientOptions(S3ClientOptions.builder().setPathStyleAccess(true).build)
    client.setEndpoint("cas00003.skyscapecloud.com:8443")
    client
  }

  def newS3Client = {
    val creds = new ProfileCredentialsProvider("drt-prod-s3")
    new AmazonS3Client(creds)
  }
}