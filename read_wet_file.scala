//import testclass.TestClass
import java.io.File;
import java.io.BufferedInputStream
import java.io.FileInputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.zip.GZIPInputStream

object Counter {
  var c: Int = 0
  var characters: Int = 0
  var records: Int = 0
  var inRecord: Boolean = false
  var recordLength: Int = 0
  def increment(a: Int): Unit = {
    c += a
  }
  def increment(a: String): Unit = {
    c += 1
    characters += (a.size + 1) // add the newlines back in
    if (a.startsWith("WARC-Type: conversion")) {
      records += 1
      inRecord = true
    }
    if (a.startsWith("Content-Length: ")) {
      if (!inRecord) {
        throw new RuntimeException("Record Format Error!!")
      }
      recordLength = a.split(": ").last.toInt
    }
  }
  def increment(a: Unit): Unit = {
    c += 1
  }
  def getCounter(): Int = {
    return c
  }
  def getCharacterCount(): Int = {
    return characters
  }
  def getRecordsCount(): Int = {
    return records
  }
}

object Parser {
  val linebuffer = scala.collection.mutable.ArrayBuffer.empty[Byte]
  val recordbuffer = scala.collection.mutable.ArrayBuffer.empty[Byte]
  val newlinebyte = "\n".getBytes("UTF-8").last
  var linecount: Int = 0
  var recordcount: Int = 0
  var recordlength: Int = 0
  var contentbytesread: Int = 0
  var bufferbytesread: Int = 0
  var printnextline: Boolean = false

  def parseBuffer_old(buffer: Array[Byte], length: Int): Unit = {
    //println("Called")
    bufferbytesread = 0
    // Short Cut, if we are in content, and we still have more to 
    // read then is in the buffer we just received, just copy the
    // entire buffer to the linebuffer
    if (recordlength != 0 && (contentbytesread + length < recordlength)) {
      //println("Copied buffer to recordbuffer: copied "+length+" bytes")
      recordbuffer.appendAll(buffer.slice(0,length))
      contentbytesread += length
    } else {
      // the end of the record is somewhere in the buffer, lets find it
      // rewrite this to use a slice, don't need to iterate, we already
      // know where in the buffer it ends, no need to search.
      while(bufferbytesread < length) {
        var b = buffer(bufferbytesread)
        bufferbytesread += 1
        if (recordlength != 0) {
          // we are in content
          if (contentbytesread < recordlength) {
            recordbuffer.append(b)
            contentbytesread += 1
          } else {
            // found the end of the record
            processRecord()
            recordbuffer.clear()
            printnextline = true
            contentbytesread = 0
            recordlength = 0
          }
        } else {
          // not in content, so we are parsing WET record headers
          if (b == newlinebyte) {
            linecount += 1
            checkLine()
            linebuffer.clear()
          } else {
            linebuffer.append(b)
          }
        }
      }
    }
  }

  /* This method searchs the read buffer sequentially for complete lines of
   * text, when it finds one it calls checkLine(). If checkLine() sets the 
   * object variable recordlength then it will call parseWetPayload to 
   * process the payload.
   */
  def searchForCompleteLine(buffer: Array[Byte], length: Int): Unit = {
    var b = buffer(bufferbytesread)
    bufferbytesread += 1
    if (b == newlinebyte) {
      linecount += 1
      checkLine() // this will set recordlength if the WET Header indicates
                  // that the next line starts a record (technically:
                  // line.startsWith("Content-Length: ")
      linebuffer.clear()
    } else {
      linebuffer.append(b)
    }

    // If we found a WET record to extract then switch to that activity,
    // otherwise get the next line
    if (recordlength != 0) {
      // found one
      parseWetPayload(buffer, length)
    } else if (bufferbytesread < length) {
      // nope .. move to the next line
      // tail recursion .. wheee
      searchForCompleteLine(buffer, length)
    }
    // we only get here if we have hit the end of the buffer
  }

  /* This method extracts the WET payload, whose length is specified in the
   * Content-length header in actual octets (not UTF-8 bytes). If the payload is 
   * only part of the buffer then this method will call searchForCompleteLine so
   * that line by line processing of the WET file can continue.
   */
  def parseWetPayload(buffer: Array[Byte], length: Int): Unit = {
    if (recordlength != 0 && (length - bufferbytesread <= recordlength - contentbytesread)) {
      // if what is left in the buffer <= what is left in the record
      // then grab the whole buffer
      //println("Copied buffer to recordbuffer: copied "+length+" bytes")
      recordbuffer.appendAll(buffer.slice(bufferbytesread,length))
      contentbytesread += length - bufferbytesread
      // the buffer is empty, this should return to parseBuffer would will then return to the call
      // usually for them to fill to buffer again
    } else if (recordlength != 0 && (length - bufferbytesread > recordlength - contentbytesread)) {
      // if what is left in the buffer > what is left in the record
      // the grab what is left in the record and pass the rest to the searchForCompleteLine hunter
      //println("ContentBytesread "+contentbytesread+" length "+length+" recordlength "+recordlength+" bufferBytesRead "+bufferbytesread)
      var recordendinbuffer = bufferbytesread + (recordlength - contentbytesread) // this is where the payload end in the buffer
      //println("recordendinbuffer: "+recordendinbuffer)

      recordbuffer.appendAll(buffer.slice(bufferbytesread, recordendinbuffer))
      processRecord()
      recordbuffer.clear()
      printnextline = true
      bufferbytesread = recordendinbuffer
      contentbytesread = 0 // reset
      recordlength = 0

      // The record is done, if there is anything left in the buffer 
      // go back to parsing WET record headers
      if (bufferbytesread < length) {
        //println("BufferBytesread = "+bufferbytesread+" Length is: "+length)
        searchForCompleteLine(buffer, length)
      }
    }
  }

  /*
   * This is the method called by the program populating the a read buffer.
   */
  def parseBuffer(buffer: Array[Byte], length: Int): Unit = {
    //println("Called")
    bufferbytesread = 0
    // pretty simple
    // the parseWetPayload and searchForCompleteLine methods will hand 
    // off between themselves to properly parse the buffer. This method
    // only needs to make sure to start with the right method
    if (recordlength != 0) {
      // we are in the middle of a WET payload section
      // call parseWetPayload
      parseWetPayload(buffer, length)
    } else {
      // we are still hunting headers, so look for lines by
      // calling searchForCompleteLine
      searchForCompleteLine(buffer, length)
    }
  }

  def processRecord(): Unit = {
    // need to implement this
    //println("Start Content")
    //println(new String(recordbuffer.toArray, "UTF-8"))
    //println("End Content")
  }

  /* When we have extracted a complete line out of the read buffer
   * this method is called to determine the proper action to take based 
   * on the content of the line.
   */
  def checkLine(): Unit = {
    var line = new String(linebuffer.toArray, "UTF-8")
    if (printnextline) {
      printnextline = false
      //println("Line after content is: "+line)
    }
    if (line.startsWith("WARC-Type: conversion")) {
      recordcount += 1
    } else if (line.startsWith("Content-Length: ")) {
      //println("Found content length line: "+line)
      recordlength = line.split(": ").last.trim.toInt
      recordlength += 2 // content always starts after next blank line \r\n
    }
  }

  def getLineCount(): Int = {
    return linecount
  }
  def getRecordCount(): Int = {
    return recordcount
  }
}

object Wrapper {
  def main(args: Array[String]): Unit = {
    //val html = scala.io.Source.fromURL("https://spark.apache.org/").mkString
    //val gzipFile = new File("./CC-MAIN-20160924173739-00000-ip-10-143-35-109.ec2.internal.warc.wet.gz")
    val gzipFile = new File("./CC-MAIN-20161202170900-00000-ip-10-31-129-80.ec2.internal.warc.wet.gz")
    val gzInputStream = new GZIPInputStream(new BufferedInputStream(new FileInputStream(gzipFile)))
    val buffer = new Array[ Byte ]( 1024 )
    Stream.continually(gzInputStream.read(buffer)).takeWhile(_ != -1).foreach(Parser.parseBuffer(buffer, _))
    println("Saw "+Parser.getLineCount()+" lines")
    println("Saw "+Parser.getRecordCount()+" records")
  }
}
