package com.jeffharwell.commoncrawl.createcorpus

import java.io.File;
import java.io.BufferedInputStream
import java.io.InputStream
import java.io.FileInputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.zip.GZIPInputStream
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.List


class Parser(inputstream: InputStream) extends Iterator[List[String]] {
  val linebuffer = scala.collection.mutable.ArrayBuffer.empty[Byte]
  val recordbuffer = scala.collection.mutable.ArrayBuffer.empty[Byte]
  val newlinebyte = "\n".getBytes("UTF-8").last
  var readbuffersize: Int = 1024
  var linecount: Int = 0
  var recordcount: Int = 0
  var recordlength: Int = 0
  var contentbytesread: Int = 0
  var bufferbytesread: Int = 0
  var printnextline: Boolean = false
  var wetrecords: ListBuffer[String] = new ListBuffer()
  var gzcInputStream: GZIPCompatibilityWrapper = new GZIPCompatibilityWrapper(inputstream)
  var gzInputStream: GZIPInputStream = new GZIPInputStream(gzcInputStream)
  var hasnext: Boolean = true

  /* Here is a wrapper class that wraps an InputStream
   * but always returns > 0 when .available() is called.
   * This will cause GZIPInputStream to always make another 
   * call to the InputStream to check for an additional 
   * concatenated GZIP file in the stream.
   */
  class GZIPCompatibilityWrapper(inputstream: InputStream) extends InputStream {
      val is: InputStream = inputstream

      override def read(): Int = {
          return(is.read());
      }

      override def read(b: Array[Byte]): Int = {
          return(is.read(b));
      }

      override def read(b: Array[Byte], off: Int, len: Int): Int = {
          return(is.read(b, off, len));
      }

      override def close(): Unit = {
          is.close();
      }

      override def available(): Int = {
          // Always say that we have 1 more byte in the
          // buffer, even when we don't
          val a: Int = is.available();
          if (a == 0) {
              return(1);
          } else {
              return(a);
          }
      }
  }

  /*
   * This is the next() implementation for our Iterator
   *
   * @return A list of individual WET records
   */
  def next(): List[String] = {
    var result = false
    do {
      result = this.parseBuffer
    } while (!result)

    return this.returnRecord() // will return an empty list if we hit end of stream
  }

  /*
   * This is the hasNext() implementation for the iterator. The variable hasnext
   * is set to false by the parseBuffer() method when we reach the end of the InputStream
   *
   * Note that this isn't entirely accurate. Because we are reading an InputStream on the fly
   * we actually don't know if there is an entire record left or not, we won't know until we 
   * try next() and actually hit the end of the stream. So. next() may return an empty list
   * after .hasNext() has indicated that there is actually another record present.
   *
   * I suppose we could get around this by reading ahead one record ... hmm
   *
   * @return boolean indicating if there is another record in the iterator
   */
  def hasNext(): Boolean = {
    return hasnext
  }
      

  /*
   * This is the method called by the program populating the a read buffer.
   * The read buffer will contain some small chunk of a WET file, the job of this
   * class is to take a sequence of those buffers and return a set of distinct records
   * from the WET file.
   *
   * @return true if we have found a complete record or reach end of stream, false otherwise
   */

  def parseBuffer(): Boolean = {
    val buffer = new Array[ Byte ]( readbuffersize )
    var length: Int = 0

    length = gzInputStream.read(buffer)
    if (length == -1) {
      // We are done
      hasnext = false
      return true
    }

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

    // At this point we are done looking at the buffer, so go back and check
    // the object data and see if we have any complete records
    if (wetrecords.size > 0) {
      // we have at least one complete record
      return true
    }
    // no complete records
    return false
  }

  /*
   * This method returns any complete WET records that we have thus far. If we don't have
   * anything it will return an empty List
   *
   * @return List[String] where each string is a complete WET record
   */
  def returnRecord(): List[String] = {
    // convert our buffer to a list
    val toreturn: List[String] = wetrecords.toList
    // clear our record buffer
    wetrecords.clear
    // return our list
    return toreturn
  }

  /* This method searchs the read buffer sequentially for complete lines of
   * text, when it finds one it calls checkLine(). If checkLine() sets the 
   * object variable recordlength then it will call parseWetPayload to 
   * process the payload.
   *
   * @param buffer the byte array to with bytes to process
   * @param length the number of bytes to read from the buffer
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
   *
   * @param buffer the byte array to with bytes to process
   * @param length the number of bytes to read from the buffer
   */
  def parseWetPayload(buffer: Array[Byte], length: Int): Unit = {
    if (recordlength != 0 && (length - bufferbytesread < recordlength - contentbytesread)) {
      // if what is left in the buffer < what is left in the record
      // then grab the whole buffer
      //println("Copied buffer to recordbuffer: copied "+length+" bytes")
      recordbuffer.appendAll(buffer.slice(bufferbytesread,length))
      contentbytesread += length - bufferbytesread
      // the buffer is empty, this should return to parseBuffer which will then return to the call
      // usually for them to fill to buffer again
    } else if (recordlength != 0 && (length - bufferbytesread >= recordlength - contentbytesread)) {
      // if what is left in the buffer >= what is left in the record
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
   * Handle the processing and recording of completed WET records when we find them
   */
  def processRecord(): Unit = {
    // We have a record, convert the bytes into a UTF-8 string and throw it in the
    // ListBuffer that is holding our complete records
    wetrecords += new String(recordbuffer.toArray, "UTF-8")
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
