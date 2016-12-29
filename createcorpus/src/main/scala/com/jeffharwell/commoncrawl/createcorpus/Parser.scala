package com.jeffharwell.commoncrawl.createcorpus

import java.io.File;
import java.io.BufferedInputStream
import java.io.FileInputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.zip.GZIPInputStream


class Parser {
  val linebuffer = scala.collection.mutable.ArrayBuffer.empty[Byte]
  val recordbuffer = scala.collection.mutable.ArrayBuffer.empty[Byte]
  val newlinebyte = "\n".getBytes("UTF-8").last
  var linecount: Int = 0
  var recordcount: Int = 0
  var recordlength: Int = 0
  var contentbytesread: Int = 0
  var bufferbytesread: Int = 0
  var printnextline: Boolean = false

  /*
   * This is the method called by the program populating the a read buffer.
   * The read buffer will contain some small chunk of a WET file, the job of this
   * class is to take a sequence of those buffers and return a set of distinct records
   * from the WET file.
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
