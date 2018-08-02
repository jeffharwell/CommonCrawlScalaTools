package com.jeffharwell.commoncrawl.warcparser

import java.io.File;
import java.io.BufferedInputStream
import java.io.InputStream
import java.io.FileInputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.zip.GZIPInputStream
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.List
import scala.collection.immutable.Map
import scala.annotation.tailrec


class Reader(inputstream: InputStream, internalbuffersize: Int) {

  // Secondary constructor, sets the default internal read buffer size
  // to 1024 if no size is specified
  def this(inputstream: InputStream) {
    this(inputstream, 1024)
  }

  /* Here is a wrapper class that wraps an InputStream
   * but always returns > 0 when .available() is called.
   * This will cause GZIPInputStream to always make another 
   * call to the InputStream to check for an additional 
   * concatenated GZIP file in the stream.
   * See:
   * http://stackoverflow.com/questions/41400810/gzipinputstream-closes-prematurely-when-decompressing-httpinputstream
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



  // Input Stream
  var gzcInputStream: GZIPCompatibilityWrapper = new GZIPCompatibilityWrapper(inputstream)
  var gzInputStream: GZIPInputStream = new GZIPInputStream(gzcInputStream)
  // Our read buffer
  var buffersize: Int = internalbuffersize
  val buffer = new Array[ Byte ]( buffersize )
  var bufferreadindex: Int = 0 // index to where we are in reading the buffer
  var buffercontentlength: Int = 0 // ammount of content in the buffer
  var endofstream: Boolean = false
  // Other Varibles
  var bytesreturned: Int = 0 // this variable keeps track of how many bytes we just returned
  // Other Constants
  val newlinebyte = "\n".getBytes("UTF-8").last

  /*
   * Get the size of the internal read buffer
   *
   * @return Integer size of the internal read buffer
   */
  def getInternalBufferSize(): Integer = {
    return buffer.size
  }

  /*
   * Getter for the bytesreturned class variable. Methods the read from the inputstream
   * and return content will all set the bytesreturned variable before they convert to
   * String and return. This method allows the caller to see how many bytes were actually
   * read, this is primarily useful to determine if the getStringFromBytes method actually
   * read as much content as you asked it to or if it ran out of inputstream.
   *
   * @return the number of bytes actually read and return from the decompressed input stream
   */
  def getBytesRead(): Int = {
    return bytesreturned
  }

  /*
   * Getter for the endofstream class variable. Methods that read from the inputstream will
   * set this to true when they encounter the end of the stream
   *
   * @return boolean true if end of stream false otherwise
   */
  def isEndOfStream(): Boolean = {
    return endofstream
  }


  /*
   * Private method to refill our internal read buffer. Updates class variables
   * bufferreadindex, buffercontentlength and endofstream
   */
  private def refillBuffer(): Unit = {
    // Grab a chunk of the input stream
    buffercontentlength = gzInputStream.read(buffer)
    // reset the read index
    bufferreadindex = 0

    // check and see if we have hit the end of the stream
    if (buffercontentlength == -1) {
      buffercontentlength = 0 // so we don't break things downstream as array(-1) is an error
      endofstream = true
    }
  }

  /*
   * This method will read the specified number of bytes (if available) from the
   * compressed input stream and return them as a UTF-8 string. If there are not
   * enough bytes available in the stream it will return what was available.
   *
   * @param n bytes to read
   */
  def getStringFromBytes(n: Int): String = {
    val contentbuffer = scala.collection.mutable.ArrayBuffer.empty[Byte]
    // reset bytes returned
    bytesreturned = 0


    if (endofstream) {
      // we were called by the input stream is already closed, throw on errer
      throw new RuntimeException("Input stream is already closed, no more data is available")
    }

    // even if we don't have enough content if we hit endofstream while refilling 
    // the buffer then return what we have
    while (contentbuffer.size < n && !endofstream) {
      if (buffercontentlength - bufferreadindex < n - contentbuffer.size) {
        // what is left in the buffer is < the amount that we need to read .. so ..
        // grab the entire buffer (if it isn't empty or already completely processed)
        if (buffercontentlength - bufferreadindex != 0) {
          // The buffer isn't empty or already completely processed
          contentbuffer.appendAll(buffer.slice(bufferreadindex, buffercontentlength))
          // update the bytesread object variable
          bytesreturned += buffercontentlength - bufferreadindex

          // just for sanity sake move the bufferreadindex although
          // we know that refillBuffer will just reset it to 0
          bufferreadindex = buffercontentlength
        }
        // now refill the buffer and keep trucking
        refillBuffer()
      } else if (buffercontentlength - bufferreadindex >= n - contentbuffer.size) {
        // What is left in the buffer is >= the amount of content we need read .. so ..
        // grab what we need from the buffer and be done

        // calculate where the end of our content is in the read buffer
        var contentendinbuffer = bufferreadindex + (n - contentbuffer.size)

        // now grab it
        contentbuffer.appendAll(buffer.slice(bufferreadindex, contentendinbuffer))
        // update the bytesread object variable
        bytesreturned += contentendinbuffer - bufferreadindex


        // keep track of where we are in the buffer by moving the bufferreadindex
        // slice is (from <= indexOf(x) < until) so read index should now be set
        // to the value of contentendinbuffer (which was calculated on # bytes not index)
        bufferreadindex = contentendinbuffer
      }
    }
    return new String(contentbuffer.toArray, "UTF-8")
  }

  /*
   * Get the next complete line from the inputstream
   *
   * @return a UTF-8 string representation of the line
   */
  def getLine(): String = {
    // reset bytes returned
    bytesreturned = 0
    // set up an array to hold the line
    val linebuffer = scala.collection.mutable.ArrayBuffer.empty[Byte]

    if (endofstream) {
      // we were called by the input stream is already closed, throw on errer
      throw new RuntimeException("Input stream is already closed, no more data is available")
    }

    // little recursive routine to read the inputstream and push the 
    // bytes into an array until we hit a newline character.
    @tailrec def readToNewline(): Unit = {
      if (bufferreadindex == buffercontentlength && !endofstream) {
        // we have read to the end of the buffer, refill it
        refillBuffer()
        readToNewline() // try again
      } else {
        var b = buffer(bufferreadindex)
        bufferreadindex += 1

        if (b != newlinebyte && !endofstream) {
          // no newline yet
          linebuffer.append(b)
          readToNewline() // next byte
        }
      }
    }

    readToNewline()

    var str = new String(linebuffer.toArray, "UTF-8")
    // right trim
    return str.replaceAll("\\s+$", "")
  }

}
