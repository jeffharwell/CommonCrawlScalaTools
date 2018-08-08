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

/*
 * Parser Factory Companion Object
 *
 * This is needed in order to implement multiple constructors and
 * encapsulate the definition of the EmptyCategorizer class.
 *
 * http://daily-scala.blogspot.com/2009/11/multiple-constructors.html
 */
object Parser {
  /*
   * When no arguments are supplied create a class that implements WARCCategorizer
   * and never categorizes anything and pass that to the constructor.
   *
   * @param inputstream a java.io.InputStream giving access to a gzipped WET archive
   */
  def apply(inputstream: InputStream): Parser[EmptyCategorizer] = {
    new Parser(inputstream, new EmptyCategorizer, 0)
  }

  /* 
   * No categorizer passed, but do include a step limit
   *
   * @param inputstream a java.io.InputStream giving access to a gzipped WET archive
   * @param steplimit Int that put an upper limit on the number of steps/transitions the
   *                  Parser FSA will make before returning with a RuntimeException. Useful
   *                  to protect against infinite loops during testing.
   */
  def apply(inputstream: InputStream, steplimit: Int): Parser[EmptyCategorizer] = {
    new Parser(inputstream, new EmptyCategorizer, steplimit)
  }

  /*
   * If a categorizer is passed then use it
   *
   * @param inputstream a java.io.InputStream giving access to a gzipped WET archive
   * @param categorizer A class the inherits from WARCCategorizer that is used to categorize
   *                    the WARC record. The parser simply passes this to the WARCConversion
   *                    object when it initializes it.
   */
  def apply[A <: WARCCategorizer](inputstream: InputStream, categorizer: A): Parser[A] = {
    new Parser(inputstream, categorizer, 0)
  }

  /*
   * Categorizer and a steplimit
   *
   * @param inputstream a java.io.InputStream giving access to a gzipped WET archive
   * @param categorizer A class the inherits from WARCCategorizer that is used to categorize
   *                    the WARC record. The parser simply passes this to the WARCConversion
   *                    object when it initializes it.
   * @param steplimit Int that put an upper limit on the number of steps/transitions the
   *                  Parser FSA will make before returning with a RuntimeException. Useful
   *                  to protect against infinite loops during testing.
   */
  def apply[A <: WARCCategorizer](inputstream: InputStream, categorizer: A, steplimit: Int): Parser[A] = {
    new Parser(inputstream, categorizer, steplimit)
  }

}


class Parser[A <: WARCCategorizer](inputstream: InputStream, categorizer: A, steplimit: Int) extends Iterator[WARCRecord] {

  /*
   * The Constructor
   *
   * Object variables and initialization.
   */

  var linecount: Int = 0
  var recordcount: Int = 0
  var recordlength: Int = 0
  var currentwarcinfo: WARCInfo = new WARCInfo() // this holds the WARCInfo object that
                                                 // gives the context for all of the 
                                                 // conversion records in the file
  var currentwarcconversion: WARCConversion = WARCConversion(categorizer) // this holds the conversion
                                                                          // record we are working on right now
  var hasnext: Boolean = true

  val statetrace = ListBuffer[State]()
  var debug = false
  var steps = 0
  var corruptiondetected = false

  var lasterror = "" // contains the last error, which is hopefully the reason
                     // we ended up in a given sink state

  // Holder for the start and finish triggers if they get defined
  var startTrigger: Option[ParserTrigger] = None : Option[ParserTrigger]
  var finishTrigger: Option[ParserTrigger] = None : Option[ParserTrigger]

  // Initialize our reader
  val reader = new Reader(inputstream)

  /*
   * Functions for setting the Start and Finish Trigger
   *
   */

  /*
   * Add an object that the parser will call once it starts parsing a WARC archive.
   *
   * @param pt An object that inherits from ParseTrigger.
   *
   */
  def addStartTrigger[A <: ParserTrigger](pt: A) = {
    startTrigger = Some(pt)

    // This is a bit tricky. The constructor for the Parser immediately instantiates
    // an FSA and call privateNext() to parse the first record. This means that by the
    // time you add a start trigger the Parser has already internally started parsing the
    // file and has either errored out or already internally loaded the first record.
    //
    // So when this method is called we just immediately call the trigger.
    callStartTrigger()
  }

  /*
   * Add an object that the parser will call once it finishes parsing a WARC archive.
   *
   * @param pt An object that inherits from ParseTrigger.
   *
   */

  def addFinishTrigger[A <: ParserTrigger](pt: A) = {
    finishTrigger = Some(pt)

    if (!hasnext) {
      // Hmm we are adding a finish trigger but we are already done parsing
      // this can happen in cases of a corrupt WARC archive where it doesn't 
      // start with a WARCInfo record.
      callFinishTrigger(None: Option[State])
    }
  }

  /*
   * Private method that calls the start trigger if it is defined
   */
  private def callStartTrigger() = {
    val extractedfilename: Option[String] = currentwarcinfo.get("WARC-Filename")
    val logmessage: Option[String] = currentwarcinfo.get("WARC-Filename") match {
      case Some(f) => None: Option[String]
      case None => Some("Unable to Parse File - didn't even get a WARCInfo record. Is the file corrupt?")
    }

    // if we have a startTrigger, call it
    startTrigger match {
      case Some(f) => f.call(extractedfilename, recordcount, logmessage)
      case None =>
    }

  }

  /*
   * Private method the calles the finish trigger if it is defined
   */
  private def callFinishTrigger(state: Option[State]) = {
    // handle the case of the finishTrigger essentially being called immediately when it is added
    // this means that by the time the trigger was added the parse is already done. This could mean
    // that the file was really corrupt and the parser swept through the entire file looking for even
    // one valid WARC Conversion record, or it may not have even been able to find a complete WARC Info
    // record. This could also mean that the caller add the finish trigger after the parse was already 
    // completed, in which case we don't know the final state and have to guess a bit as to what happened
    // based on the final state of the parser based on the corruptiondetected flag and the recordcount field.
    def handleNoState(): Some[String] = {
      if (corruptiondetected && currentwarcinfo.get("WARC-Filename") == None) {
        Some("File Parse Error - Corruption detected, was not able to find complete WARCInfo record")
      } else if (corruptiondetected) {
        if (recordcount > 0) {
          Some("File Parse Error - Corruption detected but some records were extracted")
        } else {
          Some("File Parse Error - Corruption detected and no WARC Conversion records were extracted")
        }
      } else if (recordcount > 0) {
        Some("File Parsed Normally and retrieved records ... did you added the trigger after the parse was already done??")
      } else {
        Some("File Parsed Normally but retreived no WARC Conversion records.")
      }
    }

    // Sink2 means that we got a valid WARCInfo record, so the file did parse, but we ran into trouble later and the last WARC
    // Conversion record was corrupt somehow.
    def handleSink2(): Some[String] = {
      if (recordcount > 0) {
        Some("End of file was corrupt, some records were extracted successfully.")
      } else {
        Some("End of file was corrupt and no WARC Conversion records were extracted successfully.")
      }
    }

    val extractedfilename: Option[String] = currentwarcinfo.get("WARC-Filename")
    val logmessage: Option[String] = state match {
      case Some(Sink1) => Some("Unable to Parse File - didn't even get a complete WARCInfo record. Is the file corrupt?")
      case Some(Sink2) => handleSink2()
      case Some(Final) => if (corruptiondetected) { Some("File Parsed - Some corruption detected") } else { Some("File Parsed Normally") }
      case _ => handleNoState() 
    }

    // if we have a finish trigger then call it
    finishTrigger match {
      case Some(f) => f.call(extractedfilename, recordcount, logmessage)
      case None =>
    }
  }


  /*
   * Finite State Automata
   *
   * FSA Definitions and Initialization
   */
  
  sealed abstract class Event

  sealed abstract class State {
    def m: Map[Event, State]
    // as per https://sourcemaking.com/design_patterns/state
    // the event call includes a reference to the wrapper 
    // so that the event can access the context
    def event(f: Fsa) : State = {
      // by default no state change on event
      return this
    }

    // Used by States that need to read content from the input stream
    def getContent(w: WARCRecord, f: Fsa): Unit = {
      // Read the amount of content from the InputStream that is specified in the header
      // the +2 handles the blank line (\r\n) between the headers and the content
      val c: String = reader.getStringFromBytes(f.bytestoread+2).trim()

      // Now that we have the content set bytestoread in our context class
      // to zero, this may help prevent a logic bug from causing us to skip records
      f.bytestoread = 0

      // Add the content
      w.addContent(c)
    }

    // Used by states that need to add a header from the input stream
    def addHeaderToWARCObject(w: WARCRecord): Unit = {
      // get a line
      var rawline = reader.getLine()
      if (rawline.contains(": ")) {
        var split = rawline.split(": ") // this is almost certainly a bug waiting to happen
                                        // probably should use a regex instead but that would
                                        // cost us some performance
        if (split.size == 2) {
          // ok, we have a line that split on the right delimiter into two pieces
          // might be a header pass it to the WARC object
          w.addFields(Map[String,String](split(0) -> split(1)))
        }
      }
    }

  }

  // here is the actual FSA
  // this is considered our "Context Class"
  // https://sourcemaking.com/design_patterns/state
  class Fsa {
    private var state: State = S1
    var bytestoread: Int = 0

    // This calls event on the current state which runs
    // the state entry code and returns the next state
    // We come all the way back here, instead of having the
    // state call the next state's event() directly so that
    // we can unit test the transitions
    def run(): State = {
      state = state.event(this)
      return(state)
    }

    def getState(): String = {
      // there must be a better way
      return(state.toString())
    }
  }
 
  /*
   * Events
   */
  object E1 extends Event // get WARC Info Header incomplete
  object E2 extends Event // get WARC Info Header -> get WARC Info Content
  object E4 extends Event // get WARC Info complete -> get WARC Conversion Header
  object E5 extends Event // WARC Conversion Header incomplete
  object E6 extends Event // WARC Conversion Header -> get WARC Info Content
  object E7 extends Event // WARC Conversion header corruption -> scan for next record to try
  object E8 extends Event // get WARC Conversion complete -> move to pause state S5
  object E9 extends Event // start getting the next WARC Conversion from input stream
  object E10 extends Event // we hit the end of the stream after a complete record
  object E11 extends Event // for the loop in State 6, the probably not a new record event
  object EX1 extends Event // corrupt WARC file, initial record is not a WARC Info object
  object EX3 extends Event // corrupt WARC file, WARC info content ends prematurely
  object EX5 extends Event // corrput WET archive
  object EX6 extends Event // corrupt WET archive, unable to find subsequent WARC Conversion to parse
  object EX8 extends Event // generic error, we failed to create a complete WARCConversion record
  object ENONE extends Event // it's the non-event ... I'm probably doing something wrong ... :(
  object ESTOP extends Event // this is a non-event kicked out by states
                             // that do not generate a state changing event by themselves
                             // mostly S5 and the sink states

  /*
   * States
   */

  // Sink1 is the state we end up in if the WET archive is corrupt. This primarily handles
  // corruption which prevents us from extracting a complete WARC Info type record. If we cannot 
  // get the WARCInfo record, and it isn't at the top of the file, then we have to bail out.
  object Sink1 extends State { 
    val m = Map[Event, State]()

    override def toString(): String = {
      return "Sink1"
    }
  }

  // Sink2 is where we end up if the WET file is corrupt in some way that we couldn't recover
  // from before the stream ends. The only scenario I can think of right now is if a WARC 
  // record had incomplete headers and so we move to state 6 to try to pick up the next record
  // and hit the end of the stream before we were sucessful.
  object Sink2 extends State {
    val m = Map[Event, State]()

    override def toString(): String = {
      return "Sink2"
    }
  }

  // Final is the final state of the FSA when we finish a complete archive and the achive ends 
  // cleanly with no half-specificied WARC conversion records. This doesn't mean there wasn't 
  // corruption on the way, but it wasn't corruption we couldn't recover from and successfully 
  // retrieve records after the point where something went wrong.
  object Final extends State { // this is the final state of the FSA
    val m = Map[Event, State]() // empty map

    override def toString(): String = {
      return "Final"
    }
  }

  // We have number of required fields + 4 tries to get a complete set of headers
  var state1trylimit = currentwarcinfo.numberRequiredFields() + 4
  var state1tries = 0

  /* State S1
   * this is the initial state, it parses the headers for the inital WARCInfo object.
   * It makes number of required fields (including content as a field) + 4 attempts to 
   * secure a complete set of warcinfo headers before it will transition to an error state
   * (Sink1). This means that if there are a bunch of blank lines at the top of the WET file
   * then it will be detected as corrupt and not parsed.
   */
  object S1 extends State {
    lazy val m = Map[Event, State](EX1 -> Sink1, // corrupt archive, not a warc info record
                                   E1 -> S1, // headers not complete, keep reading
                                   E2 -> S2) // headers complete, get content
    override def toString(): String = {
      return "S1"
    }
    override def event(f: Fsa): State = {
      if (debug) { statetrace += this }

      if (!currentwarcinfo.headersComplete()) {
        if (state1tries >= state1trylimit) {
          // we have hit the limit for the number of tries we have to put together
          // a complete WARCInfo header set. Proceed to Sink1 with an error
          lasterror = s"Fatal: S1 -> Sink1: We were unable to get a complete set of warcinfo headers in ${state1tries} tries"
          state1tries = 0
          corruptiondetected = true
          return(m(EX1))
        }
        state1tries += 1

        if (reader.isEndOfStream()) {
          // event EX1
          // we hit the end of the stream without getting a complete set
          // of headers, something is corrupt ... probably
          lasterror = s"S1 -> Sink1: We hit the end of the stream without getting a complete set of WARCInfo headers"
          corruptiondetected = true
          state1tries = 0
          return(m(EX1))
        }

        // Grab the next line and try to add it is a header
        try {
          addHeaderToWARCObject(currentwarcinfo)
        } catch {
          case e: WARCRecordTypeException => {
            // apparently we are not parsing a warcinfo type WARC record
            // move to the error state
            lasterror = s"S1 -> Sink1: Record does not have WARC-Type = warcinfo as required, archive may be corrupt."
            corruptiondetected = true
            state1tries = 0
            return(m(EX1))
          }
        }

        // event E1, assume we don't have a complete set of headers yet and keep looking
        return(m(E1))
      } else {
        // headers are complete, we should know how many bytes of content to read
        f.bytestoread = currentwarcinfo.getContentSizeInBytes()

        // event E2, if the headers complete we transition to S2
        return(m(E2))
      }
    }


  }


  // This state grabs the content for the initial WARC Info object
  object S2 extends State {
    lazy val m = Map[Event, State](E4 -> S3, // we have the content, start looking for conversion
                                   EX3 -> Sink1) // corrupt archive, content ended prematurely
    override def toString(): String = {
      return "S2"
    }

    override def event(f: Fsa): State = {
      if (debug) { statetrace += this }

      // I'm intentionally not going to do the logic checks that should have been caught
      // in S1. If you jump into the middle of the FSA you need to take care of that yourself.
      // e.g. currentwarcinfo.headersComplete() is assumed to be true in this state.
      
      if (reader.isEndOfStream()) {
        // event EX1
        // we hit the end of the stream without being able to get the content
        // something is corrupt ... probably
        lasterror = "S2 -> Sink1: We hit the end of stream without getting content for the WARCInfo record"
        corruptiondetected = true
        return(m(EX1))
      }

      // get the content and add it to the currentwarcinfo record
      getContent(currentwarcinfo, f)

      // WARCInfo should be complete now, if so move to WARCConversion parse state
      // if not move to error state.
      currentwarcinfo.isComplete() match {
        case true => return(m(E4))
        case false => {
          lasterror = "S2 -> Sink1: Got content and headers but WARCInfo is still incomplete"
          return(m(EX3))
        }
      }
    }
  }

  // We have number of required fields + 4 tries to get a complete set of headers
  var state3trylimit = currentwarcconversion.numberRequiredFields() + 4
  var state3tries = 0

  // getting WARC Conversion record headers
  object S3 extends State {
    override lazy val m = Map[Event, State](EX5 -> Sink1, // corrupt archive
                                             E5 -> S3,    // headers not complete, keep reading
                                             E6 -> S4,    // headers complete, move to get content
                                             E7 -> S6,    // headers corrupt, scan to find next record
                                             E10 -> Final) // done with the WET Archive File
    override def toString(): String = {
      return "S3"
    }
    override def event(f: Fsa): State = {
      if (debug) { statetrace += this }

      if (reader.isEndOfStream()) {
        // Two cases here, we might be coming from S5 and actually be at the end
        // of the WET archive, in that case we are done, go to Final state
        if (currentwarcconversion.fields.size == 0) {
          // done, go to the final state via Event E10
          state3tries = 0
          return(m(E10))
        } else {
          // event EX1
          // we hit the end of the stream without getting a complete set
          // of headers, something is corrupt ... probably
          lasterror = "S3 -> Sink1: We hit the end of the stream without getting a complete set of WARCConversion headers"
          state3tries = 0
          corruptiondetected = true
          return(m(EX5))
        }
      }

      state3tries += 1 // record our try
      // Grab the next line and try to add it is a header
      try {
        addHeaderToWARCObject(currentwarcconversion)
      } catch {
        case e: WARCRecordTypeException => {
          // apparently we are not parsing a conversion type WARC record
          // assume this record is corrupt and move to state S6 and start 
          // scanning for a new WARC record to parse
          state3tries = 0
          return(m(E7))
        }
      }


      if (!currentwarcconversion.headersComplete()) {
        if (state3tries >= state3trylimit) {
          // we are out of attempts, WARC record probably corrupt
          // move to S6 to see if there are any other records in this
          // stream that we could try to parse
          state3tries = 0
          corruptiondetected = true
          return(m(E7))
        } else {
          // keep looking for headers
          return(m(E5))
        }
      } else {
        // headers are complete, we should know how many bytes of content to read
        f.bytestoread = currentwarcconversion.getContentSizeInBytes()
        state3tries = 0 // reset our tries
        return(m(E6)) // transition to S4 to get the content
      }
    }
  }

  // getting WARC Conversion content
  object S4 extends State {
    lazy val m = Map[Event, State](E8 -> S5, // this is our stop state, record is complete
                                   EX8 -> Sink1) // error, we failed to get a complete record
    override def toString(): String = {
      return "S4"
    }

    override def event(f: Fsa): State = {
      if (debug) { statetrace += this }

      // I'm intentionally not going to do the logic checks that should have been caught
      // in S1. If you jump into the middle of the FSA you need to take care of that yourself.
      // e.g. currentwarcinfo.headersComplete() is assumed to be true in this state.
      
      if (reader.isEndOfStream()) {
        // event EX8
        // we hit the end of the stream without being able to get the content
        // something is corrupt ... probably
        lasterror = s"S4 -> ${m(EX8).toString()}: End of Stream without getting content for WARCConversion"
        corruptiondetected = true
        return(m(EX8))
      }

      // get the content and add it to the currentwarcinfo record
      getContent(currentwarcconversion, f)

      // Now add in our WARCInfo record
      currentwarcconversion.addWARCInfo(currentwarcinfo)

      // WARCConversion record should be complete now, if so move to get the next one
      // if not move to error state.
      currentwarcconversion.isComplete() match {
        case true => {
          // we have a complete WARCConversion record
          recordcount += 1
          m(E8)
        }
        case false => {
          lasterror = s"S4 -> ${m(EX8).toString()}: got content but WARCConversion still not complete"
          m(EX8)
        }
      }
    }
  }

  // This state means that we have got a complete WARCConversion record
  // a call to event moves the FSA to wipe out the current WARCConversion record
  // and move to S3 to start getting the next WARC Conversion record from the input stream
  object S5 extends State {
    lazy val m = Map[Event, State](E9 -> S3) // only one transition allowed, get the new WARCConversion
                                             // record
    override def toString(): String = {
      return "S5"
    }

    override def event(f: Fsa): State = {
      if (debug) { statetrace += this }

      // create a new WARCConversion record
      currentwarcconversion = WARCConversion(categorizer)

      // Return state S3 which will start adding headers to the new record
      // that we just created.
      return(m(E9))
    }
  }

  object S6 extends State {
    lazy val m = Map[Event, State](E9 -> S3, // found one .. maybe, back to S3 to parse
                                   E11 -> S6, // no new record yet, keep looking
                                   EX6 -> Sink2) // ran out of stream before finding new record
    override def toString(): String = {
      return "S6"
    }

    override def event(f: Fsa): State = {
      if (debug) { statetrace += this }

      if (reader.isEndOfStream()) {
        // we ran out of stream before finding another record goto Sink2
        lasterror = "S6 -> Sink2: Unable to find any additional WARCConversion records after corruption detected"
        corruptiondetected = true
        return(m(EX6))
      }

      // get a line
      var rawline = reader.getLine()
      if (rawline.contains("WARC/1.0")) {
        // alright, we might have found one, go to state 3 and try to parse it
        
        // create a new WARCConversion record
        currentwarcconversion = WARCConversion(categorizer)

        return(m(E9))
      } else {
        // nope, keep looking
        return(m(E11))
      }
    }
  }


  // Initialize the FSA
  var fsa = new Fsa

  /*
   * Iterator
   *
   * Upon initialization of the object we want to read the first record from the InputStream
   * buffer and populate the next result so that if .hasNext() is called we can tell the 
   * truth. We do this because there is no way to reliably tell if an inputstream is really 
   * empty without trying to read the next value. Much like Drivesavers, we won't say we can
   * get the information unless we already have it ;)
   *
   * Note that this actually means that in practice we will pull the first TWO WARC records 
   * from the InputStream as we need the warcinfo type record to provide the context for
   * the conversion records. The WARCConversion objects we will be returning require some of the
   * information from the warcinfo record.
   */

  // if all goes well this should set the currentwarccoversion record with a valid
  // conversion record 
  privateNext()

  /*
   * Begin Methods
   */

  /* Getters and Setters */

  def getLineCount(): Int = {
    return linecount
  }
  def getRecordCount(): Int = {
    return recordcount
  }

  /*
   * Sets debug to true. The primary change is that this causes the FSA to
   * keep a list of state changes in the statetrace listbuffer so that you can 
   * see exactly what happened. This is very helpful for troubleshooting improper
   * transitions to Sink.
   */
  def setDebug(): Unit = {
    debug = true
  }
  /*
   * Clears the debug flag, clears out any record of state transitions and stops tracking them
   */
  def clearDebug(): Unit = {
    statetrace.clear()
    debug = false
  }

  /*
   * This is the next() implementation for our Iterator
   *
   * @return A list of individual WET records
   */
  def next(): WARCRecord = {
    // docs say behaviour when next() is called after hasNext() would
    // report false is undefined, in our case we will throw a runtime
    // as I think it likely the code has made a mistake that the programmer
    // should know about.
    if (!this.hasNext()) {
      throw new RuntimeException(".next() called when no records remain in the iterator")
    }

    // Copy nextrecord into the record we are going to return
    var recordtoreturn = currentwarcconversion

    // Go ahead and try to get the record we will return if next() is called again
    privateNext()

    // Return the record we already had
    return recordtoreturn
  }

  /*
   * This is the hasNext() implementation for the iterator. The variable hasnext
   * is set to false by the parseBuffer() method when we reach the end of the InputStream
   *
   * @return boolean indicating if there is another record in the iterator
   */
  def hasNext(): Boolean = {
    return hasnext
  }

  /*
   * This method actually calls parseBuffer and extracts the next record
   *
   * @return A list of individual WET records
   */
  def privateNext(): Unit = {
    // This is the method that kicks off the FSA. We run the FSA until we either hit a sink
    // state (at which time we throw an exception ... probably) or find a complete record
    // that we could return
    //
    // Remember that this method sets up the record that will be return when .next() is actually
    // called. This method is called on initialization of the object to queue up the first record.
   
    // recursive routine to run the FSA

    // this shouldn't return until the instance variable 'currentwarcconversion' is set
    // with a complete WARC conversion record
    var state: State = fsa.run()
    steps = steps + 1
    // This is the main run loop, keep running the fsa (repeatedly calling fsa.run() until
    // we hit one of the states specified below.
    while (state != Sink1 && state != Sink2 && state != Final && state != S5) {
      // I keep having to look this up, so there is the logic.
      // When you call fsa.run() it calls the .event method on the EXISTING state.
      // That event method will run the states logic and return the new internal state.
      // So .. the state that is returned, which goes into the 'state' variable below,
      // has not had it's .event method called yet, it is waiting for the next call
      // to fsa.run().
      //
      // For example: state S5 is the stop state for having a complete WARC record.
      //              The only way to get to S5 is through S4. So if you want to 
      //              increment the total number of WARCConversion records parsed
      //              you would increment it at the end of the S4 .event() method
      //              before it returns S5. If you put the increment code in the 
      //              .event() method of S5 you would always be off one during the
      //              parsing itself, although you would catch up with S5 transitions
      //              to Final via S3 after the last record.
      state = fsa.run()
      steps = steps + 1
      if (steplimit > 0 && steps > steplimit) {
        throw new RuntimeException(s"FSA Exceded the step limit ${steplimit}. Ended in state: "+state)
      }
    }

    // These states mean we are done with the file one way
    // or another
    if (state == Sink1 || state == Sink2 || state == Final) {
      hasnext = false

      // We are done, call the finish trigger if we have one
      callFinishTrigger(Some(state))
    }
  }
}
