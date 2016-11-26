package akka.actor

/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 * WARNING: Modified version by Andrea just to test entry/exit
 */

import akka.routing.{ Deafen, Listen, Listeners }

import scala.collection.mutable
import scala.concurrent.duration.{ Duration, FiniteDuration, _ }
import scala.language.implicitConversions

/**
 * Finite State Machine actor trait. Use as follows:
 *
 * <pre>
 * trait State
 * case class One extends State
 * case class Two extends State
 *
 * case class Data(i : Int)
 * }
 *
 * class A extends Actor with FSM[A.State, A.Data] {
 * import A._
 *
 * startWith(One, Data(42))
 * when(One) {
 * case Event(SomeMsg, Data(x)) => ...
 * case Ev(SomeMsg) => ... // convenience when data not needed
 * }
 * when(Two, stateTimeout = 5 seconds) { ... }
 * initialize()
 * }
 * </pre>
 *
 * Within the partial function the following values are returned for effecting
 * state transitions:
 *
 * - <code>stay</code> for staying in the same state
 * - <code>stay using Data(...)</code> for staying in the same state, but with
 * different data
 * - <code>stay forMax 5.millis</code> for staying with a state timeout; can be
 * combined with <code>using</code>
 * - <code>goto(...)</code> for changing into a different state; also supports
 * <code>using</code> and <code>forMax</code>
 * - <code>stop</code> for terminating this FSM actor
 *
 * Each of the above also supports the method <code>replying(AnyRef)</code> for
 * sending a reply before changing state.
 *
 * While changing state, custom handlers may be invoked which are registered
 * using <code>onTransition</code>. This is meant to enable concentrating
 * different concerns in different places; you may choose to use
 * <code>when</code> for describing the properties of a state, including of
 * course initiating transitions, but you can describe the transitions using
 * <code>onTransition</code> to avoid having to duplicate that code among
 * multiple paths which lead to a transition:
 *
 * <pre>
 * onTransition {
 * case Active -&gt; _ =&gt; cancelTimer("activeTimer")
 * }
 * </pre>
 *
 * Multiple such blocks are supported and all of them will be called, not only
 * the first matching one.
 *
 * Another feature is that other actors may subscribe for transition events by
 * sending a <code>SubscribeTransitionCallback</code> message to this actor.
 * Stopping a listener without unregistering will not remove the listener from the
 * subscription list; use <code>UnsubscribeTransitionCallback</code> before stopping
 * the listener.
 *
 * State timeouts set an upper bound to the time which may pass before another
 * message is received in the current state. If no external message is
 * available, then upon expiry of the timeout a StateTimeout message is sent.
 * Note that this message will only be received in the state for which the
 * timeout was set and that any message received will cancel the timeout
 * (possibly to be started again by the next transition).
 *
 * Another feature is the ability to install and cancel single-shot as well as
 * repeated timers which arrange for the sending of a user-specified message:
 *
 * <pre>
 * setTimer("tock", TockMsg, 1 second, true) // repeating
 * setTimer("lifetime", TerminateMsg, 1 hour, false) // single-shot
 * cancelTimer("tock")
 * isTimerActive("tock")
 * </pre>
 */
trait EntryExitFSM[S, D] extends Actor with Listeners with ActorLogging {

  import FSM._

  type State = FSM.State[S, D]
  type Event = FSM.Event[D]
  type StopEvent = FSM.StopEvent[S, D]
  type StateFunction = scala.PartialFunction[Event, State]
  type Timeout = Option[FiniteDuration]
  type TransitionHandler = PartialFunction[(S, S), Unit]

  /*
     * “import” so that these are visible without an import
     */
  val Event: FSM.Event.type = FSM.Event
  val StopEvent: FSM.StopEvent.type = FSM.StopEvent

  /**
   * This extractor is just convenience for matching a (S, S) pair, including a
   * reminder what the new state is.
   */
  val `->` = FSM.`->`

  /**
   * This case object is received in case of a state timeout.
   */
  val StateTimeout = FSM.StateTimeout

  /**
   * ****************************************
   * DSL
   * ****************************************
   */

  /**
   * Insert a new StateFunction at the end of the processing chain for the
   * given state. If the stateTimeout parameter is set, entering this state
   * without a differing explicit timeout setting will trigger a StateTimeout
   * event; the same is true when using #stay.
   *
   * @param stateName designator for the state
   * @param stateTimeout default state timeout for this state
   * @param stateFunction partial function describing response to input
   */
  final def when(stateName: S, stateTimeout: FiniteDuration = null)(stateFunction: StateFunction): Unit =
    register(stateName, stateFunction, Option(stateTimeout))

  /**
   * Set initial state. Call this method from the constructor before the [[#initialize]] method.
   * If different state is needed after a restart this method, followed by [[#initialize]], can
   * be used in the actor life cycle hooks [[akka.actor.Actor#preStart]] and [[akka.actor.Actor#postRestart]].
   *
   * @param stateName initial state designator
   * @param stateData initial state data
   * @param timeout state timeout for the initial state, overriding the default timeout for that state
   */
  final def startWith(stateName: S, stateData: D, timeout: Timeout = None): Unit =
    currentState = FSM.State(stateName, stateData, timeout)

  /**
   * Produce transition to other state.
   * Return this from a state function in order to effect the transition.
   *
   * This method always triggers transition events, even for `A -> A` transitions.
   * If you want to stay in the same state without triggering an state transition event use [[#stay]] instead.
   *
   * @param nextStateName state designator for the next state
   * @return state transition descriptor
   */
  final def goto(nextStateName: S): State = FSM.State(nextStateName, currentState.stateData)

  /**
   * Produce "empty" transition descriptor.
   * Return this from a state function when no state change is to be effected.
   *
   * No transition event will be triggered by [[#stay]].
   * If you want to trigger an event like `S -&gt; S` for `onTransition` to handle use `goto` instead.
   *
   * @return descriptor for staying in current state
   */
  final def stay(): State = goto(currentState.stateName).withNotification(false) // cannot directly use currentState because of the timeout field

  /**
   * Produce change descriptor to stop this FSM actor with reason "Normal".
   */
  final def stop(): State = stop(Normal)

  /**
   * Produce change descriptor to stop this FSM actor including specified reason.
   */
  final def stop(reason: Reason): State = stop(reason, currentState.stateData)

  /**
   * Produce change descriptor to stop this FSM actor including specified reason.
   */
  final def stop(reason: Reason, stateData: D): State = stay using stateData withStopReason (reason)

  final class TransformHelper(func: StateFunction) {
    def using(andThen: PartialFunction[State, State]): StateFunction =
      func andThen (andThen orElse { case x ⇒ x })
  }

  final def transform(func: StateFunction): TransformHelper = new TransformHelper(func)

  /**
   * Schedule named timer to deliver message after given delay, possibly repeating.
   * Any existing timer with the same name will automatically be canceled before
   * adding the new timer.
   * @param name identifier to be used with cancelTimer()
   * @param msg message to be delivered
   * @param timeout delay of first message delivery and between subsequent messages
   * @param repeat send once if false, scheduleAtFixedRate if true
   */
  final def setTimer(name: String, msg: Any, timeout: FiniteDuration, repeat: Boolean = false): Unit = {
    if (debugEvent)
      log.debug("setting " + (if (repeat) "repeating " else "") + "timer '" + name + "'/" + timeout + ": " + msg)
    if (timers contains name) {
      timers(name).cancel
    }
    val timer = Timer(name, msg, repeat, timerGen.next)(context)
    timer.schedule(self, timeout)
    timers(name) = timer
  }

  /**
   * Cancel named timer, ensuring that the message is not subsequently delivered (no race).
   * @param name of the timer to cancel
   */
  final def cancelTimer(name: String): Unit = {
    if (debugEvent)
      log.debug("canceling timer '" + name + "'")
    if (timers contains name) {
      timers(name).cancel
      timers -= name
    }
  }

  /**
   * Inquire whether the named timer is still active. Returns true unless the
   * timer does not exist, has previously been canceled or if it was a
   * single-shot timer whose message was already received.
   */
  final def isTimerActive(name: String): Boolean = timers contains name

  /**
   * Set state timeout explicitly. This method can safely be used from within a
   * state handler.
   */
  final def setStateTimeout(state: S, timeout: Timeout): Unit = stateTimeouts(state) = timeout

  /**
   * INTERNAL API, used for testing.
   */
  private[akka] final def isStateTimerActive = timeoutFuture.isDefined

  /**
   * Set handler which is called upon each state transition, i.e. not when
   * staying in the same state. This may use the pair extractor defined in the
   * FSM companion object like so:
   *
   * <pre>
   * onTransition {
   * case Old -&gt; New =&gt; doSomething
   * }
   * </pre>
   *
   * It is also possible to supply a 2-ary function object:
   *
   * <pre>
   * onTransition(handler _)
   *
   * private def handler(from: S, to: S) { ... }
   * </pre>
   *
   * The underscore is unfortunately necessary to enable the nicer syntax shown
   * above (it uses the implicit conversion total2pf under the hood).
   *
   * <b>Multiple handlers may be installed, and every one of them will be
   * called, not only the first one matching.</b>
   */
  final def onTransition(transitionHandler: TransitionHandler): Unit = transitionEvent :+= transitionHandler

  /**
   * Convenience wrapper for using a total function instead of a partial
   * function literal. To be used with onTransition.
   */
  implicit final def total2pf(transitionHandler: (S, S) ⇒ Unit): TransitionHandler =
    new TransitionHandler {
      def isDefinedAt(in: (S, S)) = true

      def apply(in: (S, S)) {
        transitionHandler(in._1, in._2)
      }
    }

  /**
   * Set handler which is called upon termination of this FSM actor. Calling
   * this method again will overwrite the previous contents.
   */
  final def onTermination(terminationHandler: PartialFunction[StopEvent, Unit]): Unit =
    terminateEvent = terminationHandler

  /**
   * Set handler which is called upon reception of unhandled messages. Calling
   * this method again will overwrite the previous contents.
   *
   * The current state may be queried using ``stateName``.
   */
  final def whenUnhandled(stateFunction: StateFunction): Unit =
    handleEvent = stateFunction orElse handleEventDefault

  /**
   * Verify existence of initial state and setup timers. This should be the
   * last call within the constructor, or [[akka.actor.Actor#preStart]] and
   * [[akka.actor.Actor#postRestart]]
   *
   * An initial `currentState -> currentState` notification will be triggered by calling this method.
   *
   * @see [[#startWith]]
   */
  final def initialize(): Unit =
    if (currentState != null) makeTransition(currentState)
    else throw new IllegalStateException("You must call `startWith` before calling `initialize`")

  /**
   * Return current state name (i.e. object of type S)
   */
  final def stateName: S = {
    if (currentState != null) currentState.stateName
    else throw new IllegalStateException("You must call `startWith` before using `stateName`")
  }

  /**
   * Return current state data (i.e. object of type D)
   */
  final def stateData: D =
    if (currentState != null) currentState.stateData
    else throw new IllegalStateException("You must call `startWith` before using `stateData`")

  /**
   * Return next state data (available in onTransition handlers)
   */
  final def nextStateData = nextState match {
    case null ⇒ throw new IllegalStateException("nextStateData is only available during onTransition")
    case x    ⇒ x.stateData
  }

  /*
     * ****************************************************************
     *                PRIVATE IMPLEMENTATION DETAILS
     * ****************************************************************
     */

  private[akka] def debugEvent: Boolean = false

  /*
     * FSM State data and current timeout handling
     */
  private var currentState: State = _
  private var timeoutFuture: Option[Cancellable] = None
  private var nextState: State = _
  private var generation: Long = 0L

  /*
     * Timer handling
     */
  private val timers = mutable.Map[String, Timer]()
  private val timerGen = Iterator from 0

  /*
     * State definitions
     */
  private val stateFunctions = mutable.Map[S, StateFunction]()
  private val stateTimeouts = mutable.Map[S, Timeout]()

  private def register(name: S, function: StateFunction, timeout: Timeout): Unit = {
    if (stateFunctions contains name) {
      stateFunctions(name) = stateFunctions(name) orElse function
      stateTimeouts(name) = timeout orElse stateTimeouts(name)
    } else {
      stateFunctions(name) = function
      stateTimeouts(name) = timeout
    }
  }

  /*
     * unhandled event handler
     */
  private val handleEventDefault: StateFunction = {
    case Event(value, stateData) ⇒
      log.warning("unhandled event " + value + " in state " + stateName)
      stay
  }
  private var handleEvent: StateFunction = handleEventDefault

  /*
     * termination handling
     */
  private var terminateEvent: PartialFunction[StopEvent, Unit] = NullFunction

  /*
     * transition handling
     */
  private var transitionEvent: List[TransitionHandler] = Nil

  private def handleTransition(prev: S, next: S) {
    val tuple = (prev, next)
    for (te ← transitionEvent) {
      if (te.isDefinedAt(tuple)) te(tuple)
    }
  }

  /*
     * *******************************************
     *       Main actor receive() method
     * *******************************************
     */
  override def receive: Receive = {
    case TimeoutMarker(gen) ⇒
      if (generation == gen) {
        processMsg(StateTimeout, "state timeout")
      }
    case t @ Timer(name, msg, repeat, gen) ⇒
      if ((timers contains name) && (timers(name).generation == gen)) {
        if (timeoutFuture.isDefined) {
          timeoutFuture.get.cancel()
          timeoutFuture = None
        }
        generation += 1
        if (!repeat) {
          timers -= name
        }
        processMsg(msg, t)
      }
    case SubscribeTransitionCallBack(actorRef) ⇒
      // TODO Use context.watch(actor) and receive Terminated(actor) to clean up list
      listeners.add(actorRef)
      // send current state back as reference point
      actorRef ! CurrentState(self, currentState.stateName)
    case Listen(actorRef) ⇒
      // TODO Use context.watch(actor) and receive Terminated(actor) to clean up list
      listeners.add(actorRef)
      // send current state back as reference point
      actorRef ! CurrentState(self, currentState.stateName)
    case UnsubscribeTransitionCallBack(actorRef) ⇒
      listeners.remove(actorRef)
    case Deafen(actorRef) ⇒
      listeners.remove(actorRef)
    case value ⇒ {
      if (timeoutFuture.isDefined) {
        timeoutFuture.get.cancel()
        timeoutFuture = None
      }
      generation += 1
      processMsg(value, sender())
    }
  }

  private def processMsg(value: Any, source: AnyRef): Unit = {
    val event = Event(value, currentState.stateData)
    processEvent(event, source)
  }

  private[akka] def processEvent(event: Event, source: AnyRef): Unit = {
    val stateFunc = stateFunctions(currentState.stateName)
    val nextState = if (stateFunc isDefinedAt event) {
      stateFunc(event)
    } else {
      // handleEventDefault ensures that this is always defined
      handleEvent(event)
    }
    applyState(nextState)
  }

  private[akka] def applyState(nextState: State): Unit = {
    nextState.stopReason match {
      case None ⇒ makeTransition(nextState)
      case _ ⇒
        nextState.replies.reverse foreach { r ⇒ sender() ! r }
        terminate(nextState)
        context.stop(self)
    }
  }

  private[akka] def makeTransition(nextState: State): Unit = {
    if (!stateFunctions.contains(nextState.stateName)) {
      terminate(stay withStopReason Failure("Next state %s does not exist".format(nextState.stateName)))
    } else {
      nextState.replies.reverse foreach { r ⇒ sender() ! r }
      if (currentState.stateName != nextState.stateName || nextState.notifies) {

        val exitFunc = exitMap.get(currentState.stateName)
        exitFunc match {
          case Some(exitF) ⇒
            val exitData = if (exitF.isDefinedAt(currentState.stateData))
              exitF(currentState.stateData)
            else
              currentState.stateData
            currentState = currentState.copy(stateData = exitData)
          case None ⇒ // no exit defined
        }

        this.nextState = nextState
        handleTransition(currentState.stateName, nextState.stateName)
        gossip(Transition(self, currentState.stateName, nextState.stateName))
        this.nextState = null

        val entryFunc = entryMap.get(nextState.stateName)
        entryFunc match {
          case Some(entryF) ⇒
            val entryData = if (entryF.isDefinedAt(nextState.stateData))
              entryF(nextState.stateData)
            else nextState.stateData
            currentState = nextState.copy(stateData = entryData)
          case None ⇒
            currentState = nextState
        }

      } else {
        currentState = nextState
      }

      def scheduleTimeout(d: FiniteDuration): Some[Cancellable] = {
        import context.dispatcher
        Some(context.system.scheduler.scheduleOnce(d, self, TimeoutMarker(generation)))
      }

      currentState.timeout match {
        case SomeMaxFiniteDuration                    ⇒ // effectively disable stateTimeout
        case Some(d: FiniteDuration) if d.length >= 0 ⇒ timeoutFuture = scheduleTimeout(d)
        case _ ⇒
          val timeout = stateTimeouts(currentState.stateName)
          if (timeout.isDefined) timeoutFuture = scheduleTimeout(timeout.get)
      }
    }
  }

  /**
   * Call `onTermination` hook; if you want to retain this behavior when
   * overriding make sure to call `super.postStop()`.
   *
   * Please note that this method is called by default from `preRestart()`,
   * so override that one if `onTermination` shall not be called during
   * restart.
   */
  override def postStop(): Unit = {
    /*
         * setting this instance’s state to terminated does no harm during restart
         * since the new instance will initialize fresh using startWith()
         */
    terminate(stay withStopReason Shutdown)
    super.postStop()
  }

  private def terminate(nextState: State): Unit = {
    if (currentState.stopReason.isEmpty) {
      val reason = nextState.stopReason.get
      logTermination(reason)
      for (timer ← timers.values) timer.cancel()
      timers.clear()
      timeoutFuture.foreach {
        _.cancel()
      }
      currentState = nextState

      val stopEvent = StopEvent(reason, currentState.stateName, currentState.stateData)
      if (terminateEvent.isDefinedAt(stopEvent))
        terminateEvent(stopEvent)
    }
  }

  /**
   * By default [[FSM.Failure]] is logged at error level and other reason
   * types are not logged. It is possible to override this behavior.
   */
  protected def logTermination(reason: Reason): Unit = reason match {
    case Failure(ex: Throwable) ⇒ log.error(ex, "terminating due to Failure")
    case Failure(msg: AnyRef)   ⇒ log.error(msg.toString)
    case _                      ⇒
  }

  /*
     below this point modifications to test exit/entry
     */
  private def _onEnter(state: S): Unit = {
    entryMap.get(state) match {
      case Some(enterHandler) ⇒ enterHandler(nextStateData)
      case _                  ⇒ // no entry defined for this state
    }
  }

  private def _onExit(state: S): Unit = {
    exitMap.get(state) match {
      case Some(exitHandler) ⇒ exitHandler(stateData)
      case _                 ⇒ // no exit handler defined for this state
    }
  }

  def onEntry(state: S)(onEnterFn: PartialFunction[D, D]): Unit = {
    entryMap(state) = onEnterFn
  }

  def onExit(state: S)(onExitFn: PartialFunction[D, D]): Unit = {
    exitMap(state) = onExitFn
  }

  private val entryMap = new scala.collection.mutable.HashMap[S, PartialFunction[D, D]]()
  private val exitMap = new scala.collection.mutable.HashMap[S, PartialFunction[D, D]]()

}

