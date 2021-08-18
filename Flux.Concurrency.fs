namespace Flux.Concurrency

open Hopac
open Hopac.Infixes

[<Struct>]
type 'T AgentMailbox = private AgentMailbox of 'T Mailbox

[<Struct>]
type 'T AgentMailboxStop = private AgentMailboxStop of 'T Mailbox * unit IVar

type 'T MsgOrStop =
    | Stop of unit
    | Msg of 'T

module AgentMailbox =

    let create agent =
        let mailbox = Mailbox()
        let inline takeMsg () = Mailbox.take mailbox

        takeMsg |> agent |> Promise.start
        >>- fun stopped ->
                {| Mailbox = AgentMailbox mailbox
                   Stopped = stopped |}

    let send (AgentMailbox mailbox) msg = Mailbox.send mailbox msg

    let sendAndAwaitReply (AgentMailbox mailbox) msgBuilder =
        Alt.prepareJob
        <| fun _ ->
            let replyIVar = IVar()

            replyIVar |> msgBuilder |> Mailbox.send mailbox
            >>-. IVar.read replyIVar

module AgentMailboxStop =

    let create agent =
        let mailbox = Mailbox()
        let stopIVar = IVar()

        let inline takeMsg () =
            (Mailbox.take mailbox ^-> Msg)
            <|> (stopIVar ^-> Stop)

        takeMsg |> agent |> Promise.start
        >>- fun stopped ->
                {| Mailbox = AgentMailboxStop(mailbox, stopIVar)
                   Stopped = stopped |}

    let send (AgentMailboxStop (mailbox, _)) msg = Mailbox.send mailbox msg

    let sendAndAwaitReply (AgentMailboxStop (mailbox, _)) msgBuilder =
        Alt.prepareJob
        <| fun _ ->
            let replyIVar = IVar()

            replyIVar |> msgBuilder |> Mailbox.send mailbox
            >>-. IVar.read replyIVar

    let stop (AgentMailboxStop (_, stopIVar)) = IVar.tryFill stopIVar ()
