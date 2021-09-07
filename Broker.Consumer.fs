module Zhukov.Broker.Consumer

open Flux
open Flux.Collections
open Flux.Concurrency
open Hopac
open Hopac.Infixes
open FSharpPlus
open FSharpPlus.Operators
open FSharpPlus.Lens
open Zhukov.FSharpPlusHopac
open FSharpPlus.Lens
open FsRandom
open Zhukov.Random

type private 'DurableQueueActor Queue =
    { DurableQueueActor: 'DurableQueueActor }

module private Queue =
    let create q = { DurableQueueActor = q }

    let inline _DurableQueueActor f q =
        f q.DurableQueueActor
        <&> fun x -> { q with DurableQueueActor = x }

    let headN headNDurable n q =
        q |> view _DurableQueueActor |> headNDurable n

    let maybeMoveOffset maybeMoveOffsetDurable offset q =
        q
        |> view _DurableQueueActor
        |> maybeMoveOffsetDurable offset
        >>- Option.map (fun qs -> setl _DurableQueueActor qs q)

    let getOffset offsetDurable q =
        q |> view _DurableQueueActor |> offsetDurable

type CouldNotAck =
    | KeyNotFound of MessageKey
    | OffsetOutOfRange of {| RequestedOffset: Offset |}

type Action<'T, 'Queue> =
    | Poll of conKeyMax: int * msgCountMax: int * replyCh: (MessageKey * 'T Collections.Queue * Offset) list IVar
    | Ack of key: MessageKey * offset: Offset * replyCh: Result<Offset, CouldNotAck> IVar
    | SetWantedCount of count: int
    | AddQueue of key: MessageKey * queue: 'Queue

module PollKeyChoosingStrategy =

    let roundRobin headN randomState maxKeys checkTimeout pairs =
        let timeoutAlt =
            (checkTimeout
             |> Option.map (timeOut)
             |> Option.defaultWith Alt.zero)
            ^-> (fun () -> None)

        let latch = Latch(maxKeys)

        pairs
        |> Seq.map
            (fun (key, queue) ->
                Alt.choose [ headN 1 queue
                             >>- fun q ->
                                     (if Queue.isEmpty q then
                                          None
                                      else
                                          Some(key, queue))
                                     |> Alt.always
                             |> Alt.prepare
                             timeoutAlt
                             Latch.await latch ^-> (fun _ -> None) ]
                >>= (function
                | Some pair -> Latch.decrement latch >>-. Some pair
                | None -> Job.result None))
        |> Job.conCollect
        >>- (Seq.choose id >> Seq.toArray)
        >>- (sample randomState maxKeys)


let private agent
    randomState
    (durableQueueOps: {| GetOffset: _
                         HeadN: _
                         MaybeMoveOffset: _ |})
    (msgParent: {| Return: _ |})
    sendShutdownToClient
    takeMsg
    =

    let inline headN n q = Queue.headN durableQueueOps.HeadN n q

    let inline getOffset q =
        Queue.getOffset durableQueueOps.GetOffset q

    let inline maybeMoveOffset toOffset q =
        Queue.maybeMoveOffset durableQueueOps.MaybeMoveOffset toOffset q

    let rec loop
        (data: {| Queues: _
                  QueueWantedCount: _
                  RandomState: _
                  Stopping: _ |})
        =
        takeMsg ()
        <|> (if data.Stopping then
                 Alt.always (Stop())
             else
                 Alt.never ())
        >>= function
            | Stop _ when data.Stopping ->
                data.Queues
                |> Hamt.toSeq
                |> map (fun (KVEntry (k, v)) -> msgParent.Return k (view Queue._DurableQueueActor v))
                |> Job.conIgnore
            | Stop _ ->
                sendShutdownToClient ()
                >>-. {| data with Stopping = true |}
                >>= loop
            | Msg action ->
                match action with
                | Poll (conKeyMax, msgCountMax, replyCh) ->
                    data.Queues
                    |> Hamt.toSeqPairs
                    |> PollKeyChoosingStrategy.roundRobin
                        headN
                        data.RandomState
                        conKeyMax
                        (1000
                         |> float
                         |> System.TimeSpan.FromMilliseconds
                         |> Some)
                    >>= fun (randomState, pairs) ->
                            pairs
                            |> Seq.map
                                (fun (key, queue) ->
                                    (headN msgCountMax queue
                                     >>- fun items -> key, items, getOffset queue))
                            |> Job.conCollect
                            >>- fun results -> randomState, results |> Seq.toList
                    >>= fun (randomState, results) ->
                            IVar.fill replyCh results
                            >>-. {| data with
                                        RandomState = randomState |}
                    >>= loop
                | Ack (key, requestedOffset, replyCh) ->
                    data.Queues
                    |> Hamt.maybeFind key
                    |> Option.map
                        (fun q ->
                            let currentOffset = getOffset q

                            let diff = requestedOffset - currentOffset

                            if diff > zeroOffset && diff < offset 20 then
                                maybeMoveOffset requestedOffset q
                                >>- (Option.map (fun q' -> Hamt.add key q' data.Queues, q' |> getOffset |> Ok)
                                     >> Option.defaultWith
                                         (fun () ->
                                             data.Queues,
                                             {| RequestedOffset = requestedOffset |}
                                             |> CouldNotAck.OffsetOutOfRange
                                             |> Error))
                            else
                                Job.result (
                                    data.Queues,
                                    {| RequestedOffset = requestedOffset |}
                                    |> CouldNotAck.OffsetOutOfRange
                                    |> Error
                                ))
                    |> Option.defaultWith (fun () -> Job.result (data.Queues, key |> CouldNotAck.KeyNotFound |> Error))
                    >>= fun (qs, r) ->
                            IVar.fill replyCh r
                            >>=. loop {| data with Queues = qs |}
                | SetWantedCount count -> loop {| data with QueueWantedCount = count |}
                | AddQueue (key, queue) ->
                    {| data with
                           Queues = data.Queues |> Hamt.add key (Queue.create queue)
                           QueueWantedCount = data.QueueWantedCount + 1 |}
                    |> loop

    loop
        {| Queues = Hamt.empty
           QueueWantedCount = 0
           RandomState = randomState
           Stopping = false |}

let create randomState durableQueueOps msgParent sendShutdownToClient =
    MailboxProcessorStop.create (agent randomState durableQueueOps msgParent sendShutdownToClient)
