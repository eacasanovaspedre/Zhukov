module Zhukov.Broker.Consumer

open Aether
open Flux
open Flux.Collections
open Flux.Concurrency
open FSharpPlus
open Hopac
open Hopac.Infixes
open FSharpPlus.Lens
open FsRandom
open Zhukov
open Zhukov.Random

type private 'Queue Queue =
    { Queue: 'Queue
      DeliveredCount: Offset }

[<AutoOpen>]
module private Queue =
    let create q d = { Queue = q; DeliveredCount = d }

    let inline _queue f q =
        f q.Queue <&> fun x -> { q with Queue = x }
        
    let inline _deliveredCount f q =
        f q.DeliveredCount
        <&> fun x -> { q with DeliveredCount = x }
        
type CouldNotAck =
    | KeyNotFound of MessageKey
    | OffsetOutOfRange of
        {| MaxOffset: Offset
           MinOffset: Offset
           RequestedOffset: Offset |}

type Action<'T, 'Queue> =
    | Poll of conKeyMax: int * msgCountMax: int * replyCh: (MessageKey * 'T array * Offset) list IVar
    | Ack of key: MessageKey * offset: Offset * replyCh: Result<Offset, CouldNotAck> IVar
    | SetWantedCount of count: int
    | AddQueue of key: MessageKey * queue: 'Queue

let private getQueue { Queue = q } = q

let private setQueue v q = { q with Queue = v }

let private getDeliveredCount { DeliveredCount = d } = d

let private agent
    randomState
    (queueOps: {| Count: _
                  ToSeq: _
                  Offset: _
                  Pop: _ |})
    releaseQueue
    clientId
    client
    takeMsg
    =
    let inline qCount q = q |> view _queue |> queueOps.Count
    let inline qOffset q = q |> view _queue |> queueOps.Offset
    let inline qToSeq q = q |> view _queue |> queueOps.ToSeq

    let rec loop
        (data: {| Queues: _
                  QueueWantedCount: _
                  RandomState: _ |})
        =
        takeMsg ()
        >>= function
            | Stop () -> Job.result ()
            | Msg action ->
                match action with
                | Poll (conKeyMax, msgCountMax, replyCh) ->
                    let randomState, keysToSend =
                        data.Queues
                        |> Hamt.toSeq
                        |> Seq.filter
                            (fun (KVEntry (_, q)) ->
                                view _deliveredCount q = zeroOffset
                                && qCount q > 0)
                        |> Seq.toArray
                        |> sample data.RandomState conKeyMax
                        |> (_2
                            %-> List.map
                                    (fun (KVEntry (k, q)) ->
                                        let deliverCount = min msgCountMax (qCount q)

                                        let items =
                                            q
                                            |> qToSeq
                                            |> Seq.truncate deliverCount
                                            |> Seq.toArray

                                        k, items, qOffset q))

                    let queues =
                        keysToSend
                        |> Seq.fold
                            (fun qs (k, items, _) ->
                                Hamt.modify
                                    k
                                    (items
                                     |> Array.length
                                     |> offset
                                     |> setl _deliveredCount)
                                    qs)
                            data.Queues

                    IVar.fill replyCh keysToSend
                    >>=. loop
                             {| data with
                                    Queues = queues
                                    RandomState = randomState |}
                | Ack (key, offset, replyCh) ->
                    data.Queues
                    |> Hamt.maybeModifyAndRet'
                        key
                        (fun q ->
                            let currentOffset = qOffset q
                            let maxOffset = currentOffset + view _deliveredCount q

                            if offset <= maxOffset then
                                { Queue = queueOps.Pop offset q.Queue
                                  DeliveredCount = zeroOffset },
                                Ok offset
                            else
                                q,
                                {| MaxOffset = maxOffset
                                   MinOffset = currentOffset
                                   RequestedOffset = offset |}
                                |> OffsetOutOfRange
                                |> Error)
                    |> fun (qs, r) -> qs, Option.defaultWith (fun () -> key |> CouldNotAck.KeyNotFound |> Error) r
                    |> fun (qs, r) ->
                        r |> IVar.fill replyCh
                        >>=. Result.either
                                 (fun _ ->
                                     if Hamt.count qs > data.QueueWantedCount then
                                         qs
                                         |> Hamt.findAndRemove key
                                         |> fun (q, qs') -> q |> view _queue |> releaseQueue >>-. qs'
                                     else
                                         Job.result qs)
                                 (fun _ -> Job.result qs)
                                 r
                        >>= fun qs' -> loop {| data with Queues = qs' |}
                | SetWantedCount count -> loop {| data with QueueWantedCount = count |}
                | AddQueue (key, queue) ->
                    {| data with
                           Queues =
                               data.Queues
                               |> Hamt.add key (Queue.create queue zeroOffset)
                           QueueWantedCount = data.QueueWantedCount + 1 |}
                    |> loop

    loop
        {| Queues = Hamt.empty
           QueueWantedCount = 0
           RandomState = randomState |}

let create randomState queueOps releaseQueue clientId client =
    AgentMailboxStop.create (agent randomState queueOps releaseQueue clientId client)
