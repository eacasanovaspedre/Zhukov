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
open Zhukov.FSharpPlusHopac
open FSharpPlus.Lens
open FsRandom
open Zhukov.Random

type private 'DurableQueue Queue =
    { DurableQueue: 'DurableQueue }

module private Queue =
    let create q =
        { DurableQueue = q }

    let inline _DurableQueue f q =
        f q.DurableQueue
        <&> fun x -> { q with DurableQueue = x }

    let inline _PoppedDurableQueues f q =
        f q.PoppedDurableQueues
        <&> fun x -> { q with PoppedDurableQueues = x }

    let push durablePush x q = over _DurableQueue (durablePush x) q

    let deliver durableCount n q =
        let deliverCount = n </min/> (durableCount q) //experimenting with </ />
        let items = 


    let pop durablePop toOffset q = 

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
    | AddMessage of key: MessageKey * message: 'T * returnIt: (MessageKey -> 'T -> unit Job)

let private agent
    randomState
    (durableQueueOps: {| Count: _
                         ToSeq: _
                         Offset: _
                         Pop: _
                         Push: _ |})
    (msgParent: {| Return: _ |})
    sendShutdownToClient
    takeMsg
    =
    let inline durableQueueCount q =
        q |> view Queue._durableQueue |> durableQueueOps.Count

    let inline durableQueueOffset q =
        q |> view Queue._durableQueue |> durableQueueOps.Offset

    let inline durableQueueToSeq q =
        q |> view Queue._durableQueue |> durableQueueOps.ToSeq

    let inline durableQueuePop o q =
        over Queue._durableQueue (durableQueueOps.Pop o) q

    let inline durableQueuePush x q =
        over Queue._durableQueue (durableQueueOps.Push x) q

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
                |> map (fun (KVEntry (k, v)) -> msgParent.Return k (view Queue._durableQueue v))
                |> Job.conIgnore
            | Stop _ ->
                sendShutdownToClient ()
                >>-. {| data with Stopping = true |}
                >>= loop
            | Msg action ->
                match action with
                | Poll (conKeyMax, msgCountMax, replyCh) ->
                    let randomState, keysToSend =
                        data.Queues
                        |> Hamt.toSeqPairs
                        |> Seq.filter
                            (fun pair ->
                                view (_2 << Queue._deliveredCount) pair = zeroOffset
                                && view (_2 << Queue._durableQueue) pair |> durableQueueOps.Count > 0)
                        |> Seq.toArray
                        |> sample data.RandomState conKeyMax
                        |> (over
                                _2
                                (List.map
                                    (fun (KVEntry (k, q)) ->
                                        let deliverCount = min msgCountMax (durableQueueCount q)

                                        let items =
                                            q
                                            |> durableQueueToSeq
                                            |> Seq.truncate deliverCount
                                            |> Seq.toArray

                                        k, items, durableQueueOffset q)))

                    let queues =
                        keysToSend
                        |> Seq.fold
                            (fun qs (k, items, _) ->
                                Hamt.findAndSet
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
                    |> Hamt.maybeFind key
                    |> Option.map
                        (fun q ->
                            let currentOffset = durableQueueOffset q
                            let maxOffset = currentOffset + view _deliveredCount q

                            if currentOffset < offset && offset <= maxOffset then
                                let diff = offset - currentOffset

                                Hamt.add
                                    key
                                    (over _deliveredCount (fun c -> c - diff) (durableQueuePop offset q))
                                    data.Queues,
                                Ok offset
                            else
                                data.Queues,
                                {| MaxOffset = maxOffset
                                   MinOffset = currentOffset
                                   RequestedOffset = offset |}
                                |> CouldNotAck.OffsetOutOfRange
                                |> Error)
                    |> Option.defaultWith (fun () -> data.Queues, key |> CouldNotAck.KeyNotFound |> Error)
                    |> fun (qs, r) ->
                        r |> IVar.fill replyCh
                        >>=. Result.either
                                 (fun _ ->
                                     if Hamt.count qs > data.QueueWantedCount then
                                         qs
                                         |> Hamt.findAndRemove key
                                         |> fun (q, qs') ->
                                             q |> view _durableQueue |> msgParent.Return key
                                             >>-. qs'
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
                               |> Hamt.add key (create queue zeroOffset)
                           QueueWantedCount = data.QueueWantedCount + 1 |}
                    |> loop
                | AddMessage (key, message, returnIt) ->
                    data.Queues
                    |> Hamt.maybeFindAndSet key (durableQueuePush message)
                    |> Option.map (fun qs -> Job.result {| data with Queues = qs |})
                    |> Option.defaultValue (returnIt key message >>-. data)
                    >>= loop

    loop
        {| Queues = Hamt.empty
           QueueWantedCount = 0
           RandomState = randomState
           Stopping = false |}

let create randomState durableQueueOps msgParent sendShutdownToClient =
    MailboxProcessorStop.create (agent randomState durableQueueOps msgParent sendShutdownToClient)
