module Zhukov.Broker.Durable

open Flux.Collections
open Flux.Concurrency
open Hopac
open Hopac.Infixes
open FSharpPlus
open FSharpPlus.Operators
open FSharpPlus.Lens
open Zhukov.FSharpPlusHopac
open Zhukov.Hamt.Lens

type 'T Queue =
    { Queue: 'T Flux.Collections.Queue
      Offset: Zhukov.Broker.Offset }

module Queue =

    let inline _Queue f q =
        f q.Queue <&> fun x -> { q with Queue = x }

    let inline _Offset f q =
        f q.Offset <&> fun x -> { q with Offset = x }

type Action<'Client, 'T> =
    | AddConsumer of ConsumerId * 'Client
    | AddMessage of MessageKey * 'T

type private Data<'T, 'ConsumerServer> =
    { Consumers: Hamt<ConsumerId, {| Server: 'ConsumerServer; AssignedKeys: MessageKey Set; Stopped: unit IVar |}>
      ConsumerQueues: Hamt<MessageKey, ConsumerId>
      FreeQueues: Hamt<MessageKey, 'T Queue>
      Stopping: bool }

module private Data =

    let inline _Consumers f c =
        f c.Consumers
        <&> fun x -> { c with Consumers = x }

    let inline _ConsumerQueues f d =
        f d.ConsumerQueues
        <&> fun x -> { d with ConsumerQueues = x }

    let inline _FreeQueues f d =
        f d.FreeQueues
        <&> fun x -> { d with FreeQueues = x }

    let inline addConsumer consumerId server consumerStopped =
        over _Consumers (Hamt.add consumerId {| Server = server; AssignedKeys = Set.empty; Stopped = consumerStopped |})

let private agent durableId createConsumer saveOffset (msgConsumer: {| AddMessage: _; Stop: _ |}) takeMsg =
    let selfCh = Ch<_>()
    let saveOffset = saveOffset durableId
    

    let rec loop (data: Data<_, _>) =
        Ch.take selfCh
        <|> takeMsg ()
        <|> (if data.Stopping then
                 Alt.always (Stop ())
             else
                 Alt.never ())
        >>= function
            | Stop _ when data.Stopping ->
                Job.result 1
            | Stop _ -> Job.result 1
                // data
                // |> view Data._Consumers
                // |> Hamt.toSeq
                // |> Seq.map (
                //     KVEntry.value
                //     >> fst
                //     >> msgConsumer.Stop
                // )
                // |> Job.conIgnore
            | Msg action ->
                match action with
                | AddConsumer (consumerId, client) ->
                    createConsumer consumerId client
                    >>= fun (consumer, stopped) ->
                            data
                            |> Data.addConsumer consumerId consumer stopped
                            |> loop
                | AddMessage (key, message) ->
                    let inline _freeQueueForKey f = ((_keyMaybe key) >> Data._FreeQueues) f

                    let inline _consumerQueueForKey f =
                        ((_keyMaybe key) >> Data._ConsumerQueues) f

                    let inline _consumerForKey k f = ((_key k) >> Data._Consumers) f

                    match view _freeQueueForKey data with
                    | Some queue ->
                        let queue =
                            over Queue._Queue (Queue.snoc message) queue

                        setl _freeQueueForKey (Some queue) data |> loop
                    | None ->
                        match view _consumerQueueForKey data with
                        | Some consumerId ->
                            let consumer = view (_consumerForKey consumerId) data

                            msgConsumer.AddMessage
                                consumer.Server
                                key
                                message
                                (fun k m -> (k, m) |> AddMessage |> Msg |> Ch.send selfCh)
                            >>=. loop data
                        | None -> loop data

    loop
        { Consumers = Hamt.empty
          ConsumerQueues = Hamt.empty
          FreeQueues = Hamt.empty
          Stopping = false }

let create durableId createConsumer saveOffset msgConsumer =
    MailboxProcessorStop.create (agent durableId createConsumer saveOffset msgConsumer)
