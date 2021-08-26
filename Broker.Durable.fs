module Zhukov.Broker.Durable

open Flux.Collections
open Flux.Concurrency
open Hopac
open Hopac.Infixes
open FSharpPlus.Lens
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

type private Data<'T, 'Consumer> =
    { Consumers: Hamt<ConsumerId, 'Consumer * MessageKey Set>
      ConsumerQueues: Hamt<MessageKey, ConsumerId>
      FreeQueues: Hamt<MessageKey, 'T Queue> }

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

    let inline addConsumer clientId consumer =
        over _Consumers (Hamt.add clientId (consumer, Set.empty))

let private agent (consumerOps: {| Create: _; AddMessage: _ |}) durableId takeMsg =
    let selfCh = Ch<_>()

    let rec loop (data: Data<'T, _>) =
        (Ch.take selfCh <|> takeMsg ())
        >>= function
            | Stop () -> Job.result ()
            | Msg action ->
                match action with
                | AddConsumer (consumerId, client) ->
                    consumerOps.Create consumerId client
                    >>= fun consumer -> data |> Data.addConsumer consumerId consumer |> loop
                | AddMessage (key, message: 'T) ->
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
                            let consumer, _ = view (_consumerForKey consumerId) data

                            consumerOps.AddMessage
                                consumer
                                key
                                message
                                (fun k m -> (k, m) |> AddMessage |> Msg |> Ch.send selfCh)
                            >>=. loop data
                        | None -> loop data

    loop
        { Consumers = Hamt.empty
          ConsumerQueues = Hamt.empty
          FreeQueues = Hamt.empty }

let create consumerOps durableId =
    MailboxProcessorStop.create (agent consumerOps durableId)
