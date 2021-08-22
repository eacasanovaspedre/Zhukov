module Zhukov.Broker.Durable

open Flux.Collections
open Flux.Concurrency
open Hopac
open Hopac.Infixes

type 'T Queue = { Queue: 'T Flux.Collections.Queue; Offset: Zhukov.Broker.Offset }  

type 'Client Action = AddClient of ClientId * 'Client

let private agent createConsumer durableId takeMsg =
    let rec loop (data: {| Consumers: Hamt<_, _> |}) =
        takeMsg ()
        >>= function
            | Stop () -> Job.result ()
            | Msg action ->
                match action with
                | AddClient (clientId, client) ->
                    createConsumer clientId client
                    >>= fun consumer ->
                            loop
                                {| data with
                                       Consumers = Hamt.add clientId consumer data.Consumers |}

    loop {| Consumers = Hamt.empty |}

let create createConsumer durableId =
    MailboxProcessorStop.create (agent createConsumer durableId)