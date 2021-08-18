namespace Zhukov.Broker

open Flux
open Flux.Collections
open Flux.Concurrency
open FSharpPlus
open Hopac
open Hopac.Infixes
open NodaTime

[<Struct>]
type ClientId = ClientId of NEString

[<Struct>]
type ChannelId = ChannelId of NEString

[<Struct>]
type MessageKey = MessageKey of byte array

[<Struct>]
type GroupId = Group of NEString

[<Struct>]
type DurableId = DurableId of ChannelId * GroupId

[<Struct>]
type Offset = Offset of int64 with
    static member (+) (Offset a, Offset b) = Offset (a + b)
    static member Zero = Offset 0L

[<AutoOpen>]
module Offset =
    let zeroOffset = Offset.Zero
    let inline offset x = int64 x |> Offset

type Message =
    { Body: byte array
      ReceivedAt: Instant }

module Root =

    type Action<'Client> =
        | AddMessage of ChannelId * MessageKey * Message //TODO
        | NewClient of DurableId * ClientId * 'Client

    let private agent createDurable sendClientToDurable takeMsg =
        let rec loop (data: {| Durables: Hamt<_, _> |}) =
            takeMsg ()
            >>= function
                | Stop () -> Job.result ()
                | Msg action ->
                    match action with
                    | AddMessage (ch, key, msg) -> loop data
                    | NewClient (durableId, clientId, client) ->
                        Hamt.maybeFind durableId data.Durables
                        |> Option.map (fun durable -> Job.result (durable, data.Durables))
                        |> Option.defaultWith
                            (fun () ->
                                createDurable durableId
                                >>- (fun durable -> durable, Hamt.add durableId durable data.Durables))
                        >>= fun (durable, durables) ->
                                sendClientToDurable clientId client durable
                                >>-. {| data with Durables = durables |}
                        >>= loop

        loop {| Durables = Hamt.empty |}

    let create createDurable sendClientToDurable =
        AgentMailboxStop.create (agent createDurable sendClientToDurable)