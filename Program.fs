module Zhukov.Main

open Flux.Concurrency
open Logary
open Hopac
open Hopac.Infixes
open Zhukov.Broker
open Zhukov.Logging

[<EntryPoint>]
let main argv =
    let logary = runLogary ()

    let shutdownJob =
        Shutdown.shutdownJob
            (createLoggerStr
             |> Shutdown.onShutdownReceived
             |> Some)
            (createLoggerStr |> Shutdown.onMsgReceived |> Some)
            (createLoggerStr
             |> Shutdown.onModShuttingDown
             |> Some)
            (createLoggerStr |> Shutdown.onModShutDown |> Some)

    job {
        let! shutdown = shutdownJob

        let! server = Listener.listenForConnections ()

        let! consumer1 =
            Consumer.create
                (FsRandom.Utility.createRandomState ())
                {| Count = fun { Durable.Queue = q } -> Flux.Collections.Queue.count q
                   Offset = fun { Offset = o } -> o
                   Pop =
                       fun target { Queue = q; Offset = o } ->
                           let rec loop q o =
                               if o = target then
                                   { Durable.Queue = q
                                     Durable.Offset = o }
                               else
                                   loop (Flux.Collections.Queue.tail q) (o + offset 1)

                           loop q o
                   Push =
                       fun x q ->
                           { q with
                                 Durable.Queue = Flux.Collections.Queue.snoc x q.Queue }
                   ToSeq = fun { Durable.Queue = q; Offset = o } -> Flux.Collections.Queue.toSeq q |}
                (fun x -> Job.result ())

        let! consumerShutdownId =
            Shutdown.register (Some "consumer1") (fun () -> MailboxProcessorStop.stop consumer1.Mailbox >>=. consumer1.Stopped) [] shutdown.Mailbox

        let! poll1 = MailboxProcessorStop.sendAndAwaitReply consumer1.Mailbox (fun r -> Consumer.Action.Poll(5, 2, r))

        printfn "poll %A" poll1

        let k1 = MessageKey(Flux.Text.stringToBytesUTF8 "k1")
        let k2 = MessageKey(Flux.Text.stringToBytesUTF8 "k2")

        do!
            MailboxProcessorStop.send
                consumer1.Mailbox
                (Consumer.Action.AddQueue(
                    k1,
                    { Queue = Flux.Collections.Queue.empty
                      Offset = zeroOffset }
                ))

        do!
            MailboxProcessorStop.send
                consumer1.Mailbox
                (Consumer.Action.AddQueue(
                    k2,
                    { Queue = Flux.Collections.Queue.ofSeq ["k2m1"; "k2m2"; "k2m3"; "k2m4"; "k2m5"; "k2m6"; "k2m7"; "k2m8"; "k2m9"]
                      Offset = offset 1320 }
                ))

        do!
            MailboxProcessorStop.send
                consumer1.Mailbox
                (Consumer.Action.NewMessage(MessageKey(Flux.Text.stringToBytesUTF8 "k1"), "message1"))

        let! poll2 = MailboxProcessorStop.sendAndAwaitReply consumer1.Mailbox (fun r -> Consumer.Action.Poll(5, 2, r))

        printfn "poll %A" poll2

        let! ack1 = MailboxProcessorStop.sendAndAwaitReply consumer1.Mailbox (fun r -> Consumer.Action.Ack(k2, offset 1322, r))

        printfn "ack %A" ack1

        let! poll3 = MailboxProcessorStop.sendAndAwaitReply consumer1.Mailbox (fun r -> Consumer.Action.Poll(5, 2, r))

        printfn "poll %A" poll3

        let! serverId = Shutdown.register (Some "server") (fun () -> server.Stop true) [] shutdown.Mailbox

        do!
            server.Stopping
            >>= fun fromShutdown ->
                    if fromShutdown then
                        Job.result serverId
                    else
                        Shutdown.unregister serverId shutdown.Mailbox
            |> Job.startIgnore

        do! shutdown.Stopped
        do! logary.shutdown ()
        return 0
    }
    |> run
