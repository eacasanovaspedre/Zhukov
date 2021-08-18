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
                   Pop = fun target { Queue = q; Offset = o } ->
                            let rec loop q o =
                                if o = target then
                                    { Durable.Queue = q; Durable.Offset = o }
                                else
                                    loop (Flux.Collections.Queue.tail q) (o + offset 1)
                            loop q o
                   ToSeq = fun { Durable.Queue = q; Offset = o } -> Flux.Collections.Queue.toSeq q |}
                (fun x -> Job.result ())
                "clientId"
                ""
        
        let! consumerShutdownId = Shutdown.register (Some "consumer1") (fun () -> AgentMailboxStop.stop consumer1.Mailbox) [] shutdown.Mailbox
        
        //AgentMailboxStop.send()
        
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
