module Zhukov.Shutdown

open System
open Flux.Concurrency
open FSharpPlus
open Zhukov.Operators
open Hopac
open Hopac.Infixes

type ShutdownAgentMsg =
    | Register of string option * (unit -> Job<unit>) * Guid list * IVar<Guid>
    | Unregister of Guid

let private startShutdown onModShuttingDown onModShutDown data =
    let getOrCreateLatch jid latches =
        latches
        |> Map.tryFind jid
        |> Option.map (fun latch -> latches, latch)
        |> Option.defaultWith (fun () -> let latch = Latch 0 in Map.add jid latch latches, latch)

    let allConfigReady = IVar()

    let rec loop latches jobs thisConfigReady =
        function
        | (jid, (jname, jfun, jdeps)) :: ds ->
            let latches, latch = getOrCreateLatch jid latches
            let nextConfigReady = IVar()

            let job =
                nextConfigReady
                >>= IVar.fill thisConfigReady
                >>= fun () -> allConfigReady
                >>= fun () -> latch
#if RELEASE //this is here because for some unknown reason in DEBUG profile this causes a crash with InvalidCastException
                >>- fun () -> onModShuttingDown jname jid |> ignore
#endif
                >>= jfun
#if RELEASE
                >>- fun () -> onModShutDown jname jid |> ignore
#endif

            let latches, job =
                jdeps
                |> List.fold
                    (fun (latches, job) dep ->
                        let latches, latch = getOrCreateLatch dep latches
                        latches, Latch.holding latch job)
                    (latches, job)

            loop latches (job :: jobs) nextConfigReady ds
        | [] -> jobs, thisConfigReady

    let jobs, startConfig = loop Map.empty [] allConfigReady data

    Job.conIgnore [ Job.conIgnore jobs
                    IVar.fill startConfig () ]

let private shutdownAgent onShutdownReceived onMsgReceived onModShuttingDown onModShutDown takeMsg =
    let rec agentLoop data =
        takeMsg ()
        >>= function
            | Stop () ->
                onShutdownReceived ()
                startShutdown onModShuttingDown onModShutDown (Map.toList data)
            | Msg msg ->
                onMsgReceived msg

                match msg with
                | Register (jname, jfun, jdeps, ivar) ->
                    let jid = Guid.NewGuid()
                    let data = Map.add jid (jname, jfun, jdeps) data
                    IVar.fill ivar jid >>=. agentLoop data
                | Unregister j -> data |> Map.remove j |> agentLoop

    agentLoop Map.empty

let shutdownJob onShutdownReceived onMsgReceived onModShuttingDown onModShutDown =
    AgentMailboxStop.create
    <| shutdownAgent
        (Option.defaultValue ignore onShutdownReceived)
        (Option.defaultValue ignore onMsgReceived)
        (Option.defaultValue ignore2 onModShuttingDown)
        (Option.defaultValue ignore2 onModShutDown)
    >>- tap (fun x ->
                AppDomain.CurrentDomain.ProcessExit.Subscribe
                    (fun _ ->
                        AgentMailboxStop.stop x.Mailbox >>=. x.Stopped
                        |> run)
                |> ignore

                Console.CancelKeyPress.Subscribe
                    (fun _ ->
                        AgentMailboxStop.stop x.Mailbox >>=. x.Stopped
                        |> run)
                |> ignore)

let register jobId jobShutdown jobDependencies mailbox =
    AgentMailboxStop.sendAndAwaitReply mailbox (fun ivar -> Register(jobId, jobShutdown, jobDependencies, ivar))

let unregister jobId mailbox =
    AgentMailboxStop.send mailbox (Unregister jobId)
    >>- (konst jobId)
