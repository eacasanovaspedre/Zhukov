module Zhukov.Logging

open Logary
open Logary.Logger

module Shutdown =
    open Zhukov.Shutdown

    let name = nameof Zhukov.Shutdown

    let onShutdownReceived createLogger =
        let logger = createLogger name
        fun () ->
            Message.eventInfo "Starting shutdown"
            |> logSimple logger

    let onMsgReceived createLogger =
        let logger = createLogger name
        function
        | Register (name, _, dependencies, _) -> 
            Message.eventInfo $"Registering module {name} for graceful shutdown depending on {dependencies}" |> logSimple logger
        | Unregister name -> 
            Message.eventInfo $"Unregistering module {name} from graceful shutdown" |> logSimple logger

    let onModShuttingDown createLogger =
        let logger = createLogger name
        fun jname jid -> Message.eventInfo $"Module {jid} named {jname} is shutting down" |> logSimple logger

    let onModShutDown createLogger =
        let logger = createLogger name
        fun jname jid -> Message.eventInfo $"Module {jid} named {jname} has been shut down" |> logSimple logger