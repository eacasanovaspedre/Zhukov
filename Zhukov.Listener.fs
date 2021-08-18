module Zhukov.Listener

open System
open System.Net.Sockets
open System.Net
open FSharpPlus
open Hopac
open Hopac.Infixes
open FSharp.Core

let rec private listeningLoop (socket: Socket) conns =
    Job.fromBeginEnd
        socket.BeginAccept
        (fun x ->
            try
                socket.EndAccept x |> Ok
            with
            | :? ObjectDisposedException as e -> Error e
            | _ -> reraise ())
    >>= Result.either
            (fun rSocket ->
                Ch.send conns rSocket
                >>=. listeningLoop socket conns)
            (fun _ -> Job.unit ())

let safeShutdown (socket: Socket) =
    try
        if socket.Connected then
            socket.Shutdown SocketShutdown.Both
    with
    | :? SocketException as e ->
        if e.SocketErrorCode <> SocketError.NotConnected then
            reraise ()
    | _ -> reraise ()

let listenForConnections () =
    let conns = Ch()
    let stop = IVar()
    let stopping = IVar()

    let socketJob =
        job {
            use socket =
                new Socket (SocketType.Stream, ProtocolType.Tcp)

            socket.Bind (IPEndPoint (IPAddress.Loopback, 9191))
            socket.Listen 250

            do!
                listeningLoop socket conns
                >>- fun () -> printfn "Listening finished"
                |> Job.start

            let! stopCode = stop
            do! IVar.fill stopping stopCode
            safeShutdown socket
            printfn "Closing socket"
            socket.Close()
        }

    socketJob |> Job.start
    >>-. {| ConnectionsChannel = conns
            Stop = IVar.tryFill stop
            Stopping = stopping |}
