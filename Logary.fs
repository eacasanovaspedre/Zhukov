module Zhukov.Logary

open Logary.Configuration
open Logary
open Hopac.Hopac

let inline getLoggerFunc (logary: LogManager) = (System.Reflection.MethodBase.GetCurrentMethod()).Name |> logary.getLogger

let inline getLoggerLocalType (logary: LogManager) = (System.Reflection.MethodBase.GetCurrentMethod().DeclaringType).Name |> logary.getLogger

let runLogary () =
    let logary = 
        Config.create "Zhukov" "local"
        |> Config.target (Targets.LiterateConsole.create { Targets.LiterateConsole.empty with theme = Targets.LiterateConsole.Themes.defaultTheme } "nice console")
        |> Config.build
        |> run
    let logger = getLoggerLocalType logary
    Message.eventInfo "Logs configured"
    |> logger.logSimple
    logary

let createLoggerStr (name: string) = Log.create name

let createLoggerType (type': System.Type) = Log.create type'