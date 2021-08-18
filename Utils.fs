namespace Zhukov

module Operators =

    let inline ignore2 _ _ = ()

module Hamt =
    open Flux.Collections

    let modify k f h =
        let x = Hamt.find k h
        Hamt.add k (f x) h

    let chooseKey k f h =
        let x = Hamt.find k h

        x
        |> f
        |> Option.map (fun r -> Hamt.add k r h)
        |> Option.defaultWith (fun () -> Hamt.remove k h)

    let maybeModify k f h =
        h
        |> Hamt.maybeFind k
        |> Option.map (fun x -> Hamt.add k (f x) h)

    let maybeModify' k f h =
        h
        |> Hamt.maybeFind k
        |> Option.map (fun x -> Hamt.add k (f x) h)
        |> Option.defaultValue h

    let maybeModifyAndRet k f h =
        h
        |> Hamt.maybeFind k
        |> Option.map (fun x -> let v, r = f x in Hamt.add k v h, r)

    let maybeModifyAndRet' k f h =
        h
        |> Hamt.maybeFind k
        |> Option.map (fun x -> let v, r = f x in Hamt.add k v h, Some r)
        |> Option.defaultValue (h, None)
        
    let findAndRemove k h =
        let v = Hamt.find k h
        v, Hamt.remove k h

module Random =
    open Operators
    open FsRandom

    let random n state =
        Random.next (Statistics.uniformDiscrete (1, n)) state

    let sample rState need items =
        let isChosen state count need =
            let (r, rState) = (random count state) in r <= need, rState

        let count = Array.length items

        items
        |> Seq.scan
            (fun (rState, count, need, chosen) item ->
                let isChosen, rState = isChosen rState count need

                if isChosen then
                    rState, count - 1, need - 1, item :: chosen
                else
                    rState, count - 1, need, chosen)
            (rState, count, need, [])
        |> Seq.filter (fun (_, count, need, _) -> need = 0 || count = 0)
        |> Seq.head
        |> fun (rState, _, _, chosen) -> rState, chosen
