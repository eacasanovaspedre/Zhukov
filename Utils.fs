namespace Zhukov

module Operators =

    let inline ignore2 _ _ = ()

module Hamt =
    open FSharpPlus
    open FSharpPlus.Lens
    open Flux.Collections.Hamt.Lens

    module Lens =

        let inline _key k f = let g, s = _key k in lens g (flip s) f

        let inline _keyMaybe k f = let g, s = _keyMaybe k in lens g (flip s) f

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
