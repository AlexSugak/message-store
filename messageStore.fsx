open System;
open System.IO;
open System.Collections.Concurrent;

// types

type Result<'TSuccess, 'TError> = 
    | Success of 'TSuccess
    | Failure of 'TError

type Error = 
    | DirectoryNotFound of DirectoryInfo

type MessageId = MessageId of int
let idValue (MessageId mId) = mId

type Message = {
    Id: MessageId
    Body: string
}

type MessageInfo = MessageInfo of FileInfo

type LogLevel = 
    | Debug
    | Info 
    | Error

// stores

module FileStore = 
    let getInfo (dir: DirectoryInfo) (id: MessageId) = 
        MessageInfo (new FileInfo(Path.Combine(dir.FullName, sprintf "%A.txt" (idValue id))))

    let save dir msg = 
        let fi = getInfo dir msg.Id
        match dir.Exists, fi with
        | false, _               -> Failure (DirectoryNotFound dir)
        | true, MessageInfo info -> File.WriteAllText(info.FullName, msg.Body)
                                    Success ()

    let read dir id = 
        let fi = getInfo dir id
        match fi with
        | MessageInfo info when not info.Exists -> None
        | MessageInfo info                      -> let body = File.ReadAllText(info.FullName)
                                                   Some { Id = id; Body = body}

module StoreLogger =
    let save log writer msg =
        log Info (sprintf "Saving message %A." msg.Id)  
        let r = writer msg
        log Info (sprintf "Saved message %A." msg.Id)  
        r
    
    let read log reader id =
        log Debug (sprintf "Reading message %A." id)
        let msg = reader id
        match msg with
        | None   -> log Debug (sprintf "No message %A found." id)
        | Some _ -> log Debug (sprintf "Returning message %A." id)
        msg  

module StoreCache = 
    let save (cache: ConcurrentDictionary<MessageId, Message>) writer msg =
        let r = writer msg
        match r with 
        | Failure _ -> ()
        | Success _ -> cache.AddOrUpdate(msg.Id, msg, (fun i m -> msg)) |> ignore
        r

    let read (cache: ConcurrentDictionary<MessageId, Message>) reader id = 
        match cache.TryGetValue id with 
        | true, msg -> msg |> Some
        | false, _  -> reader id

// composition

let workingDir = new DirectoryInfo("C:\Temp")
let cache = new ConcurrentDictionary<MessageId, Message>()
let logToConsole level msg = printfn "%A: %s" level msg

let fileWriter = FileStore.save workingDir
let cacheWriter = StoreCache.save cache
let loggedWriter = StoreLogger.save logToConsole

let messageWriter = fileWriter |> cacheWriter |> loggedWriter 

let fileReader = FileStore.read workingDir
let cacheReader = StoreCache.read cache
let loggedReader = StoreLogger.read logToConsole

let messageReader = fileReader |> cacheReader |> loggedReader

// test
let id = MessageId 123
let msg = { Id = id; Body = "foo"}
let test = messageReader id
messageWriter msg |> ignore
let test2 = messageReader id