open Core.Std
open Core_extended

let dup_stdin () = Unix.File_descr.to_int (Unix.dup Unix.stdin)

let is_unix () =
  match Unix.getsockname Unix.stdin with
    | Unix.ADDR_UNIX _ -> true
    | Unix.ADDR_INET _ -> false

open Async.Std

module Constants = struct
  (* env variables *)
  let fcgi_web_server_addrs = "FCGI_WEB_SERVER_ADDRS"

  (* listening socket file number *)
  let fcgi_listensock_fileno = 0

  (* number of bytes in a fcgi_header.  future versions of the protocol
     will not reduce this number. *)
  let fcgi_header_len = 8

  (* value for version component of fcgi_header *)
  let fcgi_version = 1

  (* values for type component of fcgi_header *)
  let fcgi_begin_request = 1
  let fcgi_abort_request = 2
  let fcgi_end_request = 3
  let fcgi_params = 4
  let fcgi_stdin = 5
  let fcgi_stdout = 6
  let fcgi_stderr = 7
  let fcgi_data = 8
  let fcgi_get_values = 9
  let fcgi_get_values_result = 10
  let fcgi_unknown_type = 11
  let fcgi_maxtype = fcgi_unknown_type

  (* value for requestid component of fcgi_header *)
  let fcgi_null_request_id = 0

  (* mask for flags component of fcgi_beginrequestbody *)
  let fcgi_keep_conn = 1

  (* values for role component of fcgi_beginrequestbody *)
  let fcgi_responder = 1
  let fcgi_authorizer = 2
  let fcgi_filter = 3

  (* values for protocolstatus component of fcgi_endrequestbody *)
  let fcgi_request_complete = 0
  let fcgi_cant_mpx_conn = 1
  let fcgi_overloaded = 2
  let fcgi_unknown_role = 3

  (* Variable names for FCGI_GET_VALUES / FCGI_GET_VALUES_RESULT records *)
  let fcgi_max_conns = "FCGI_MAX_CONNS"
  let fcgi_max_reqs =  "FCGI_MAX_REQS"
  let fcgi_mpxs_conns = "FCGI_MPXS_CONNS"
end

module Logging = struct
  type level = Quiet | Verbose | Debug

  type t = {
    level: level;
    logging_function: string -> unit;
  }

  let create ?(level=Quiet) ?(logging_function=(fun s -> Syslog.syslog s)) () =
    { level; logging_function; }

  let logger = ref (create ())

  let set_logger logger' =
    logger := logger'

  let log level' message =
    let { level; logging_function; } = !logger in
    if level = level' then
      logging_function message
    else
      ()

  let log_request level request_id message =
    log level ("[Request: " ^ (Int.to_string request_id) ^ "] " ^ message)
end

open Logging

module Configuration = struct
  type connection = Passive | TcpServer of string * int

  type t = {
    connection: connection;
    max_connections: int;
  }

  let create ?(max_connections=100) ~connection () =
    { connection; max_connections; }
end

module Record = struct
  type t = {
    version: int;
    type_: int;
    request_id: int;
    contents: string;
  }

  let read_int buffer from n_bytes =
    let rec loop i acc =
      if i = n_bytes then
        acc
      else
        loop (i + 1)  (acc + ((Char.to_int (buffer.[from + i])) lsl ((n_bytes - 1 - i) * 8)))
    in
    loop 0 0

  let write_int buffer n_bytes value =
    let rec loop i =
      if i = n_bytes then
        ()
      else
        (Buffer.add_char buffer (Char.of_int_exn ((value lsr ((n_bytes - 1 - i) * 8)) land 0xFF));
         loop (i + 1))
    in
    loop 0

  let read_record reader : t option Deferred.t =
    let buffer = String.make 8 '\000' in
    Reader.really_read reader ~len:8 buffer
    >>= function
      | `Eof _ -> return None
      | `Ok ->
        let version = read_int buffer 0 1 in
        let type_ = read_int buffer 1 1 in
        let request_id = read_int buffer 2 2 in
        let content_len = read_int buffer 4 2 in
        let padding_len = read_int buffer 6 1 in
        (* TODO: check for length to avoid allocating too much memory? *)
        let content_buffer = String.make (content_len + padding_len) '\000' in
        log_request Debug request_id ("Reading record of type: " ^ (Int.to_string type_));
        Reader.really_read reader ~len:(content_len + padding_len) content_buffer
        >>| function
          | `Eof _ -> None
          | `Ok -> Some {
            version;
            type_;
            request_id;
            contents=(String.sub content_buffer ~pos:0 ~len:content_len);
          }

  let write_record writer { version; type_; request_id; contents } =
    let buffer = Buffer.create (8 + (String.length contents)) in
    write_int buffer 1 version;
    write_int buffer 1 type_;
    write_int buffer 2 request_id;
    write_int buffer 2 (String.length contents);
    write_int buffer 2 0; (* 0 padding and empty reserved byte *)
    Buffer.add_string buffer contents;
    log_request Debug request_id ("writing record: " ^ (Buffer.contents buffer));
    Writer.write writer (Buffer.contents buffer)

  let decode_pair buffer from =
    let read_len from =
      if String.length buffer < from + 1 then
        None
      else
        let x = read_int buffer from 1 in
        if (x lsr 7) = 0 then
          (* single byte *)
          Some (x, from + 1)
        else
          if String.length buffer < from + 4 then
            None
          else
            Some ((read_int buffer from 4) land 0x7fffffff, from + 4)
    in
    let open Option.Monad_infix in
    read_len from
    >>= fun (name_len, from) ->
    read_len from
    >>= fun (value_len, from) ->
    if String.length buffer < from + name_len + value_len then
      None
    else
      let name = String.sub buffer ~pos:from ~len:name_len in
      let value = String.sub buffer ~pos:(from + name_len) ~len:value_len in
      Some ((name, value), from + name_len + value_len)

  let decode_pairs buffer =
    let open Option.Monad_infix in
    let rec loop alist from =
      if from < String.length buffer then
        decode_pair buffer from
        >>= fun (pair, from) ->
        loop (pair::alist) from
      else (
        Some (String.Map.of_alist_exn alist))
    in
    loop [] 0

  let encode_pair name value =
    let name_len = String.length name in
    let value_len = String.length value in
    let name_n_bytes = if name_len <= 127 then 1 else 4 in
    let value_n_bytes = if value_len <= 127 then 1 else 4 in
    let buffer = Buffer.create (name_n_bytes + value_n_bytes + name_len + value_len) in
    write_int buffer name_n_bytes name_len;
    write_int buffer value_n_bytes value_len;
    Buffer.add_string buffer name;
    Buffer.add_string buffer value;
    buffer

  let encode_pairs pairs =
    let buffer = Buffer.create 0 in
    Map.iter ~f:(fun ~key ~data -> Buffer.add_buffer buffer (encode_pair key data)) pairs;
    Buffer.contents buffer

  let encode_end_request_contents app_status protocol_status =
    let buffer = Buffer.create 8 in
    write_int buffer 4 app_status;
    write_int buffer 1 protocol_status;
    write_int buffer 3 0; (* reserved bytes *)
    Buffer.contents buffer

  type begin_request_contents = { role: int; flags: int; }

  let decode_begin_request_contents contents =
    if String.length contents < 3 then
      (log Verbose "begin request record contents too short";
       None)
    else
      Some { role=read_int contents 0 2;
             flags=read_int contents 2 1; }
end

module Request = struct
  type status = Created | Active | Closed

  module Role = struct
    type t = Responder | Authorizer | Filter

    let of_int n =
      if n = Constants.fcgi_responder then
        Some Responder
      else if n = Constants.fcgi_authorizer then
        Some Authorizer
      else if n = Constants.fcgi_filter then
        Some Filter
      else
        None
  end

  type t = {
    request_id: int;
    role: Role.t;
    params: string;
    stdin: string Pipe.Reader.t;
    stdin_writer: string Pipe.Writer.t;
    stdout: string Pipe.Writer.t;
    stderr: string Pipe.Writer.t;
    data: string Pipe.Reader.t;
    data_writer: string Pipe.Writer.t;
    mutable status: status;
    abort: unit Ivar.t;
    close_connection: bool;
  }

  let close ({ request_id; stdout; stderr; data_writer; stdin_writer; status; _ } as request)
      app_status transport_writer transport_close =
    if status = Closed then
      ()
    else
      begin
        Record.write_record transport_writer {
          version=Constants.fcgi_version;
          type_=Constants.fcgi_end_request;
          request_id;
          contents=Record.encode_end_request_contents app_status Constants.fcgi_request_complete;
        };
        Pipe.close stdout;
        Pipe.close stderr;
        Pipe.close data_writer;
        Pipe.close stdin_writer;
        transport_close ();
        request.status <- Closed
      end

  let create request_id role transport_writer close_connection =
    let (stdin_reader, stdin_writer) = Pipe.create () in
    let (stdout_reader, stdout_writer) = Pipe.create () in
    let (stderr_reader, stderr_writer) = Pipe.create () in
    let (data_reader, data_writer) = Pipe.create () in
    let request = {
      request_id;
      role;
      params="";
      status=Created;
      abort=Ivar.create ();
      stdin=stdin_reader;
      stdin_writer=stdin_writer;
      data=data_reader;
      data_writer=data_writer;
      stdout=stdout_writer;
      stderr=stderr_writer;
      close_connection;
    } in
    let rec output_loop pipe_reader type_ =
      Pipe.read pipe_reader
      >>> function
        | `Eof -> ()
        | `Ok contents ->
          log_request Debug request_id ("Sending response: " ^ contents);
          Record.write_record transport_writer {
            version=Constants.fcgi_version;
            type_;
            request_id;
            contents;
          };
          Writer.flushed transport_writer
          >>> fun () ->
          output_loop pipe_reader type_
    in
    output_loop stdout_reader Constants.fcgi_stdout;
    output_loop stderr_reader Constants.fcgi_stderr;
    request

    let activate request app transport_writer transport_close =
      match request with
        | { request_id; status=Created; _ } ->
          (match Record.decode_pairs request.params with
             | None ->
               log_request Verbose request_id "Can't parse request parameters"
             | Some decoded_params ->
               (* launch the app *)
               (let request = {request with status=Active} in
                log_request Debug request_id "Launching application";
                try_with (fun () -> app request decoded_params)
                >>> function
                  | Ok status -> close request status transport_writer transport_close
                  | Error e ->
                    let e_str = Exn.to_string e in
                    log_request Verbose request_id e_str;
                    Pipe.write request.stderr e_str
                    >>> fun () ->
                    close request 500 transport_writer transport_close));
          request
        | _ -> request
end

module ActiveRequests = struct
  type t = Table of Request.t Int.Table.t | ArrayTable of Request.t option array

  let create () = ref (ArrayTable (Array.create ~len:100 None))

  let add active_requests ({ Request.request_id; _ } as request) =
    active_requests := match !active_requests with
      | ArrayTable a ->
        if request_id >= 0 && request_id < Array.length a then
          (a.(request_id) <- Some request;
           !active_requests)
        else
          (let active_requests' = Int.Table.create () in
           Array.iteri ~f:(fun i request_opt ->
             match request_opt with
               | None -> ()
               | Some request ->
                 Hashtbl.set active_requests' ~key:i ~data:request)
             a;
           Table active_requests')
      | Table t ->
        Hashtbl.set t ~key:request_id ~data:request;
        !active_requests

  let remove active_requests request_id =
    match !active_requests with
      | ArrayTable a ->
        if request_id >= 0 && request_id < Array.length a then
          a.(request_id) <- None
        else
          ()
      | Table t ->
        Hashtbl.remove t request_id

  let find active_requests request_id =
    match !active_requests with
      | ArrayTable a ->
        if request_id >= 0 && request_id < Array.length a then
          a.(request_id)
        else
          None
      | Table t ->
        Hashtbl.find t request_id
end

module Transport = struct
  type t = {
    app: Request.t -> string String.Map.t -> int Deferred.t;
    active_requests: ActiveRequests.t ref;
    (* socket reader and writer *)
    reader: Reader.t;
    writer: Writer.t;
  }

  let close { reader; writer; _ } =
    Writer.close writer ~force_close:(Clock.after (sec 30.))
    >>> fun () ->
    don't_wait_for (Reader.close reader)

  let handle_begin_request conf ({ active_requests; writer; _ } as t) { Record.request_id; contents; _ } =
    log_request Debug request_id "Begin request";
    match ActiveRequests.find active_requests request_id with
      | Some _ -> log_request Verbose request_id "trying to begin an already existing request"
      | None ->
        match Record.decode_begin_request_contents contents with
          | None -> log_request Verbose request_id "can't decode begin request record"
          | Some { Record.role=role; flags=flags; } ->
            let close_connection = (flags land Constants.fcgi_keep_conn) = 0 in
            log_request Debug request_id
              ("Role: " ^ (Int.to_string role) ^ ", Flags: " ^ (Int.to_string flags));
            match Request.Role.of_int role with
              | None ->
                log_request Verbose request_id ("Unkown role: " ^ (Int.to_string role));
                Record.write_record writer {
                  version=Constants.fcgi_version;
                  type_=Constants.fcgi_unknown_role;
                  request_id;
                  contents="";
                };
                if close_connection then
                  close t
                else
                  ()
              | Some role ->
                ActiveRequests.add active_requests (Request.create request_id role writer close_connection)

  let update_request { active_requests; _ } { Record.request_id; _ } f =
    match ActiveRequests.find active_requests request_id with
      | None -> ()
      | Some request -> ActiveRequests.add active_requests (f request)

  let handle_abort_request conf t record =
    update_request t record
      (fun ({ Request.abort; _ } as request) ->
        (* ask the application to abort and return a status code *)
        Ivar.fill abort ();
        request)

  let handle_params conf t ({ Record.contents; _ } as record) =
    update_request t record
      (fun request ->
        {request with Request.params=(request.Request.params ^ contents)})

  let handle_input get_stream conf ({ app; active_requests; writer; _ } as t) (({ request_id; contents; _ } as record) : Record.t) =
    update_request t record
      (fun request ->
        let stream_writer = get_stream request in
        (if String.length contents > 0 then
            Pipe.write_without_pushback stream_writer contents
         else
            (* a 0 length means end of stream *)
            Pipe.close stream_writer);
        let transport_close () =
          ActiveRequests.remove active_requests request_id;
          if request.Request.close_connection then
            close t
          else
            ()
        in
        Request.activate request app writer transport_close)

  let handle_stdin = handle_input (fun { stdin_writer; _ } -> stdin_writer)

  let handle_data = handle_input (fun { data_writer; _ } -> data_writer)

  let handle_get_values { Configuration.max_connections; _ } { writer; _ }
      { Record.request_id; contents; _ } =
    let fcgi_options = String.Map.of_alist_exn [
      (Constants.fcgi_max_conns, (Int.to_string max_connections));
      (Constants.fcgi_max_reqs, (Int.to_string max_connections));
      (Constants.fcgi_mpxs_conns, "1");
    ] in
    if request_id <> Constants.fcgi_null_request_id then
      log_request Verbose request_id "fcgi_get_values can only be called with a null request id"
    else
      match Record.decode_pairs contents with
        | None -> log_request Verbose request_id "Can't decode fcgi_get_value record"
        | Some name_value_pairs ->
          Record.write_record
            writer
            {
              version=Constants.fcgi_version;
              type_=Constants.fcgi_get_values_result;
              request_id=Constants.fcgi_null_request_id;
              contents=Record.encode_pairs (Map.filter ~f:(fun ~key ~data ->
                match Map.find name_value_pairs key with
                  | None -> false
                  | Some _ -> true
              ) fcgi_options);
            }

  let handle_unknown_type conf { writer; _ } record =
    Record.write_record writer
      {
        version=Constants.fcgi_version;
        type_=Constants.fcgi_unknown_type;
        request_id=Constants.fcgi_null_request_id;
        contents="";
      }

  let type_dispatch_table = [| handle_begin_request;
                               handle_abort_request;
                               handle_unknown_type; (* end request *)
                               handle_params;
                               handle_stdin;
                               handle_unknown_type; (* stdout *)
                               handle_unknown_type; (* stderr *)
                               handle_data;
                               handle_get_values;
                               handle_unknown_type; (* get value result *) |]

  let dispatch_record conf t ({ Record.type_; _ } as record) =
    let dispatch_index = type_ - 1 in
    if dispatch_index >= Array.length type_dispatch_table || dispatch_index < 0 then
       handle_unknown_type conf t record
    else
      type_dispatch_table.(dispatch_index) conf t record

  let dispatch_loop conf app reader writer =
    let t = { app; active_requests=ActiveRequests.create (); reader; writer; } in
    let rec loop () =
      Record.read_record reader
      >>= function
        | None ->
          log Debug "transport connection closed";
          (* call close again, in case the connection was closed by the remote host *)
          return (close t)
        | Some record ->
          dispatch_record conf t record;
          loop ()
    in
    loop ()
end

let is_valid address =
  match Sys.getenv Constants.fcgi_web_server_addrs with
    | None -> true (* no restrictions *)
    | Some ips -> let parts = String.split ~on:':' (Socket.Address.to_string address) in
                  let open Option.Monad_infix in
                      match List.nth parts 0
                      >>= fun ip -> List.find (String.split ~on:',' ips) ~f:((=) ip)
                      with
                        | None -> false
                        | Some _ -> true

let rec accept_client conf socket app =
  Socket.accept socket
  >>> function
    | `Socket_closed -> log Debug "main socket closed"
    | `Ok (client_socket, client_address) ->
      log Debug "new client connected";
      accept_client conf socket app;
      let fd = Socket.fd client_socket in
      if is_valid client_address then
        begin
          let reader = Reader.create fd in
          let writer = Writer.create fd in
          try_with (fun () -> Transport.dispatch_loop conf app reader writer)
          >>> function
            | Ok () -> ()
            | Error e ->
              log Debug "exception raised from dispatch_loop";
              (Writer.close writer ~force_close:(Clock.after (sec 30.))
               >>> fun () ->
               don't_wait_for (Reader.close reader));
              raise e
        end
      else
        begin
          log Verbose ("Rejecting connection from IP: " ^ (Socket.Address.to_string client_address));
          don't_wait_for (Unix.close fd)
        end

let serve { Configuration.max_connections; _ } bind_address port =
  let socket = Socket.create Socket.Type.tcp in
  try_with (fun () ->
    Socket.setopt socket Socket.Opt.reuseaddr true;
    Socket.bind socket
      (Socket.Address.Inet.create (UnixLabels.inet_addr_of_string bind_address) ~port)
    >>| fun bound_socket ->
    Socket.listen ~max_pending_connections:max_connections bound_socket)
  >>| function
    | Ok socket -> socket
    | Error e ->
      log Debug "exception raised while configuring main socket";
      don't_wait_for (Unix.close (Socket.fd socket));
      raise e

let within_monitor f =
  let monitor = Monitor.create ~name:"syslog monitor" () in
  Stream.iter (Monitor.errors monitor) ~f:(fun _exn ->
    List.iter
      ~f:(fun line -> Syslog.syslog line)
      (String.split ~on:'\n' (Exn.to_string _exn)));
  within' ~monitor f

let run ({ Configuration.connection; _ } as conf) app =
  match connection with
    | Configuration.Passive ->
      let descr = Async_unix.Raw_fd.File_descr.of_int (dup_stdin ()) in
      let fd = Fd.create (Fd.Kind.Socket `Passive) descr (Info.of_string "<stdin>") in
      return (if is_unix () then
          accept_client conf (Socket.of_fd fd Socket.Type.unix) app
        else
          accept_client conf (Socket.of_fd fd Socket.Type.tcp) app)
    | Configuration.TcpServer (bind_address, port) ->
      serve conf bind_address port
      >>| fun socket ->
      accept_client conf socket app

let start ~conf app =
  log Debug "Starting FCGI server";
  don't_wait_for (within_monitor (fun () -> run conf app));
  never_returns (Scheduler.go ())

let read { Request.stdin; _ } (env : string String.Map.t) =
  Pipe.read_all stdin
  >>| fun chunks ->
  let contents = Queue.to_list chunks |> String.concat in
  match Map.find env "CONTENT_LENGTH" with
    | None -> Some contents
    | Some length ->
      try
        let length = Int.of_string length in
        if length <> String.length contents then
          (log Verbose "Content length doesn't match data read from stdin";
           None)
        else
          Some contents
      with _ ->
        log Verbose "content length is invalid";
        None

let write { Request.stdout; _ } = Pipe.write stdout

let on_abort { Request.abort; _ } = Ivar.read abort

(* middleware example *)
let don't_stream app =
  fun request env ->
    read request env
    >>= (function
      | None -> return (500, "\r\n\r\nError receiving request")
      | Some body -> app request env body)
    >>= fun (status, response) ->
    write request ("Status: " ^ (Int.to_string status) ^ "\r\n" ^ response)
    >>| fun () -> status
