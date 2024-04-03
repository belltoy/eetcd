%% @doc This module provides the watch API functions for etcd watch API, just like eetcd_kv,
%% eetcd_lease does, etc.
%%
%% But the watch API is a bit different from the others because it's a bi-direction streaming gRPC
%% with some complex logic to handle single or multiple watches in a single stream, especially the
%% latter. That causes the watch API to be a bit more complex than the others. For example, if you
%% want to reuse the same stream for multiple watches, when you send another watch request via the
%% exist stream, the next received may not be the response for the last watch create request, but
%% the events for the previous watches. In other words, etcd server cannot guarantee the order of
%% the responses amount events and responses, so you have to handle the responses carefully.
%%
%% This module only provides a wrapper for the watch API RPC calls but a higher level watcher
%% implementation. The user should handle the responses and events by themselves in their own higher
%% level design.
%%
%% In a higher level design of the watch API, people can force the user to use a single watch
%% within a single stream by providing a simple watcher implementation. Or they can provide a
%% complex watcher implementation that can handle multiple watches in a single stream.

-module(eetcd_watch).
-include("eetcd.hrl").

-export_type([watch_conn/0]).

-type watch_id() :: pos_integer().
-type watch_conn() :: #{http2_pid => pid(),
                        monitor_ref => reference(),
                        stream_ref => gun:stream_ref(),

                        %% A buffer for incompleted response frame
                        unprocessed => binary(),

                        %% Revision is the revision of the KV when the watchResponse is created, aka
                        %% the [Start_Revision](https://etcd.io/docs/v3.5/learning/api/).
                        watch_ids => #{ watch_id() =>
                                        #{
                                          %% For a normal response, the revision should be the same as the last modified revision inside Events.
                                          revision => integer(),

                                          %% CompactRevision is set when the watcher is cancelled due to compaction.
                                          compact_revision => integer()
                                         }}
                       }.

%% API
-export([new/0, with_key/2,
    with_range_end/2, with_prefix/1, with_from_key/1,
    with_start_revision/2,
    with_filter_delete/1, with_filter_put/1,
    with_prev_kv/1,
    with_watch_id/2,
    with_fragment/1,
    with_progress_notify/1
]).
-export([watch/2, watch/3]).
-export([watches/1]).
-export([watch_stream/2]).
-export([unwatch/2]).
-export([cancel_watch/1, cancel_watch/2]).
-export([rev/1]).

-define(AUTO_WATCH_ID, 0).

%%% @doc init watch request
-spec new() -> context().
new() -> #{}.

%% @doc AutoWatchID is the watcher ID passed in WatchStream.Watch when no
%% user-provided ID is available, an ID will automatically be assigned.
-spec with_watch_id(context(), watch_id()) -> context().
with_watch_id(Context, WatchId) ->
    maps:put(watch_id, WatchId, Context).

%% @doc Get the previous key-value pair before the event happens.
%% If the previous KV is already compacted, nothing will be returned.
-spec with_prev_kv(context()) -> context().
with_prev_kv(Context) ->
    maps:put(prev_kv, true, Context).

%% @doc WithFragment to receive raw watch response with fragmentation.
%%Fragmentation is disabled by default. If fragmentation is enabled,
%%etcd watch server will split watch response before sending to clients
%%when the total size of watch events exceed server-side request limit.
%%The default server-side request limit is 1.5 MiB, which can be configured
%%as "--max-request-bytes" flag value + gRPC-overhead 512 bytes.
-spec with_fragment(context()) -> context().
with_fragment(Context) ->
    maps:put(fragment, true, Context).

%% @doc Start revision, an optional revision for where to inclusively begin watching.
%% If not given, it will stream events following the revision of the watch creation
%% response header revision.
%% The entire available event history can be watched starting from the last compaction revision.
%%
%% Note that the start revision is inclusive, so for example, if the start revision is 100,
%% the first event returned will be at revision 100. So in practice, the start revision is better
%% set to the last **GET** revision + 1 to exclude the previous change before the watch.
with_start_revision(Context, Rev) ->
    maps:put(start_revision, Rev, Context).

%% @doc Make watch server send periodic progress updates
%% every 10 minutes when there is no incoming events.
%% Progress updates have zero events in WatchResponse.
-spec with_progress_notify(context()) -> context().
with_progress_notify(Context) ->
    maps:put(progress_notify, true, Context).

%% @doc discards PUT events from the watcher.
-spec with_filter_put(context()) -> context().
with_filter_put(Context) ->
    maps:update_with(filters,
        fun(V) -> lists:usort(['NOPUT' | V]) end,
        ['NOPUT'],
        Context).

%% @doc discards DELETE events from the watcher.
-spec with_filter_delete(context()) -> context().
with_filter_delete(Context) ->
    maps:update_with(filters,
        fun(V) -> lists:usort(['NODELETE' | V]) end,
        ['NODELETE'],
        Context).

%%% @doc Sets the byte slice for the Op's `key'.
-spec with_key(context(), key()) -> context().
with_key(Context, Key) ->
    maps:put(key, Key, Context).

%% @doc Enables `watch' requests to operate
%% on the keys with matching prefix. For example, `watch("foo", with_prefix())'
%% can return 'foo1', 'foo2', and so on.
-spec with_prefix(context()) -> context().
with_prefix(#{key := Key} = Context) ->
    with_range_end(Context, eetcd:get_prefix_range_end(Key)).

%%  @doc Specifies the range of `get', `delete' requests
%% to be equal or greater than the key in the argument.
-spec with_from_key(context()) -> context().
with_from_key(Context) ->
    with_range_end(Context, "\x00").

%% @doc Sets the byte slice for the Op's `range_end'.
-spec with_range_end(context(), iodata()) -> context().
with_range_end(Context, End) ->
    maps:put(range_end, End, Context).

%% @doc Returns the watches in the given watch connection.
-spec watches(watch_conn()) -> Watches
      when Watches :: #{ integer() => #{ revision => integer(), compact_revision => integer()}}.
watches(#{watch_ids := Watches} = _WatchConn) ->
    Watches.

%% @doc @equiv watch(name(), context(), 5000).
-spec watch(name(), context()) ->
    {ok, watch_conn(), WatchId :: watch_id()} |
    {error, eetcd_error()}.
watch(Name, CreateReq) ->
    watch(Name, CreateReq, undefined).

%% @doc Watch watches for events happening or that have happened. Both input and output are streams;
%% the input stream is for creating watchers and the output stream sends events.
%% One watch RPC can watch on multiple key ranges, streaming events for several watches at once.
%% The entire event history can be watched starting from the last compaction revision.
%%
%% Watch creates a watcher. The watcher watches the events happening or
%% happened on the given key or range [key, end) from the given startRev.
%%
%%	The whole event history can be watched unless compacted.
%%	If `startRev <= 0', watch observes events after currentRev.
%%
%%	The returned "id" is the ID of this watcher. It appears as WatchID
%%  in events that are sent to the created watcher through stream channel.
-spec watch(name(), context(), watch_conn() | undefined) ->
    {ok, watch_conn(), WatchId :: watch_id()} |
    {error, eetcd_error()}.
%% TODO: Timeout
watch(_Name, CreateReq, #{http2_pid := Gun,
                          stream_ref := StreamRef,
                          monitor_ref := MRef} = WatchConn)
  when is_pid(Gun), is_reference(StreamRef), is_reference(MRef) ->
    case assign_watch_id(CreateReq, WatchConn) of
        {error, already_in_use} -> {error, already_in_use};
        {ok, NewCreateReq, NewWatchConn, WatchId} ->
            Request = #{request_union => {create_request, NewCreateReq}},
            eetcd_stream:data(Gun, StreamRef, Request, 'Etcd.WatchRequest', nofin),
            {ok, NewWatchConn, WatchId}
    end;
watch(Name, CreateReq, undefined) ->
    case eetcd_watch_gen:watch(Name) of
        {ok, Gun, StreamRef} ->
            MRef = erlang:monitor(process, Gun),
            WatchConn = #{
                http2_pid => Gun,
                monitor_ref => MRef,
                stream_ref => StreamRef,
                watch_ids => #{},
                unprocessed => <<>>
            },
            watch(Name, CreateReq, WatchConn);
        {error, _Reason} = E -> E
    end.

%% @doc Streams the next batch of events/response from the given message.
%% This function processes a "message" which can be any term, but should be a message received by the process that owns the stream_ref.
%% Processing a message means that this function will parse it and check if it's a message that is directed to this connection,
%% that is, a gun_* message received on the gun connection.
%% If it is, then this function will parse the message, turn it into  watch responses, and possibly take action given the responses.
%% If there's no error, this function returns {ok, WatchConn, 'Etcd.WatchResponse'()}|{more, WatchConn}
%% If there's an error, {error, eetcd_error()} is returned.
%% If the given message is not from the gun connection, this function returns unknown.
-spec watch_stream(watch_conn(), Message) ->
    {ok, watch_conn(), [router_pb:'Etcd.WatchResponse'()]}
    | {more, watch_conn()}
    | unknown
    | {error, eetcd_error()} when
    Message :: term().

watch_stream(#{stream_ref := Ref, http2_pid := Pid, unprocessed := Unprocessed} = Conn,
             {gun_data, Pid, Ref, nofin, Data}) ->
    Bin = <<Unprocessed/binary, Data/binary>>,
    decode(Conn#{unprocessed => <<>>}, Bin);
watch_stream(#{stream_ref := SRef, http2_pid := Pid} = Conn,
             {gun_response, Pid, SRef, _IsFin, 200 = _Status, _Headers}) ->
    %% ignore response header
    {more, Conn};
watch_stream(#{stream_ref := SRef, http2_pid := Pid, monitor_ref := MRef} = _Conn,
             {gun_response, Pid, SRef, _IsFin, Status, Headers}) when Status =/= 200 ->
    %% error response
    erlang:demonitor(MRef, [flush]),
    gun:cancel(Pid, SRef),
    Msg = proplists:get_value(<<"grpc-message">>, Headers, <<"">>),
    {error, ?GRPC_ERROR(Status, Msg)};
watch_stream(#{stream_ref := SRef, http2_pid := Pid, monitor_ref := MRef},
    {gun_trailers, Pid, SRef, [{<<"grpc-status">>, Status}, {<<"grpc-message">>, Msg}]}) ->
    erlang:demonitor(MRef, [flush]),
    gun:cancel(Pid, SRef),
    {error, ?GRPC_ERROR(Status, Msg)};
watch_stream(#{stream_ref := SRef, http2_pid := Pid, monitor_ref := MRef},
    {gun_error, Pid, SRef, Reason}) -> %% stream error
    erlang:demonitor(MRef, [flush]),
    gun:cancel(Pid, SRef),
    {error, {gun_stream_error, Reason}};
watch_stream(#{http2_pid := Pid, stream_ref := SRef, monitor_ref := MRef},
    {gun_error, Pid, Reason}) -> %% gun connection process state error
    erlang:demonitor(MRef, [flush]),
    gun:cancel(Pid, SRef),
    {error, {gun_conn_error, Reason}};
watch_stream(#{http2_pid := Pid, monitor_ref := MRef},
    {'DOWN', MRef, process, Pid, Reason}) -> %% gun connection down
    erlang:demonitor(MRef, [flush]),
    {error, {gun_down, Reason}};
watch_stream(_Conn, _Unknow) -> unknown.

%% @doc Rev returns the current revision of the KV the stream watches on.

rev(#{revision := Rev}) -> Rev.

%% @doc  Cancel watching so that no more events are transmitted.
%% This is a synchronous operation.
%% Other change events will be returned in OtherEvents when these events arrive between the request and the response.
%%
%% Notice that this function will cancel all the watches in the same stream.
-spec unwatch(watch_conn(), Timeout) ->
    {ok, Responses, OtherEvents}
    | {error, eetcd_error(), Responses, OtherEvents} when
    Timeout :: pos_integer(),
    Responses :: [router_pb:'Etcd.WatchResponse'()],
    OtherEvents :: [router_pb:'Etcd.WatchResponse'()].
unwatch(WatchConn, Timeout) ->
    %% FIXME: Don't await response here because the response is not guaranteed to be ordered
    unwatch_and_await_resp(WatchConn, Timeout, [], []).

cancel_watch(#{watch_ids := WatchIds} = WatchConn) ->
    do_cancel_watch(WatchConn, maps:keys(WatchIds), []).

cancel_watch(#{watch_ids := WatchIds} = WatchConn, WatchId) when is_map_key(WatchId, WatchIds) ->
    _ = do_cancel_watch(WatchConn, [WatchId], []),
    ok;
cancel_watch(_WatchConn, _WatchId) ->
    {error, watch_id_not_found}.

%%====================================================================
%% Internal functions
%%====================================================================

do_cancel_watch(_WatchConn, [], Acc) ->
    lists:reverse(Acc);
do_cancel_watch(#{http2_pid := Gun,
                  stream_ref := StreamRef,
                  watch_ids := WatchIds} = WatchConn, [WatchId | Rest], Acc) ->
    Request = #{request_union => {cancel_request, #{watch_id => WatchId}}},
    eetcd_stream:data(Gun, StreamRef, Request, 'Etcd.WatchRequest', nofin),
    do_cancel_watch(WatchConn#{watch_ids => maps:remove(WatchId, WatchIds)}, Rest, [WatchId | Acc]).

unwatch_and_await_resp(#{http2_pid := Gun,
                         stream_ref := StreamRef,
                         monitor_ref := MRef,
                         watch_ids := WatchIds} = _WatchConn, _Timeout, RespAcc, Acc)
    when erlang:map_size(WatchIds) =:= 0 ->
    gun:cancel(Gun, StreamRef),
    erlang:demonitor(MRef, [flush]),
    {ok, lists:reverse(RespAcc), lists:reverse(Acc)};
unwatch_and_await_resp(#{http2_pid := Gun,
                         stream_ref := StreamRef,
                         watch_ids := WatchIds} = WatchConn, Timeout, RespAcc, Acc) ->
    [WatchId|_Rest] = maps:keys(WatchIds),
    IsFin = case maps:size(WatchIds) of
                1 -> fin;
                _ -> nofin
            end,
    Request = #{request_union => {cancel_request, #{watch_id => WatchId}}},
    eetcd_stream:data(Gun, StreamRef, Request, 'Etcd.WatchRequest', IsFin),
    await_unwatch_resp(WatchConn, Timeout, RespAcc, Acc).

await_unwatch_resp(#{http2_pid := Gun,
                     monitor_ref := MRef,
                     stream_ref := StreamRef,
                     watch_ids := WatchIds,
                     unprocessed := Unprocessed} = WatchConn, Timeout, RespAcc, Acc) ->
    case eetcd_stream:await(Gun, StreamRef, Timeout, MRef) of
        {data, nofin, Data} ->
            Bin = <<Unprocessed/binary, Data/binary>>,
            case eetcd_grpc:decode(identity, Bin, 'Etcd.WatchResponse') of
                {ok, Resp, NewUnprocessed} ->
                    case Resp of
                        #{created := false, watch_id := WatchId, canceled := true} ->
                            unwatch_and_await_resp(WatchConn#{unprocessed => NewUnprocessed,
                                                          watch_ids => maps:without([WatchId], WatchIds)
                                                         }, Timeout, [Resp|RespAcc], Acc);
                        OtherEvent ->
                            await_unwatch_resp(WatchConn#{unprocessed => NewUnprocessed}, Timeout, RespAcc, [OtherEvent | Acc])
                    end;
                more ->
                    await_unwatch_resp(WatchConn#{unprocessed => Bin}, Timeout, RespAcc, Acc)
            end;
        {error, Reason} -> {error, Reason, lists:reverse(RespAcc), lists:reverse(Acc)}
    end.

-spec assign_watch_id(context(), watch_conn()) ->
    {ok, context(), watch_conn(), watch_id()} | {error, already_in_use}.
assign_watch_id(CreateReq, #{watch_ids := WatchIds} = WatchConn) ->
    %% DON'T use 0 as watch_id because it's reserved for auto-assign on etcd server side
    %% So if the request has no watch_id, we should assign a new one from 1 or the next
    %% available + 1
    case maps:get(watch_id, CreateReq, undefined) of
        Id when Id =:= undefined; Id =:= ?AUTO_WATCH_ID ->
            %% Use the next available watch_id
            NextId = try lists:max(maps:keys(WatchIds)) catch _:_ -> 0 end + 1,
            NewCreateReq = with_watch_id(CreateReq, NextId),
            %% Initialize the watch_ids in the WatchConn
            {ok, NewCreateReq, WatchConn#{watch_ids => WatchIds#{NextId => #{}}}, NextId};
        Id when is_map_key(Id, WatchIds) ->
            {error, already_in_use};
        Id ->
            {ok, CreateReq, WatchConn#{watch_ids => WatchIds#{Id => #{}}}, Id}
    end.

-spec decode(watch_conn(), binary()) ->
    {ok, watch_conn(), [router_pb:'Etcd.WatchResponse'()]} | {more, watch_conn()}.
decode(Conn, Bin) ->
    decode(Conn, Bin, []).

-spec decode(watch_conn(), binary(), [router_pb:'Etcd.WatchResponse'()]) ->
    {ok, watch_conn(), [router_pb:'Etcd.WatchResponse'()]} | {more, watch_conn()}.
decode(Conn, <<>>, []) ->
    {more, Conn};
decode(Conn, <<>>, Responses) ->
    {ok, Conn, lists:reverse(Responses)};
decode(#{watch_ids := Ids} = Conn, Bin, Responses) ->
    case eetcd_grpc:decode(identity, Bin, 'Etcd.WatchResponse') of
        %% Create response
        {ok, #{created := true,
               canceled := false,
               watch_id := WatchId,
               compact_revision := CompactRev,
               header := #{revision := Rev}} = Resp, Rest} when is_map_key(WatchId, Ids) ->
            NewConn = Conn#{
                watch_ids => Ids#{WatchId => #{revision => Rev, compact_revision => CompactRev}}
            },
            decode(NewConn, Rest, [Resp | Responses]);

        %% Cancel response
        {ok, #{created := false,
               canceled := true,
               watch_id := WatchId} = Resp, Rest} when is_map_key(WatchId, Ids) ->
            decode(Conn#{watch_ids => maps:remove(WatchId, Ids)}, Rest, [Resp | Responses]);

        %% Events
        {ok, #{created := false,
               canceled := false,
               watch_id := WatchId,
               events := _Events,
               compact_revision := CompactRev,
               header := #{revision := Rev}} = Resp, Rest} when is_map_key(WatchId, Ids) ->
            NewConn = Conn#{
                watch_ids => Ids#{
                    WatchId => #{revision => Rev, compact_revision => CompactRev}
                }
            },
            decode(NewConn, Rest, [Resp | Responses]);

        more ->
            decode(Conn#{unprocessed => Bin}, <<>>, Responses)
    end.
