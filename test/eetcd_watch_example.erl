-module(eetcd_watch_example).

-behaviour(gen_server).
-define(NAME, watch_example_conn).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(KEY1, <<"heartbeat:">>).
-define(KEY2, <<"heartbeat2:">>).
-define(CHECK_RETRY_MS, 3000).

start_link() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    erlang:process_flag(trap_exit, true),
    Registers = ["127.0.0.1:2379"],
    {ok, _Pid} = eetcd:open(?NAME, Registers),

    ets:new(?MODULE, [named_table, {read_concurrency, true}, public]),

    {ok, Services, Rev1} = get_exist_services(?KEY1),
    ets:insert(?MODULE, Services),
    {ok, Services2, Rev2} = get_exist_services(?KEY2),
    ets:insert(?MODULE, Services2),

    {ok, Conn1, WatchId1} = watch_services_event_(?KEY1, Rev1 + 1),
    {ok, Conn2, WatchId2} = watch_services_event_(?KEY2, Rev2 + 1, Conn1),
    Mapping = #{WatchId1 => {?KEY1, Rev1}, WatchId2 => {?KEY2, Rev2}},

    {ok, ensure_retry(#{conn      => Conn2,
                        mapping   => Mapping,
                        retries   => [],
                        retry_ref => undefined})}.

get_exist_services(KeyPrefix) ->
    Ctx = eetcd_kv:new(?NAME),
    Ctx1 = eetcd_kv:with_key(Ctx, KeyPrefix),
    Ctx2 = eetcd_kv:with_prefix(Ctx1),
    Ctx3 = eetcd_kv:with_keys_only(Ctx2),
    {ok, #{header := #{revision := Revision}, kvs := Services}} = eetcd_kv:get(Ctx3),
    Services1 =
        [begin
             [_, Type, IP, Port] = binary:split(Key, [<<"|">>], [global]),
             {{IP, Port}, Type}
         end || #{key := Key} <- Services],
    {ok, Services1, Revision}.

watch_services_event_(Key, Revision) ->
    watch_services_event_(Key, Revision, undefined).

watch_services_event_(Key, Revision, Conn) ->
    ReqInit = eetcd_watch:new(),
    ReqKey = eetcd_watch:with_key(ReqInit, Key),
    ReqPrefix = eetcd_watch:with_prefix(ReqKey),
    Req = eetcd_watch:with_start_revision(ReqPrefix, Revision),
    eetcd_watch:watch(?NAME, Req, Conn).

handle_info(retry, #{retries := []} = State) ->
    {noreply, ensure_retry(State#{retry_ref => undefined})};
handle_info(retry, State) ->
    NewState = retry(State),
    {noreply, ensure_retry(NewState#{retry_ref => undefined})};

handle_info(Msg, #{conn := Conn, mapping := Mapping} = State) ->
    case eetcd_watch:watch_stream(Conn, Msg) of
        {ok, NewConn, WatchResponses} ->
            io:format("Received watch responses: ~p~n", [WatchResponses]),
            update_services([ R
                              || #{created := Created, canceled := Canceled} = R <- WatchResponses,
                                   Created =:= false, Canceled =:= false
                            ]),
            io:format("ets: ~p~n", [ets:tab2list(?MODULE)]),
            {noreply, State#{conn => NewConn, mapping => update_revision(Mapping, WatchResponses)}};
        {more, NewConn} ->
            {noreply, State#{conn => NewConn}};
        {error, Reason} ->
            %% TODO handle error like stream error and/or connection error
            io:format("watch_stream error for ~p, state: ~p~n", [Reason, State]),
            {noreply, State#{conn => undefined, retries => maps:values(Mapping)}};
        unknown ->
            {noreply, State}
    end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    eetcd:close(?NAME),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

update_services(#{events := Events}) ->
    [begin
         X = binary:split(Key, [<<"|">>], [global]),
         [_, Type, IP, Port] = X,
         case EventType of
             'PUT' -> ets:insert(?MODULE, {{IP, Port}, Type});
             'DELETE' -> ets:delete(?MODULE, {IP, Port})
         end
     end || #{kv := #{key := Key}, type := EventType} <- Events],
    ok.

update_revision(Mapping, []) -> Mapping;
update_revision(Mapping, [#{header := #{revision := Rev}, watch_id := WatchId} | Rest]) ->
    Mapping1 = maps:update_with(WatchId, fun({Key, _}) -> {Key, Rev} end, Mapping),
    update_revision(Mapping1, Rest).

ensure_retry(#{retry_ref := undefined} = State) ->
    Ref = erlang:send_after(?CHECK_RETRY_MS, self(), retry),
    State#{retry_ref := Ref};
ensure_retry(State) ->
    State.

retry(#{mapping := _Mapping, retries := [], conn := _Conn} = State) -> State;
retry(#{mapping := Mapping, retries := Retries, conn := Conn} = State) ->
    {NewConn, Watches, NewRetries} =
    lists:foldl(fun({Key, Rev}, {Conn0, Acc, Retries0}) ->
                      case watch_services_event_(Key, Rev + 1, Conn0) of
                          {ok, NewConn0, NewWatchId} ->
                              {NewConn0, [{NewWatchId, {Key, Rev}}|Acc], Retries0};
                          {error, Reason} ->
                              io:format("Watch key ~p error: ~p~n", [Key, Reason]),
                              {Conn0, Acc, [{Key, Rev}|Retries0]}
                      end
              end, {Conn, [], []}, Retries),
    State#{conn => NewConn,
           mapping => maps:merge(Mapping, maps:from_list(Watches)),
           retries => NewRetries}.
