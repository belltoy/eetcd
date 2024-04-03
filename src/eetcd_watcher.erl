-module(eetcd_watcher).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([
    start_link/3,
    start_link/4,
    stop/1,
    watch/2,
    unwatch/1,
    unwatch/2
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    handle_continue/2,
    terminate/2
]).

-type watch_id() :: pos_integer().
-type timer() :: reference().

-record(state, {
    name :: atom(),
    watch_reqs = #{} :: #{watch_id() => {gen_server:from(), timer()}},
    cancel_reqs = #{} :: #{watch_id() => {gen_server:from(), timer()}},
    cancel_all = undefined :: {gen_server:from(), timer(), [watch_id()]} | undefined,
    watch_conn = undefined :: eetcd_watch:watch_conn() | undefined,
    event_manager_pid :: pid()
}).

-define(DEFAULT_WATCH_TIMEOUT, 5000).
-define(DEFAULT_UNWATCH_TIMEOUT, 5000).

start_link(EtcdName, Handler, InitArgs) ->
    gen_server:start_link(?MODULE, {EtcdName, Handler, InitArgs, []}, []).

start_link(EtcdName, Handler, InitArgs, WatchRequests) ->
    gen_server:start_link(?MODULE, {EtcdName, Handler, InitArgs, WatchRequests}, []).

stop(Pid) ->
    gen_server:call(Pid, stop).

watch(Pid, Req) ->
    watch(Pid, Req, ?DEFAULT_WATCH_TIMEOUT).

watch(Pid, Req, Timeout) ->
    gen_server:call(Pid, {watch, {Req, Timeout}}).

unwatch(Pid) ->
    gen_server:call(Pid, {unwatch, ?DEFAULT_UNWATCH_TIMEOUT}).

unwatch(Pid, WatchId) ->
    unwatch(Pid, WatchId, ?DEFAULT_UNWATCH_TIMEOUT).

unwatch(Pid, WatchId, Timeout) ->
    gen_server:call(Pid, {unwatch, WatchId, Timeout}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init({EtcdName, Handler, InitArgs, WatchRequests}) ->
    process_flag(trap_exit, true),
    {ok, Pid} = gen_event:start_link(),

    ok = gen_event:add_sup_handler(Pid, Handler, InitArgs),

    State = #state{name = EtcdName, event_manager_pid = Pid},

    {ok, State, {continue, {do_watch, WatchRequests}}}.

handle_continue({do_watch, []}, State) ->
    {noreply, State};
handle_continue({do_watch, [{WatchRequest, Timeout} | Rest]},
                #state{watch_conn = WatchConn0, watch_reqs = Reqs0} = State) ->
    case eetcd_watch:watch(State#state.name, WatchRequest, WatchConn0) of
        {ok, WatchConn, WatchId} ->
            TRef = erlang:send_after(Timeout, self(), {watch_timeout, WatchId}),
            Reqs = Reqs0#{WatchId => {undefined, TRef}},
            {noreply, State#state{watch_conn = WatchConn,
                                  watch_reqs = Reqs},
             {continue, {do_watch, Rest}}};
        {error, Reason} ->
            {stop, Reason, State}
    end.

handle_call({stop, Args}, _From, Args) ->
    {stop, normal, Args};

handle_call({watch, {Req, Timeout}}, From,
            #state{name = Name, watch_reqs = WatchReqs} = State) ->
    case eetcd_watch:watch(Name, Req) of
        {ok, WatchConn, WatchId} ->
            TRef = erlang:send_after(Timeout, self(), {watch_timeout, WatchId}),
            {noreply, State#state{watch_conn = WatchConn,
                                  watch_reqs = WatchReqs#{WatchId => {From, TRef}}}};
        {error, _} = E ->
            {reply, E, State}
    end;

%% Unwatch all
handle_call({unwatch, Timeout}, From, #state{watch_conn = WatchConn} = State) ->
    CancelIds = eetcd_watch:cancel_watch(WatchConn),
    TRef = erlang:send_after(Timeout, self(), {unwatch_timeout, all}),
    {noreply, State#state{cancel_all = {From, TRef, CancelIds}}};

handle_call({unwatch, WatchId, _Timeout}, _From,
            #state{cancel_reqs = CancelReqs} = State) when is_map_key(WatchId, CancelReqs) ->
    {reply, {error, already_unwatched}, State};
handle_call({unwatch, WatchId, Timeout}, From,
            #state{watch_conn = WatchConn, cancel_reqs = CancelReqs} = State) ->
    case eetcd_watch:cancel_watch(WatchConn, WatchId) of
        ok ->
            TRef = erlang:send_after(Timeout, self(), {unwatch_timeout, WatchId}),
            {noreply, State#state{cancel_reqs = CancelReqs#{WatchId => {From, TRef}}}};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({watch_timeout, WatchId}, #state{watch_reqs = WatchReqs} = State)
  when is_map_key(WatchId, WatchReqs) ->
    ?LOG_ERROR("watch timeout: ~p", [WatchId]),
    #{WatchId := {From, _TRef}} = WatchReqs,
    reply(From, {error, watch_timeout}),
    {stop, watch_timeout, State};

handle_info({unwatch_timeout, all}, #state{cancel_all = {From, _TRef, _Ids}} = State) ->
    ?LOG_ERROR("unwatch all timeout"),
    reply(From, {error, unwatch_timeout}),
    {stop, unwatch_timeout, State};
handle_info({unwatch_timeout, WatchId}, #state{cancel_reqs = CancelReqs} = State)
  when is_map_key(WatchId, CancelReqs) ->
    ?LOG_ERROR("unwatch watch_id ~p timeout", [WatchId]),
    #{WatchId := {From, _TRef}} = CancelReqs,
    reply(From, {error, unwatch_timeout}),
    {stop, unwatch_timeout, State};

handle_info({gen_event_EXIT, Handler, Reason}, State) ->
    ?LOG_ERROR("The gen_event handler ~p exited with reason ~p", [Handler, Reason]),
    {stop, {gen_event_EXIT, Handler, Reason}, State};

handle_info(Msg, State) ->
    case eetcd_watch:watch_stream(State#state.watch_conn, Msg) of
        {ok, WatchConn, Responses} ->
            handle_responses(Responses, State#state{watch_conn = WatchConn});
        {more, WatchConn} ->
            {noreply, State#state{watch_conn = WatchConn}};
        {error, Reason} ->
            %% TODO: Handle errors
            %% {error, {grpc_error, #{'grpc-status' => Status, 'grpc-message' => Message}}}
            %% {error, {gun_stream_error, Reason}}
            %% {error, {gun_conn_error, Reason}}
            %% {error, {gun_down, Reason}}
            {stop, Reason, State};
        unknown ->
            ?LOG_WARNING("unknown message: ~p", [Msg]),
            {noreply, State}
    end.

terminate(Reason, _State) ->
    ?LOG_DEBUG("eetcd watcher terminated with reason: ~p", [Reason]),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

handle_responses([], State) ->
    {noreply, State};

handle_responses([#{created := true, watch_id := Id} | Rest],
             #state{watch_reqs = WatchReqs} = State) when is_map_key(Id, WatchReqs) ->
    {{From, TRef}, WatchReqs1} = maps:take(Id, WatchReqs),
    erlang:cancel_timer(TRef),
    reply(From, ok),
    handle_responses(Rest, State#state{watch_reqs = WatchReqs1});

handle_responses([#{canceled := true, watch_id := Id} | Rest],
             #state{cancel_reqs = CancelReqs} = State) when is_map_key(Id, CancelReqs) ->
    {{From, TRef}, CancelReqs1} = maps:take(Id, CancelReqs),
    erlang:cancel_timer(TRef),
    reply(From, ok),
    handle_responses(Rest, State#state{cancel_reqs = CancelReqs1});

handle_responses([#{canceled := true, watch_id := Id} | Rest],
                 #state{cancel_all = {From, TRef, Ids}} = State) ->
    case lists:member(Id, Ids) of
        true ->
            case lists:delete(Id, Ids) of
                [] ->
                    erlang:cancel_timer(TRef),
                    reply(From, ok),
                    handle_responses(Rest, State#state{cancel_all = undefined});
                Ids1 ->
                    handle_responses(Rest, State#state{cancel_all = {From, TRef, Ids1}})
            end;
        false ->
            ?LOG_WARNING("Unknown canceled watch id: ~p", [Id]),
            {stop, unknown_canceled_watch_id, State}
    end;

handle_responses([#{created := false, canceled := false, events := Events} | Rest], State) ->
    gen_event:notify(State#state.event_manager_pid, Events),
    handle_responses(Rest, State).

reply(undefined, _Msg) -> ok;
reply(From, Msg) -> gen_server:reply(From, Msg).
