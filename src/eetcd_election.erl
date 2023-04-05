-module(eetcd_election).
-include("eetcd.hrl").

-export([new/1, with_timeout/2, with_name/2, with_lease/2, with_leader/2]).
-export([campaign/4, proclaim/3, leader/2, resign/2]).
-export([campaign/1, proclaim/1, leader/1, resign/1]).
-export([campaign_request/4, campaign_response/2]).
-export([observe/3, observe_stream/2]).

-export_type([campaign_ctx/0, observe_ctx/0]).
-type leader_key() :: router_pb:'Etcd.LeaderKey'().
-type observe_ctx() :: #{
    leader => map() | election_no_leader,
    http2_pid => pid(),
    monitor_ref => reference(),
    stream_ref => gun:stream_ref()
}.
-type campaign_ctx() :: #{
    campaign := leader_key() | waiting_campaign_response,
    http2_pid => pid(),
    monitor_ref => reference(),
    stream_ref => gun:stream_ref()
}.

%%% @doc Creates a blank context for a request.
-spec new(new_context()) -> context().
new(Ctx) -> eetcd:new(Ctx).

%% @doc Timeout is an integer greater than zero which specifies how many milliseconds to wait for a reply,
%% or the atom infinity to wait indefinitely. Default value is 5000.
%% If no reply is received within the specified time, the function call fails with `{error, timeout}'.
-spec with_timeout(context(), pos_integer()|infinity) -> context().
with_timeout(Ctx, Timeout) -> eetcd:with_timeout(Ctx, Timeout).

%%% @doc name is the election's identifier for the campaign.
-spec with_name(context(), Name :: binary()) -> context().
with_name({E, Ctx}, Name) ->
    {E, maps:put(name, Name, Ctx)}.

%%% @doc lease is the ID of the lease attached to leadership of the election. If the
%%  lease expires or is revoked before resigning leadership, then the
%%  leadership is transferred to the next campaigner, if any.
-spec with_lease(context(), LeaseID :: integer()) -> context().
with_lease({E, Ctx}, LeaseID) ->
    {E, maps:put(lease, LeaseID, Ctx)}.

%%% @doc value is the value set when the campaigner wins the election.
-spec with_value(context(), Value :: binary()) -> context().
with_value({E, Ctx}, Value) ->
    {E, maps:put(value, Value, Ctx)}.

%%% @doc  leader describes the resources used for holding leadership of the election.
%%%  It's a map return from CampaignResponse
%%% name is the election identifier that corresponds to the leadership key.
%%% key is an opaque key representing the ownership of the election. If the key is deleted, then leadership is lost.
%%% rev is the creation revision of the key. It can be used to test for ownership of an election during transactions by testing the key's creation revision matches rev.
%%% lease is the lease ID of the election leader.
-spec with_leader(context(), LeaderKey :: leader_key()) -> context().
with_leader({E, Ctx}, LeaderKey) ->
    {E, maps:put(leader, LeaderKey, Ctx)}.

%%% @doc
%%% Campaign waits to acquire leadership in an election, returning a LeaderKey
%%% representing the leadership if successful. The LeaderKey can then be used
%%% to issue new values on the election, transactionally guard API requests on
%%% leadership still being held, and resign from the election.
%%% <dl>
%%% <dt> 1. base </dt>
%%% <dd> `eetcd_election:campaign(ConnName, Name, LeaseId, Value).' </dd>
%%% <dt> 2. elixir </dt>
%%% <dd>
%%% ```
%%% :eetcd_election.new(connName)
%%% |> :eetcd_election.with_timeout(3000)
%%% |> :eetcd_election.with_name(name)
%%% |> :eetcd_election.with_lease(leaseId)
%%% |> :eetcd_election.with_value(Value)
%%% |> :eetcd_kv.campaign()
%%% '''
%%% </dd> </dl>
%%% {@link eetcd_election:with_name/2}, {@link eetcd_election:with_lease/2},
%%% {@link eetcd_election:with_value/2}, {@link eetcd_election:with_timeout/2}
%%% @end
-spec campaign(Ctx :: context()) -> {ok, router_pb:'Etcd.CampaignResponse'()} | {error, eetcd_error()}.
campaign(Ctx) ->
    eetcd_election_gen:campaign(Ctx).

-spec campaign(Ctx :: new_context(), Name :: binary(), LeaseId :: integer(), Value :: binary()) ->
    {ok, router_pb:'Etcd.CampaignResponse'()} | {error, eetcd_error()}.
campaign(Ctx, Name, LeaseId, Value) ->
    Ctx1 = new(Ctx),
    Ctx2 = with_name(Ctx1, Name),
    Ctx3 = with_lease(Ctx2, LeaseId),
    Ctx4 = with_value(Ctx3, Value),
    eetcd_election_gen:campaign(Ctx4).

%%% @doc campaign async to acquire leadership.
%%% if there is already a leader, campaign/4 will be held(block) forever until timeout.
%%% the campaign_request/4 will return immediately,
%%% then your can use campaign_response/2 to handle `Etcd.CampaignResponse`.
%%% gen_server example
%%% ```
%%% init(Arg) ->
%%%   ...
%%%   {ok, CCtx} = eetcd_election:campaign_request(connName, Name, LeaderId, Value),
%%%   ...
%%% handle_info(Msg, State=#{ctx := CCtx}) ->
%%%   case eetcd_election:campaign_response(CCtx, Msg) of
%%%          unknown -> do_handle_your_msg(Msg, State);
%%%         {ok, #{campaign := Leader}} -> campaign_win(Leader);
%%%         {error, Reason} -> campaign_error(Reason)
%%%   end.
%%% '''
-spec campaign_request(name(), Name :: binary(), LeaseId :: integer(), Value :: binary()) ->
    {ok, campaign_ctx()} | {error, eetcd_error()}.
campaign_request(ConnName, Name, LeaseId, Value) ->
    Request0 = with_name({#{}, #{}}, Name),
    Request1 = with_lease(Request0, LeaseId),
    {_, Request} = with_value(Request1, Value),
    case eetcd_stream:new(ConnName, <<"/v3electionpb.Election/Campaign">>) of
        {ok, Gun, StreamRef} ->
            MRef = erlang:monitor(process, Gun),
            eetcd_stream:data(Gun, StreamRef, Request, 'Etcd.CampaignRequest', fin),
            {ok,
                #{
                    http2_pid => Gun,
                    monitor_ref => MRef,
                    stream_ref => StreamRef,
                    campaign => waiting_campaign_response
                }
            };
        Err -> Err
    end.

-spec campaign_response(campaign_ctx(), term()) ->
    unknown|{ok, campaign_ctx()} | {error, eetcd_error()}.
%%% @doc handle campaign async response `Etcd.CampaignResponse'.
campaign_response(CCtx, Msg) ->
    case resp_stream(CCtx, Msg) of
        {ok, Bin} ->
            case maps:get(monitor_ref, CCtx, undefined) of
                MRef when is_reference(MRef) ->
                    erlang:demonitor(MRef, [flush]);
                _ ->
                    ok
            end,
            {ok, #{leader := Leader}, <<>>}
                = eetcd_grpc:decode(identity, Bin, 'Etcd.CampaignResponse'),
            {ok, #{campaign => Leader}};
        Other -> Other
    end.

%%% @doc
%%% Proclaim updates the leader's posted value with a new value.
%%% Leader is the leadership hold on the election.
%%% Value is an update meant to overwrite the leader's current value.
%%% <dl>
%%% <dt> 1. base </dt>
%%% <dd> `eetcd_election:proclaim(ConnName, Leader, Value).' </dd>
%%% <dt> 2. elixir </dt>
%%% <dd>
%%% ```
%%% :eetcd_election.new(connName)
%%% |> :eetcd_election.with_leader(name)
%%% |> :eetcd_election.with_value(Value)
%%% |> :eetcd_kv.proclaim()
%%% '''
%%% </dd> </dl>
%%% {@link eetcd_election:with_leader/2}, {@link eetcd_election:with_value/2}
%%% @end
-spec proclaim(Ctx :: context()) ->
    {ok, router_pb:'Etcd.ProclaimResponse'()} | {error, eetcd_error()}.
proclaim(Ctx) ->
    eetcd_election_gen:proclaim(Ctx).

-spec proclaim(Ctx :: new_context(), LeaderKey :: leader_key(), Value :: binary()) ->
    {ok, router_pb:'Etcd.ProclaimResponse'()} | {error, eetcd_error()}.
proclaim(Ctx, LeaderKey, Val) ->
    Ctx1 = new(Ctx),
    Ctx2 = with_leader(Ctx1, LeaderKey),
    Ctx3 = with_value(Ctx2, Val),
    eetcd_election_gen:proclaim(Ctx3).

%%% @doc
%%% Resign releases election leadership so other campaigners may acquire
%%  leadership on the election.
%%% <dl>
%%% <dt> 1. base </dt>
%%% <dd> `eetcd_election:resign(ConnName, Leader).' </dd>
%%% <dt> 2. elixir </dt>
%%% <dd>
%%% ```
%%% :eetcd_election.new(connName)
%%% |> :eetcd_election.with_leader(Leader)
%%% |> :eetcd_kv.resign()
%%% '''
%%% </dd> </dl>
%%% {@link eetcd_election:with_leader/2}
%%% @end
-spec resign(Ctx :: context()) ->
    {ok, router_pb:'Etcd.ResignResponse'()} | {error, eetcd_error()}.
resign(Ctx) ->
    eetcd_election_gen:resign(Ctx).

-spec resign(Ctx :: new_context(), LeaderKey :: leader_key()) ->
    {ok, router_pb:'Etcd.ResignResponse'()} | {error, eetcd_error()}.
resign(Ctx, LeaderKey) ->
    Ctx1 = new(Ctx),
    Ctx2 = with_leader(Ctx1, LeaderKey),
    eetcd_election_gen:resign(Ctx2).

%%% @doc
%%% Leader returns the current election proclamation, if any.
%%% <dl>
%%% <dt> 1. base </dt>
%%% <dd> `eetcd_election:leader(ConnName, Name).' </dd>
%%% <dt> 2. elixir </dt>
%%% <dd>
%%% ```
%%% :eetcd_election.new(connName)
%%% |> :eetcd_election.with_name(name)
%%% |> :eetcd_kv.leader()
%%% '''
%%% </dd> </dl>
%%% {@link eetcd_election:with_name/2}
%%% @end
-spec leader(Ctx :: context()) ->
    {ok, router_pb:'Etcd.LeaderResponse'()} | {error, eetcd_error()}.
leader(Ctx) ->
    eetcd_election_gen:leader(Ctx).

-spec leader(Ctx :: new_context(), Name :: binary()) ->
    {ok, router_pb:'Etcd.LeaderResponse'()} | {error, eetcd_error()}.
leader(Ctx, Name) ->
    Ctx1 = new(Ctx),
    Ctx2 = with_name(Ctx1, Name),
    eetcd_election_gen:leader(Ctx2).

%%% @doc Observe streams election proclamations in-order as made by the election's elected leaders.
%%% Timeout is an integer greater than zero which specifies how many milliseconds to wait for a leaders,
%%% or the atom infinity to wait indefinitely. If no leader is received within the specified time,
%%% the function call return 'election_no_leader'. and will streams election proclamations by order messages.
-spec observe(name(), binary(), timeout()) -> {ok, observe_ctx()}|{error, eetcd_error()}.
observe(ConnName, Name, Timeout) ->
    Request = #{name => Name},
    {ok, Gun, StreamRef} = eetcd_election_gen:observe(ConnName),
    MRef = erlang:monitor(process, Gun),
    eetcd_stream:data(Gun, StreamRef, Request, 'Etcd.LeaderRequest', fin),
    case eetcd_stream:await(Gun, StreamRef, Timeout, MRef) of
        {response, nofin, 200, _Headers} ->
            case eetcd_stream:await(Gun, StreamRef, Timeout, MRef) of
                {data, nofin, Body} ->
                    {ok, #{kv := KV}, <<>>}
                        = eetcd_grpc:decode(identity, Body, 'Etcd.LeaderResponse'),
                    {ok,
                        #{
                            http2_pid => Gun,
                            monitor_ref => MRef,
                            stream_ref => StreamRef,
                            leader => KV
                        }
                    };
                {error, _} = Err1 ->
                    erlang:demonitor(MRef, [flush]),
                    Err1
            end;
        {response, fin, 200, RespHeaders} ->
            erlang:demonitor(MRef, [flush]),
            {error, eetcd_grpc:grpc_status(RespHeaders)};
        {error, timeout} ->
            {ok,
                #{
                    http2_pid => Gun,
                    monitor_ref => MRef,
                    stream_ref => StreamRef,
                    leader => election_no_leader
                }
            };
        {error, _} = Err2 ->
            erlang:demonitor(MRef, [flush]),
            Err2
    end.

%%% @doc handle observe stream `Etcd.LeaderResponse'.
-spec observe_stream(observe_ctx(), term()) ->
    unknown|{ok, observe_ctx()} | {error, eetcd_error()}.
observe_stream(OCtx, Msg) ->
    case resp_stream(OCtx, Msg) of
        {ok, Bin} ->
            {ok, #{kv := KV}, <<>>} = eetcd_grpc:decode(identity, Bin, 'Etcd.LeaderResponse'),
            {ok, OCtx#{leader => KV}};
        Other -> Other
    end.

resp_stream(#{stream_ref := Ref, http2_pid := Pid},
    {gun_response, Pid, Ref, nofin, 200, _Headers}) ->
    receive {gun_data, Pid, Ref, nofin, Bin} ->
        receive {gun_trailers, Pid, Ref, [{<<"grpc-status">>, <<"0">>}, {<<"grpc-message">>, <<>>}]} ->
            {ok, Bin};
        {gun_trailers, Pid, Ref, [{<<"grpc-status">>, GrpcStatus}, {<<"grpc-message">>, GrpcMsg}]} ->
            {error, ?GRPC_ERROR(GrpcStatus, GrpcMsg)}
        after 2000 -> unknown
        end
    after 2000 -> unknown
    end;
resp_stream(#{stream_ref := Ref, http2_pid := Pid},
    {gun_data, Pid, Ref, nofin, Bin}) ->
    {ok, Bin};
resp_stream(#{stream_ref := SRef, http2_pid := Pid, monitor_ref := MRef},
    {gun_trailers, Pid, SRef, [{<<"grpc-status">>, GrpcStatus}, {<<"grpc-message">>, GrpcMsg}]}) ->    %% grpc error
    erlang:demonitor(MRef, [flush]),
    gun:cancel(Pid, SRef),
    {error, ?GRPC_ERROR(GrpcStatus, GrpcMsg)};
resp_stream(#{stream_ref := SRef, http2_pid := Pid, monitor_ref := MRef},
    {gun_error, Pid, SRef, Reason}) -> %% stream error
    erlang:demonitor(MRef, [flush]),
    gun:cancel(Pid, SRef),
    {error, {gun_stream_error, Reason}};
resp_stream(#{http2_pid := Pid, stream_ref := SRef, monitor_ref := MRef},
    {gun_error, Pid, Reason}) -> %% gun connection process state error
    erlang:demonitor(MRef, [flush]),
    gun:cancel(Pid, SRef),
    {error, {gun_conn_error, Reason}};
resp_stream(#{http2_pid := Pid, monitor_ref := MRef},
    {'DOWN', MRef, process, Pid, Reason}) -> %% gun connection down
    erlang:demonitor(MRef, [flush]),
    {error, {gun_down, Reason}};
resp_stream(_OCtx, _UnKnow) -> unknown.
