-module(eetcd_watch_h_SUITE).

-include_lib("eunit/include/eunit.hrl").

-behaviour(gen_event).

-export([init/1, handle_event/2, handle_call/2, terminate/2]).
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([test_watcher/1]).

%%====================================================================
%% gen_event handler callbacks
%%====================================================================
init(InitArgs) ->
    {ok, InitArgs}.

handle_event(Events, #{testcase := Pid} = State) ->
    Pid ! {change, Events},
    {ok, State}.

handle_call(_Request, State) ->
    {ok, ok, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% ct
%%====================================================================
all() ->
    [test_watcher].

init_per_suite(Config) ->
    application:ensure_all_started(eetcd),
    {ok, _Pid} = eetcd:open(?MODULE, ["127.0.0.1:2379", "127.0.0.1:2479", "127.0.0.1:2579"]),
    Config.

end_per_suite(_Config) ->
    ok.

test_watcher(_Config) ->
    {ok, Pid} = eetcd_watcher:start_link(?MODULE, ?MODULE, #{testcase => self()}),
    Req0 = eetcd_watch:new(),
    Req1 = eetcd_watch:with_key(Req0, <<"foo">>),
    ok = eetcd_watcher:watch(Pid, Req1),

    spawn(fun() ->
            {ok, _} = eetcd_kv:put(?MODULE, <<"foo">>, <<"bar1">>),
            {ok, _} = eetcd_kv:put(?MODULE, <<"foo">>, <<"bar2">>),
            {ok, _} = eetcd_kv:put(?MODULE, <<"foo">>, <<"bar3">>),
            {ok, _} = eetcd_kv:delete(?MODULE, <<"foo">>)
          end),

    Result = loop_recv([]),
    ?assertMatch([
        #{type := 'PUT', kv := #{key := <<"foo">>, value := <<"bar1">>}},
        #{type := 'PUT', kv := #{key := <<"foo">>, value := <<"bar2">>}},
        #{type := 'PUT', kv := #{key := <<"foo">>, value := <<"bar3">>}},
        #{type := 'DELETE', kv := #{key := <<"foo">>}}
    ], Result),
    ct:pal("Result: ~p~n", [Result]),
    eetcd_watcher:stop(Pid),

    ok.

%%====================================================================
%% Internal functions
%%====================================================================

loop_recv(Acc) ->
    receive
        {change, Events} ->
            loop_recv([Events | Acc])
    after 1000 ->
        lists:flatten(lists:reverse(Acc))
    end.
