-module(lim_config_machine).

-include_lib("limiter_proto/include/lim_limiter_thrift.hrl").
-include_lib("limiter_proto/include/lim_base_thrift.hrl").

%% Accessors

-export([created_at/1]).

%% API

-export([start/2]).
-export([get/2]).

-export([get_limit/2]).
-export([hold/2]).
-export([commit/2]).
-export([rollback/2]).

-type woody_context() :: woody_context:ctx().
-type lim_context() :: lim_context:t().
-type processor_type() :: binary().

-type config() :: #{
    id := lim_id(),
    processor_type := processor_type(),
    created_at := lim_time:timestamp_ms(),
    type => turnover,
    scope => global,
    body_type => cash,
    time_range => month
}.

-type create_params() :: #{
    processor_type := processor_type(),
    type => turnover,
    scope => global,
    body_type => cash,
    time_range => month
}.

-type lim_id() :: lim_limiter_thrift:'LimitID'().
-type lim_change() :: lim_limiter_thrift:'LimitChange'().
-type limit() :: lim_limiter_thrift:'Limit'().
-type timestamp() :: lim_base_thrift:'Timestamp'().

-export_type([config/0]).
-export_type([create_params/0]).
-export_type([lim_id/0]).
-export_type([lim_change/0]).
-export_type([limit/0]).
-export_type([timestamp/0]).

%% Machinery callbacks

-behaviour(machinery).

-export([init/4]).
-export([process_call/4]).
-export([process_timeout/3]).
-export([process_repair/4]).

-type state() :: #{
    config := config()
}.

-type args(T) :: machinery:args(T).
-type machine() :: machinery:machine(_, state()).
-type handler_args() :: machinery:handler_args(_).
-type handler_opts() :: machinery:handler_opts(_).
-type result(A) :: machinery:result(none(), A).

-define(NS, lim_config).

%% Handler behaviour

-callback get_limit(
    ID :: lim_id(),
    Config :: config(),
    LimitContext :: lim_context()
) -> _GetResult.

-callback hold(
    LimitChange :: lim_change(),
    Config :: config(),
    LimitContext :: lim_context()
) -> _HoldResult.

-callback commit(
    LimitChange :: lim_change(),
    Config :: config(),
    LimitContext :: lim_context()
) -> _CommitResult.

-callback rollback(
    LimitChange :: lim_change(),
    Config :: config(),
    LimitContext :: lim_context()
) -> _RollbackResult.

-import(lim_pipeline, [do/1, unwrap/1, unwrap/2]).

%% Accessors

-spec created_at(config()) -> lim_maybe:maybe(timestamp()).
created_at(#{created_at := CreatedAt}) ->
    lim_time:to_rfc3339(CreatedAt);
created_at(_) ->
    undefined.

%%

-spec start(create_params(), lim_context()) -> {ok, config()} | {error, notfound}.
start(Params, LimitContext) ->
    do(fun() ->
        {ok, WoodyCtx} = lim_context:woody_context(LimitContext),
        {ok, {ID, _}} = bender_generator_client:gen_snowflake(WoodyCtx),
        _ = machinery:start(
            ?NS,
            ID,
            Params#{id => ID, created_at => lim_time:now()},
            get_backend(WoodyCtx)
        ),
        unwrap(get(ID, LimitContext))
    end).

-spec get(lim_id(), lim_context()) ->
    {ok, config()} | {error, {limit, notfound}}.
get(ID, LimitContext) ->
    do(fun() ->
        {ok, WoodyCtx} = lim_context:woody_context(LimitContext),
        Machine = unwrap(limit, machinery:get(?NS, ID, get_backend(WoodyCtx))),
        #{
            config := Config
        } = get_machine_state(Machine),
        Config
    end).

-spec get_limit(lim_id(), lim_context()) ->
    _GetResult.
get_limit(ID, LimitContext) ->
    do(fun() ->
        Config = #{processor := Processor} = unwrap(get(ID, LimitContext)),
        Handler = unwrap(handler, lim_router:get_handler(Processor)),
        Handler:get_limit(ID, Config, LimitContext)
    end).

-spec hold(lim_change(), lim_context()) ->
    _HoldResult.
hold(LimitChange = #limiter_LimitChange{id = ID}, LimitContext) ->
    do(fun() ->
        Config = #{processor := Processor} = unwrap(get(ID, LimitContext)),
        Handler = unwrap(handler, lim_router:get_handler(Processor)),
        Handler:hold(LimitChange, Config, LimitContext)
    end).

-spec commit(lim_change(), lim_context()) ->
    _CommitResult.
commit(LimitChange = #limiter_LimitChange{id = ID}, LimitContext) ->
    do(fun() ->
        Config = #{processor := Processor} = unwrap(get(ID, LimitContext)),
        Handler = unwrap(handler, lim_router:get_handler(Processor)),
        Handler:commit(LimitChange, Config, LimitContext)
    end).

-spec rollback(lim_change(), lim_context()) ->
    _RollbackResult.
rollback(LimitChange = #limiter_LimitChange{id = ID}, LimitContext) ->
    do(fun() ->
        Config = #{processor := Processor} = unwrap(get(ID, LimitContext)),
        Handler = unwrap(handler, lim_router:get_handler(Processor)),
        Handler:rollback(LimitChange, Config, LimitContext)
    end).

%%% Machinery callbacks

-spec init(args(config()), machine(), handler_args(), handler_opts()) -> result(state()).
init(Config, _Machine, _HandlerArgs, _HandlerOpts) ->
    #{
        aux_state => #{
            config => Config
        }
    }.

-spec process_call(args(_), machine(), handler_args(), handler_opts()) -> no_return().
process_call(_Args, _Machine, _HandlerArgs, _HandlerOpts) ->
    not_implemented(call).

-spec process_timeout(machine(), handler_args(), handler_opts()) -> no_return().
process_timeout(_Machine, _HandlerArgs, _HandlerOpts) ->
    not_implemented(timeout).

-spec process_repair(args(_), machine(), handler_args(), handler_opts()) -> no_return().
process_repair(_Args, _Machine, _HandlerArgs, _HandlerOpts) ->
    not_implemented(repair).

%%% Internal functions

-spec get_machine_state(machine()) -> state().
get_machine_state(#{aux_state := State}) ->
    State.

-spec get_backend(woody_context()) -> machinery_mg_backend:backend().
get_backend(WoodyCtx) ->
    lim_utils:get_backend(config, WoodyCtx).

-spec not_implemented(any()) -> no_return().
not_implemented(What) ->
    erlang:error({not_implemented, What}).
