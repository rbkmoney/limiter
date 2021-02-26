-module(lim_range_machine).

%% Accessors

-export([id/1]).
-export([created_at/1]).
-export([type/1]).
-export([ranges/1]).

%% API

-export([get/2]).
-export([ensure_exist/2]).
-export([add_range/3]).
-export([get_range/2]).
-export([get_range_balance/3]).
-export([ensure_range_exist/3]).
-export([ensure_range_exist_/3]).

%% Machinery callbacks

-behaviour(machinery).

-export([init/4]).
-export([process_call/4]).
-export([process_timeout/3]).
-export([process_repair/4]).

-type args(T) :: machinery:args(T).
-type machine() :: machinery:machine(event(), _).
-type handler_args() :: machinery:handler_args(_).
-type handler_opts() :: machinery:handler_opts(_).
-type result() :: machinery:result(timestamped_event(event()), none()).
-type response(T) :: machinery:response(T).

%%

-type woody_context() :: woody_context:ctx().
-type lim_context() :: lim_context:t().
-type timestamp() :: lim_config_machine:timestamp().
-type lim_id() :: lim_config_machine:lim_id().

-type limit_range_state() :: #{
    id := lim_id(),
    type := time_range_type(),
    created_at := timestamp(),
    ranges => [time_range()]
}.

-type timestamped_event(T) ::
    {ev, timestamp(), T}.

-type event() ::
    {created, limit_range()}
    | {time_range_created, time_range()}.

-type limit_range() :: #{
    id := lim_id(),
    type := time_range_type(),
    created_at := timestamp()
}.

-type time_range() :: #{
    account_id := lim_accounting:account_id(),
    upper := timestamp(),
    lower := timestamp()
}.

-type time_range_type() :: month | infinity.

-type create_params() :: #{
    id := lim_id(),
    type := time_range_type(),
    created_at := timestamp()
}.

-type range_call() ::
    {add_range, timestamp()}.

-type add_range_error() ::
    {inconsistent_timestamp, timestamp()}.

-export_type([timestamped_event/1]).
-export_type([event/0]).

-define(NS, 'lim_range/v1').

-import(lim_pipeline, [do/1, unwrap/1, unwrap/2]).

%% Accessors

-spec id(limit_range_state()) -> lim_id().
id(State) ->
    maps:get(id, State).

-spec created_at(limit_range_state()) -> timestamp().
created_at(State) ->
    maps:get(created_at, State).

-spec type(limit_range_state()) -> time_range_type().
type(State) ->
    maps:get(type, State).

-spec ranges(limit_range_state()) -> [time_range()].
ranges(#{ranges := Ranges}) ->
    Ranges;
ranges(_State) ->
    [].

%%% API

-spec get(lim_id(), lim_context()) -> {ok, limit_range_state()} | {error, notfound}.
get(ID, LimitContext) ->
    do(fun() ->
        {ok, WoodyCtx} = lim_context:woody_context(LimitContext),
        unwrap(get_state(ID, WoodyCtx))
    end).

-spec ensure_exist(create_params(), lim_context()) -> {ok, limit_range_state()}.
ensure_exist(Params = #{id := ID}, LimitContext) ->
    {ok, WoodyCtx} = lim_context:woody_context(LimitContext),
    case get_state(ID, WoodyCtx) of
        {ok, State} ->
            State;
        {error, notfound} ->
            _ = start(ID, Params, WoodyCtx),
            case get_state(ID, WoodyCtx) of
                {ok, State} ->
                    {ok, State};
                {error, notfound} ->
                    erlang:error({cant_get_after_start, ID})
            end
    end.

-spec add_range(lim_id(), timestamp(), lim_context()) -> ok | {error, notfound}.
add_range(ID, StartTimestamp, LimitContext) ->
    do(fun() ->
        {ok, WoodyCtx} = lim_context:woody_context(LimitContext),
        unwrap(call(ID, {add_range, StartTimestamp}, WoodyCtx))
    end).

-spec get_range(timestamp(), limit_range_state()) -> {ok, time_range()} | {error, notfound}.
get_range(Timestamp, State) ->
    find_time_range(Timestamp, ranges(State)).

-spec get_range_balance(timestamp(), limit_range_state(), lim_context()) ->
    {ok, lim_accounting:balance()}
    | {error, {range | account, notfound}}.
get_range_balance(Timestamp, State, LimitContext) ->
    do(fun() ->
        #{account_id := AccountID} = unwrap(range, find_time_range(Timestamp, ranges(State))),
        unwrap(account, lim_accounting:get_balance(AccountID, LimitContext))
    end).

-spec ensure_range_exist(lim_id(), timestamp(), lim_context()) ->
    {ok, time_range()}
    | {error,
        {limit_range, notfound}
        | {inconsistent_timestamp, timestamp()}}.
ensure_range_exist(ID, StartTimestamp, LimitContext) ->
    do(fun() ->
        {ok, WoodyCtx} = lim_context:woody_context(LimitContext),
        State = unwrap(limit_range, get_state(ID, WoodyCtx)),
        unwrap(ensure_range_exist_(StartTimestamp, State, LimitContext))
    end).

-spec ensure_range_exist_(timestamp(), limit_range_state(), lim_context()) ->
    {ok, time_range()}
    | {error, {inconsistent_timestamp, timestamp()}}.
ensure_range_exist_(StartTimestamp, State, LimitContext) ->
    do(fun() ->
        {ok, WoodyCtx} = lim_context:woody_context(LimitContext),
        case find_time_range(StartTimestamp, ranges(State)) of
            {error, notfound} ->
                unwrap(call(id(State), {add_range, StartTimestamp}, WoodyCtx));
            {ok, Range} ->
                {ok, Range}
        end
    end).

%%% Machinery callbacks

-spec init(args([event()]), machine(), handler_args(), handler_opts()) -> result().
init(Events, _Machine, _HandlerArgs, _HandlerOpts) ->
    #{
        events => emit_events(Events)
    }.

-spec process_call(args(range_call()), machine(), handler_args(), handler_opts()) ->
    {response({ok, time_range()} | add_range_error()), result()} | no_return().
process_call({add_range, StartTimestamp}, Machine, _HandlerArgs, _HandlerOpts) ->
    State = collapse(Machine),
    case assert_timestamp(StartTimestamp, created_at(State)) of
        true ->
            %% TODO: Check this range aleady exists befor add
            {ok, AccountID} = lim_accounting:create_account(),
            TimeRange = #{
                account_id => AccountID,
                upper => StartTimestamp,
                lower => mk_lower_time_range_edge(StartTimestamp, type(State))
            },
            {{ok, TimeRange}, #{events => emit_events([{time_range_created, TimeRange}])}};
        false ->
            {{inconsistent_timestamp, StartTimestamp}, #{}}
    end;
process_call(_Args, _Machine, _HandlerArgs, _HandlerOpts) ->
    not_implemented(call).

-spec process_timeout(machine(), handler_args(), handler_opts()) -> no_return().
process_timeout(_Machine, _HandlerArgs, _HandlerOpts) ->
    not_implemented(timeout).

-spec process_repair(args(_), machine(), handler_args(), handler_opts()) -> no_return().
process_repair(_Args, _Machine, _HandlerArgs, _HandlerOpts) ->
    not_implemented(repair).

%%% Internal functions

find_time_range(_Timestamp, []) ->
    {error, notfound};
find_time_range(_Timestamp, _Ranges = [Head | _Rest]) ->
    %% TODO: some logic here)
    % (lim_time:from_rfc3339(Timestamp) >= lim_time:from_rfc3339(Lower)) and
    %     (lim_time:from_rfc3339(Timestamp) < lim_time:from_rfc3339(Upper))
    {ok, Head}.

mk_lower_time_range_edge(StartTimestamp, _TimeRangeType) ->
    %% TODO: some logic here)
    StartTimestamp.

%%

-spec start(lim_id(), create_params(), woody_context()) -> ok | {error, exists}.
start(ID, Params, WoodyCtx) ->
    machinery:start(?NS, ID, Params, get_backend(WoodyCtx)).

-spec call(lim_id(), range_call(), woody_context()) -> {ok, response(_)} | {error, notfound}.
call(ID, Msg, WoodyCtx) ->
    machinery:call(?NS, ID, Msg, get_backend(WoodyCtx)).

-spec get_state(lim_id(), woody_context()) -> {ok, limit_range_state()} | {error, notfound}.
get_state(ID, WoodyCtx) ->
    case machinery:get(?NS, ID, get_backend(WoodyCtx)) of
        {ok, Machine} ->
            {ok, collapse(Machine)};
        {error, notfound} = Error ->
            Error
    end.

emit_events(Events) ->
    emit_timestamped_events(Events, lim_time:machinery_now()).

emit_timestamped_events(Events, Ts) ->
    [{ev, Ts, Body} || Body <- Events].

collapse(#{history := History}) ->
    lists:foldl(fun(Ev, St) -> apply_event(Ev, St) end, undefined, History).

-spec get_backend(woody_context()) -> machinery_mg_backend:backend().
get_backend(WoodyCtx) ->
    lim_utils:get_backend(?NS, WoodyCtx).

-spec not_implemented(any()) -> no_return().
not_implemented(What) ->
    erlang:error({not_implemented, What}).

%%

assert_timestamp(StartTimestamp, CreateAt) ->
    lim_time:from_rfc3339(StartTimestamp) >= lim_time:from_rfc3339(CreateAt).

-spec apply_event(event(), lim_maybe:maybe(limit_range_state())) -> limit_range_state().
apply_event({created, LimitRange}, undefined) ->
    LimitRange;
apply_event({time_range_created, TimeRange}, LimitRange = #{ranges := Ranges}) ->
    LimitRange#{ranges => [TimeRange | Ranges]};
apply_event({time_range_created, TimeRange}, LimitRange) ->
    LimitRange#{ranges => [TimeRange]}.
