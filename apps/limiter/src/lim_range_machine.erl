-module(lim_range_machine).

%% Accessors

-export([id/1]).
-export([created_at/1]).
-export([type/1]).
-export([ranges/1]).
-export([currency/1]).

%% API

-export([get/2]).
-export([ensure_exist/2]).
-export([get_range/2]).
-export([get_range_balance/3]).
-export([ensure_range_exist/3]).
-export([ensure_range_exist_in_state/3]).

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
-type time_range_type() :: lim_config_machine:time_range_type().
-type time_range() :: lim_config_machine:time_range().
-type currency() :: lim_config_machine:currency().

-type limit_range_state() :: #{
    id := lim_id(),
    type := time_range_type(),
    created_at := timestamp(),
    currency => currency(),
    ranges => [time_range_ext()]
}.

-type timestamped_event(T) ::
    {ev, machinery:timestamp(), T}.

-type event() ::
    {created, limit_range()}
    | {time_range_created, time_range_ext()}.

-type limit_range() :: #{
    id := lim_id(),
    type := time_range_type(),
    created_at := timestamp(),
    currency => currency()
}.

-type time_range_ext() :: #{
    account_id_from := lim_accounting:account_id(),
    account_id_to := lim_accounting:account_id(),
    upper := timestamp(),
    lower := timestamp()
}.

-type create_params() :: #{
    id := lim_id(),
    type := time_range_type(),
    created_at := timestamp(),
    currency => currency()
}.

-type range_call() ::
    {add_range, time_range()}.

-export_type([timestamped_event/1]).
-export_type([event/0]).

-define(NS, 'lim/range_v1').

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

-spec ranges(limit_range_state()) -> [time_range_ext()].
ranges(#{ranges := Ranges}) ->
    Ranges;
ranges(_State) ->
    [].

-spec currency(limit_range_state()) -> currency().
currency(#{currency := Currency}) ->
    Currency;
currency(_State) ->
    lim_accounting:get_default_currency().

%%% API

-spec get(lim_id(), lim_context()) -> {ok, limit_range_state()} | {error, notfound}.
get(ID, LimitContext) ->
    {ok, WoodyCtx} = lim_context:woody_context(LimitContext),
    get_state(ID, WoodyCtx).

-spec ensure_exist(create_params(), lim_context()) -> {ok, limit_range_state()}.
ensure_exist(Params = #{id := ID}, LimitContext) ->
    {ok, WoodyCtx} = lim_context:woody_context(LimitContext),
    case get_state(ID, WoodyCtx) of
        {ok, State} ->
            {ok, State};
        {error, notfound} ->
            _ = start(ID, Params, WoodyCtx),
            case get_state(ID, WoodyCtx) of
                {ok, State} ->
                    {ok, State};
                {error, notfound} ->
                    erlang:error({cant_get_after_start, ID})
            end
    end.

-spec get_range(time_range(), limit_range_state()) -> {ok, time_range_ext()} | {error, notfound}.
get_range(TimeRange, State) ->
    find_time_range(TimeRange, ranges(State)).

-spec get_range_balance(time_range(), limit_range_state(), lim_context()) ->
    {ok, lim_accounting:balance()}
    | {error, {range, notfound}}.
get_range_balance(TimeRange, State, LimitContext) ->
    do(fun() ->
        #{account_id_to := AccountID} = unwrap(range, find_time_range(TimeRange, ranges(State))),
        {ok, Balance} = lim_accounting:get_balance(AccountID, LimitContext),
        Balance
    end).

-spec ensure_range_exist(lim_id(), time_range(), lim_context()) ->
    {ok, time_range_ext()}
    | {error, {limit_range, notfound}}.
ensure_range_exist(ID, TimeRange, LimitContext) ->
    do(fun() ->
        {ok, WoodyCtx} = lim_context:woody_context(LimitContext),
        State = unwrap(limit_range, get_state(ID, WoodyCtx)),
        unwrap(ensure_range_exist_in_state(TimeRange, State, LimitContext))
    end).

-spec ensure_range_exist_in_state(time_range(), limit_range_state(), lim_context()) -> {ok, time_range_ext()}.
ensure_range_exist_in_state(TimeRange, State, LimitContext) ->
    {ok, WoodyCtx} = lim_context:woody_context(LimitContext),
    case find_time_range(TimeRange, ranges(State)) of
        {error, notfound} ->
            call(id(State), {add_range, TimeRange}, WoodyCtx);
        {ok, Range} ->
            {ok, Range}
    end.

%%% Machinery callbacks

-spec init(args([event()]), machine(), handler_args(), handler_opts()) -> result().
init(Events, _Machine, _HandlerArgs, _HandlerOpts) ->
    #{
        events => emit_events(Events)
    }.

-spec process_call(args(range_call()), machine(), handler_args(), handler_opts()) ->
    {response(time_range_ext()), result()} | no_return().
process_call({add_range, TimeRange0}, Machine, _HandlerArgs, #{woody_ctx := WoodyCtx}) ->
    State = collapse(Machine),
    case find_time_range(TimeRange0, ranges(State)) of
        {error, notfound} ->
            Currency = currency(State),
            {ok, LimitContext} = lim_context:create(WoodyCtx),
            {ok, AccountIDFrom} = lim_accounting:create_account(Currency, LimitContext),
            {ok, AccountIDTo} = lim_accounting:create_account(Currency, LimitContext),
            TimeRange1 = TimeRange0#{
                account_id_from => AccountIDFrom,
                account_id_to => AccountIDTo
            },
            {TimeRange1, #{events => emit_events([{time_range_created, TimeRange1}])}};
        {ok, Range} ->
            {Range, #{}}
    end.

-spec process_timeout(machine(), handler_args(), handler_opts()) -> no_return().
process_timeout(_Machine, _HandlerArgs, _HandlerOpts) ->
    not_implemented(timeout).

-spec process_repair(args(_), machine(), handler_args(), handler_opts()) -> no_return().
process_repair(_Args, _Machine, _HandlerArgs, _HandlerOpts) ->
    not_implemented(repair).

%%% Internal functions

find_time_range(_TimeRange, []) ->
    {error, notfound};
find_time_range(#{lower := Lower}, [Head = #{lower := Lower} | _Rest]) ->
    {ok, Head};
find_time_range(TimeRange, [_Head | Rest]) ->
    find_time_range(TimeRange, Rest).

%%

-spec start(lim_id(), create_params(), woody_context()) -> ok | {error, exists}.
start(ID, Params, WoodyCtx) ->
    machinery:start(?NS, ID, [{created, Params}], get_backend(WoodyCtx)).

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

-spec apply_event(machinery:event(timestamped_event(event())), lim_maybe:maybe(limit_range_state())) ->
    limit_range_state().
apply_event({_ID, _Ts, {ev, _EvTs, Event}}, Config) ->
    apply_event_(Event, Config).

-spec apply_event_(event(), lim_maybe:maybe(limit_range_state())) -> limit_range_state().
apply_event_({created, LimitRange}, undefined) ->
    LimitRange;
apply_event_({time_range_created, TimeRange}, LimitRange = #{ranges := Ranges}) ->
    LimitRange#{ranges => [TimeRange | Ranges]};
apply_event_({time_range_created, TimeRange}, LimitRange) ->
    LimitRange#{ranges => [TimeRange]}.
