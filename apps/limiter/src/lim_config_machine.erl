-module(lim_config_machine).

-include_lib("limiter_proto/include/lim_limiter_thrift.hrl").
-include_lib("limiter_proto/include/lim_base_thrift.hrl").

%% Accessors

-export([created_at/1]).
-export([id/1]).
-export([description/1]).
-export([body_type/1]).
-export([started_at/1]).
-export([shard_size/1]).
-export([time_range_type/1]).

%% API

-export([start/3]).
-export([get/2]).

-export([get_limit/2]).
-export([hold/2]).
-export([commit/2]).
-export([rollback/2]).

-export([calculate_shard_id/2]).
-export([calculate_time_range/2]).
-export([mk_scope_prefix/2]).

-type woody_context() :: woody_context:ctx().
-type lim_context() :: lim_context:t().
-type processor_type() :: lim_router:processor_type().
-type processor() :: lim_router:processor().
-type description() :: binary().

-type limit_type() :: turnover.
-type limit_scope() :: global | {scope, party | shop | wallet | identity}.
-type body_type() :: cash | amount.
-type shard_size() :: pos_integer().
-type shard_id() :: binary().
-type prefix() :: binary().
-type time_range_type() :: {calendar, year | month | week | day} | {interval, pos_integer()}.
-type time_range() :: #{
    upper := timestamp(),
    lower := timestamp()
}.

-type config() :: #{
    id := lim_id(),
    processor_type := processor_type(),
    created_at := lim_time:timestamp_ms(),
    body_type := body_type(),
    started_at := timestamp(),
    shard_size := shard_size(),
    time_range_type := time_range_type(),
    type => limit_type(),
    scope => limit_scope(),
    description => description()
}.

-type create_params() :: #{
    processor_type := processor_type(),
    body_type := body_type(),
    started_at := timestamp(),
    shard_size := shard_size(),
    time_range_type := time_range_type(),
    type => limit_type(),
    scope => limit_scope(),
    description => description()
}.

-type lim_id() :: lim_limiter_thrift:'LimitID'().
-type lim_change() :: lim_limiter_thrift:'LimitChange'().
-type limit() :: lim_limiter_thrift:'Limit'().
-type timestamp() :: lim_base_thrift:'Timestamp'().

-type scope_prefix_error() :: {failed_to_find_data_for, party | shop | wallet | identity}.

-export_type([config/0]).
-export_type([body_type/0]).
-export_type([limit_type/0]).
-export_type([limit_scope/0]).
-export_type([time_range_type/0]).
-export_type([time_range/0]).
-export_type([create_params/0]).
-export_type([lim_id/0]).
-export_type([lim_change/0]).
-export_type([limit/0]).
-export_type([timestamp/0]).
-export_type([state/0]).
-export_type([scope_prefix_error/0]).

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

-define(NS, 'lim_config/v1').

%% Handler behaviour

-callback get_limit(
    ID :: lim_id(),
    Config :: config(),
    LimitContext :: lim_context()
) -> {ok, limit()} | {error, get_limit_error()}.

-callback hold(
    LimitChange :: lim_change(),
    Config :: config(),
    LimitContext :: lim_context()
) -> ok | {error, hold_error()}.

-callback commit(
    LimitChange :: lim_change(),
    Config :: config(),
    LimitContext :: lim_context()
) -> ok | {error, commit_error()}.

-callback rollback(
    LimitChange :: lim_change(),
    Config :: config(),
    LimitContext :: lim_context()
) -> ok | {error, rollback_error()}.

-type get_limit_error() :: lim_turnover_processor:get_limit_error().
-type hold_error() :: lim_turnover_processor:hold_error().
-type commit_error() :: lim_turnover_processor:commit_error().
-type rollback_error() :: lim_turnover_processor:rollback_error().

-type config_error() :: {handler | config, notfound}.

-import(lim_pipeline, [do/1, unwrap/1, unwrap/2]).

%% Accessors

-spec created_at(config()) -> timestamp().
created_at(#{created_at := CreatedAt}) ->
    lim_time:to_rfc3339(CreatedAt).

-spec id(config()) -> lim_id().
id(#{id := ID}) ->
    ID.

-spec description(config()) -> lim_maybe:maybe(description()).
description(#{description := ID}) ->
    ID;
description(_) ->
    undefined.

-spec body_type(config()) -> body_type().
body_type(#{body_type := BodyType}) ->
    BodyType.

-spec started_at(config()) -> timestamp().
started_at(#{started_at := Value}) ->
    Value.

-spec shard_size(config()) -> shard_size().
shard_size(#{shard_size := Value}) ->
    Value.

-spec time_range_type(config()) -> time_range_type().
time_range_type(#{time_range_type := Value}) ->
    Value.

%%

-spec start(lim_id(), create_params(), lim_context()) -> {ok, config()}.
start(ID, Params, LimitContext) ->
    {ok, WoodyCtx} = lim_context:woody_context(LimitContext),
    Config = genlib_map:compact(Params#{id => ID, created_at => lim_time:now()}),
    _ = machinery:start(?NS, ID, Config, get_backend(WoodyCtx)),
    {ok, Config}.

-spec get(lim_id(), lim_context()) -> {ok, config()} | {error, notfound}.
get(ID, LimitContext) ->
    do(fun() ->
        {ok, WoodyCtx} = lim_context:woody_context(LimitContext),
        Machine = unwrap(machinery:get(?NS, ID, get_backend(WoodyCtx))),
        #{
            config := Config
        } = get_machine_state(Machine),
        Config
    end).

-spec get_limit(lim_id(), lim_context()) -> {ok, limit()} | {error, config_error() | {processor(), get_limit_error()}}.
get_limit(ID, LimitContext) ->
    do(fun() ->
        Config = #{processor := Processor} = unwrap(config, get(ID, LimitContext)),
        Handler = unwrap(handler, lim_router:get_handler(Processor)),
        unwrap(Handler, Handler:get_limit(ID, Config, LimitContext))
    end).

-spec hold(lim_change(), lim_context()) -> ok | {error, config_error() | {processor(), hold_error()}}.
hold(LimitChange = #limiter_LimitChange{id = ID}, LimitContext) ->
    do(fun() ->
        Config = #{processor := Processor} = unwrap(config, get(ID, LimitContext)),
        Handler = unwrap(handler, lim_router:get_handler(Processor)),
        unwrap(Handler, Handler:hold(LimitChange, Config, LimitContext))
    end).

-spec commit(lim_change(), lim_context()) -> ok | {error, config_error() | {processor(), commit_error()}}.
commit(LimitChange = #limiter_LimitChange{id = ID}, LimitContext) ->
    do(fun() ->
        Config = #{processor := Processor} = unwrap(config, get(ID, LimitContext)),
        Handler = unwrap(handler, lim_router:get_handler(Processor)),
        unwrap(Handler, Handler:commit(LimitChange, Config, LimitContext))
    end).

-spec rollback(lim_change(), lim_context()) -> ok | {error, config_error() | {processor(), rollback_error()}}.
rollback(LimitChange = #limiter_LimitChange{id = ID}, LimitContext) ->
    do(fun() ->
        Config = #{processor := Processor} = unwrap(config, get(ID, LimitContext)),
        Handler = unwrap(handler, lim_router:get_handler(Processor)),
        unwrap(Handler, Handler:rollback(LimitChange, Config, LimitContext))
    end).

-spec calculate_time_range(timestamp(), config()) -> time_range().
calculate_time_range(Timestamp, Config) ->
    StartedAt = started_at(Config),
    {{StartDate, StartTime}, USec} = lim_range_codec:parse_timestamp(StartedAt),
    {{CurrentDate, CurrentTime}, USec} = lim_range_codec:parse_timestamp(Timestamp),
    CurrentSec = calendar:datetime_to_gregorian_seconds({CurrentDate, CurrentTime}),
    case time_range_type(Config) of
        {calendar, Range} ->
            case Range of
                year ->
                    {_StartYear, StartMonth, StartDay} = StartDate,
                    {CurrentYear, _CurrentMonth, _} = CurrentDate,
                    ClampedStartDay = clamp_month_start_day(CurrentYear, StartMonth, StartDay),
                    LowerSec = calendar:datetime_to_gregorian_seconds(
                        {{CurrentYear, StartMonth, ClampedStartDay}, StartTime}
                    ),
                    NextYearDay = clamp_month_start_day(CurrentYear + 1, StartMonth, StartDay),
                    UpperSec = calendar:datetime_to_gregorian_seconds(
                        {{CurrentYear + 1, StartMonth, NextYearDay}, StartTime}
                    ),
                    calculate_year_time_range(CurrentSec, LowerSec, UpperSec);
                month ->
                    {_StartYear, _StartMonth, StartDay} = StartDate,
                    {CurrentYear, CurrentMonth, _} = CurrentDate,
                    ClampedStartDay = clamp_month_start_day(CurrentYear, CurrentMonth, StartDay),
                    LowerSec = calendar:datetime_to_gregorian_seconds(
                        {{CurrentYear, CurrentMonth, ClampedStartDay}, StartTime}
                    ),
                    UpperSec =
                        case CurrentMonth < 12 of
                            true ->
                                NextMonthDay = clamp_month_start_day(CurrentYear, CurrentMonth + 1, StartDay),
                                calendar:datetime_to_gregorian_seconds(
                                    {{CurrentYear, CurrentMonth + 1, NextMonthDay}, StartTime}
                                );
                            false ->
                                NextYearDay = clamp_month_start_day(CurrentYear + 1, CurrentMonth, StartDay),
                                calendar:datetime_to_gregorian_seconds(
                                    {{CurrentYear + 1, 1, NextYearDay}, StartTime}
                                )
                        end,
                    calculate_month_time_range(CurrentSec, LowerSec, UpperSec);
                week ->
                    StartWeekRem = calendar:date_to_gregorian_days(StartDate) rem 7,
                    LowerWeek = (calendar:date_to_gregorian_days(CurrentDate) div 7) * 7 + StartWeekRem,
                    UpperWeek = LowerWeek + 7,
                    LowerSec = calendar:datetime_to_gregorian_seconds(
                        {calendar:gregorian_days_to_date(LowerWeek), StartTime}
                    ),
                    UpperSec = calendar:datetime_to_gregorian_seconds(
                        {calendar:gregorian_days_to_date(UpperWeek), StartTime}
                    ),
                    calculate_week_time_range(CurrentSec, LowerSec, UpperSec);
                day ->
                    Lower = calendar:date_to_gregorian_days(CurrentDate),
                    UpperDate = calendar:gregorian_days_to_date(Lower + 1),
                    LowerSec = calendar:datetime_to_gregorian_seconds({CurrentDate, StartTime}),
                    UpperSec = calendar:datetime_to_gregorian_seconds({UpperDate, StartTime}),
                    calculate_day_time_range(CurrentSec, LowerSec, UpperSec)
            end;
        {interval, _Interval} ->
            erlang:error({interval_time_range_not_implemented, Config})
    end.

clamp_month_start_day(Year, Month, StartDay) ->
    Last = calendar:last_day_of_the_month(Year, Month),
    case StartDay > Last of
        true ->
            Last;
        false ->
            StartDay
    end.

calculate_year_time_range(CurrentSec, LowerSec, UpperSec) when
    CurrentSec >= LowerSec andalso
        CurrentSec < UpperSec
->
    mk_time_range(LowerSec, UpperSec);
calculate_year_time_range(CurrentSec, LowerSec, _UpperSec) when CurrentSec < LowerSec ->
    {{Year, Month, Day}, Time} = calendar:gregorian_seconds_to_datetime(LowerSec),
    PrevYearDay = clamp_month_start_day(Year - 1, Month, Day),
    LowerDate = {Year - 1, Month, PrevYearDay},
    #{
        lower => marshal_timestamp({LowerDate, Time}),
        upper => marshal_timestamp(calendar:gregorian_seconds_to_datetime(LowerSec))
    }.

calculate_month_time_range(CurrentSec, LowerSec, UpperSec) when
    CurrentSec >= LowerSec andalso
        CurrentSec < UpperSec
->
    mk_time_range(LowerSec, UpperSec);
calculate_month_time_range(CurrentSec, LowerSec, _UpperSec) when CurrentSec < LowerSec ->
    {{Year, Month, Day}, Time} = calendar:gregorian_seconds_to_datetime(LowerSec),
    LowerDate =
        case Month =:= 1 of
            true ->
                PrevYearDay = clamp_month_start_day(Year - 1, 12, Day),
                {Year - 1, 12, PrevYearDay};
            false ->
                PrevMonthDay = clamp_month_start_day(Year, Month - 1, Day),
                {Year, Month - 1, PrevMonthDay}
        end,
    #{
        lower => marshal_timestamp({LowerDate, Time}),
        upper => marshal_timestamp(calendar:gregorian_seconds_to_datetime(LowerSec))
    }.

calculate_week_time_range(CurrentSec, LowerSec, UpperSec) when
    CurrentSec >= LowerSec andalso
        CurrentSec < UpperSec
->
    mk_time_range(LowerSec, UpperSec);
calculate_week_time_range(CurrentSec, LowerSec, _UpperSec) when CurrentSec < LowerSec ->
    {Date, Time} = calendar:gregorian_seconds_to_datetime(LowerSec),
    Days = calendar:date_to_gregorian_days(Date),
    LowerDate = calendar:gregorian_days_to_date(Days - 7),
    #{
        lower => marshal_timestamp({LowerDate, Time}),
        upper => marshal_timestamp(calendar:gregorian_seconds_to_datetime(LowerSec))
    }.

calculate_day_time_range(CurrentSec, LowerSec, UpperSec) when
    CurrentSec >= LowerSec andalso
        CurrentSec < UpperSec
->
    mk_time_range(LowerSec, UpperSec);
calculate_day_time_range(CurrentSec, LowerSec, _UpperSec) when CurrentSec < LowerSec ->
    {Date, Time} = calendar:gregorian_seconds_to_datetime(LowerSec),
    Days = calendar:date_to_gregorian_days(Date),
    LowerDate = calendar:gregorian_days_to_date(Days - 1),
    #{
        lower => marshal_timestamp({LowerDate, Time}),
        upper => marshal_timestamp(calendar:gregorian_seconds_to_datetime(LowerSec))
    }.

mk_time_range(LowerSec, UpperSec) ->
    #{
        lower => marshal_timestamp(calendar:gregorian_seconds_to_datetime(LowerSec)),
        upper => marshal_timestamp(calendar:gregorian_seconds_to_datetime(UpperSec))
    }.

marshal_timestamp(DateTime) ->
    lim_range_codec:marshal(timestamp, {DateTime, 0}).

-spec calculate_shard_id(timestamp(), config()) -> shard_id().
calculate_shard_id(Timestamp, Config) ->
    StartedAt = started_at(Config),
    ShardSize = shard_size(Config),
    {{StartDate, _}, USec} = lim_range_codec:parse_timestamp(StartedAt),
    {{CurrentDate, _}, USec} = lim_range_codec:parse_timestamp(Timestamp),
    case time_range_type(Config) of
        {calendar, Range} ->
            Units =
                case Range of
                    year ->
                        {StartYear, _, _} = StartDate,
                        {CurrentYear, _, _} = CurrentDate,
                        CurrentYear - StartYear;
                    month ->
                        {StartYear, StartMonth, _} = StartDate,
                        {CurrentYear, CurrentMonth, _} = CurrentDate,
                        YearDiff = CurrentYear - StartYear,
                        MonthDiff = CurrentMonth - StartMonth,
                        YearDiff * 12 + MonthDiff;
                    week ->
                        StartWeeks = calendar:date_to_gregorian_days(StartDate) div 7,
                        CurrentWeeks = calendar:date_to_gregorian_days(CurrentDate) div 7,
                        CurrentWeeks - StartWeeks;
                    day ->
                        StartDays = calendar:date_to_gregorian_days(StartDate),
                        CurrentDays = calendar:date_to_gregorian_days(CurrentDate),
                        CurrentDays - StartDays
                end,
            SignPrefix = mk_sign_prefix(Units),
            RangePrefix = mk_prefix(Range),
            mk_shard_id(<<SignPrefix/binary, "/", RangePrefix/binary>>, Units, ShardSize);
        {interval, _Interval} ->
            erlang:error({interval_time_range_not_implemented, Config})
    end.

mk_prefix(day) -> <<"day">>;
mk_prefix(week) -> <<"week">>;
mk_prefix(month) -> <<"month">>;
mk_prefix(year) -> <<"year">>.

mk_sign_prefix(Units) when Units >= 0 -> <<"future">>;
mk_sign_prefix(_) -> <<"past">>.

mk_shard_id(Prefix, Units0, ShardSize) ->
    Units1 = abs(Units0),
    ID = list_to_binary(integer_to_list(Units1 div ShardSize)),
    <<Prefix/binary, "/", ID/binary>>.

-spec mk_scope_prefix(config(), lim_context()) -> {ok, prefix()} | {error, scope_prefix_error()}.
mk_scope_prefix(#{scope := global}, _LimitContext) ->
    {ok, <<>>};
mk_scope_prefix(#{scope := {scope, party}}, LimitContext) ->
    case lim_context:party_id(LimitContext) of
        {ok, PartyID} ->
            {ok, <<"/", PartyID/binary>>};
        {error, notfound} ->
            {error, {failed_to_find_data_for, party}}
    end;
mk_scope_prefix(#{scope := {scope, shop}}, LimitContext) ->
    case lim_context:party_id(LimitContext) of
        {ok, PartyID} ->
            case lim_context:shop_id(LimitContext) of
                {ok, ShopID} ->
                    {ok, <<"/", PartyID/binary, "/", ShopID/binary>>};
                {error, notfound} ->
                    {error, {failed_to_find_data_for, shop}}
            end;
        {error, notfound} ->
            {error, {failed_to_find_data_for, party}}
    end;
mk_scope_prefix(#{scope := {scope, wallet}}, LimitContext) ->
    case lim_context:wallet_id(LimitContext) of
        {ok, WalletID} ->
            {ok, <<"/", WalletID/binary>>};
        {error, notfound} ->
            {error, {failed_to_find_data_for, wallet}}
    end;
mk_scope_prefix(#{scope := {scope, identity}}, LimitContext) ->
    case lim_context:identity_id(LimitContext) of
        {ok, IdentityID} ->
            {ok, <<"/", IdentityID/binary>>};
        {error, notfound} ->
            {error, {failed_to_find_data_for, identity}}
    end.

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
    lim_utils:get_backend(?NS, WoodyCtx).

-spec not_implemented(any()) -> no_return().
not_implemented(What) ->
    erlang:error({not_implemented, What}).
