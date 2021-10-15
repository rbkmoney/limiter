-module(lim_config_codec).

-include_lib("limiter_proto/include/lim_limiter_config_thrift.hrl").

-export([marshal/2]).
-export([unmarshal/2]).
-export([marshal_config/1]).
-export([unmarshal_body_type/1]).
-export([unmarshal_op_behaviour/1]).
-export([unmarshal_params/1]).
-export([maybe_apply/2]).

%% Types

-type type_name() :: atom() | {list, atom()} | {set, atom()}.

-type encoded_value() :: encoded_value(any()).
-type encoded_value(T) :: T.

-type decoded_value() :: decoded_value(any()).
-type decoded_value(T) :: T.

-spec maybe_apply(any(), function()) -> any().
maybe_apply(undefined, _) ->
    undefined;
maybe_apply(Value, Fun) ->
    Fun(Value).

%% API

-spec marshal(type_name(), decoded_value()) -> encoded_value().
marshal(timestamped_change, {ev, Timestamp, Change}) ->
    #limiter_config_TimestampedChange{
        change = marshal_change(Change),
        occured_at = marshal_timestamp(Timestamp)
    }.

marshal_timestamp({DateTime, USec}) ->
    DateTimeinSeconds = genlib_time:daytime_to_unixtime(DateTime),
    {TimeinUnit, Unit} =
        case USec of
            0 ->
                {DateTimeinSeconds, second};
            USec ->
                MicroSec = erlang:convert_time_unit(DateTimeinSeconds, second, microsecond),
                {MicroSec + USec, microsecond}
        end,
    genlib_rfc3339:format_relaxed(TimeinUnit, Unit).

marshal_change({created, Config}) ->
    {created, #limiter_config_CreatedChange{limit_config = marshal_config(Config)}}.

-spec marshal_config(decoded_value()) -> encoded_value().
marshal_config(Config) ->
    #limiter_config_LimitConfig{
        id = lim_config_machine:id(Config),
        processor_type = lim_config_machine:processor_type(Config),
        description = lim_config_machine:description(Config),
        body_type = marshal_body_type(lim_config_machine:body_type(Config)),
        created_at = lim_config_machine:created_at(Config),
        started_at = lim_config_machine:started_at(Config),
        shard_size = lim_config_machine:shard_size(Config),
        time_range_type = marshal_time_range_type(lim_config_machine:time_range_type(Config)),
        context_type = marshal_context_type(lim_config_machine:context_type(Config)),
        type = maybe_apply(lim_config_machine:type(Config), fun marshal_type/1),
        scope = maybe_apply(lim_config_machine:scope(Config), fun marshal_scope/1),
        op_behaviour = maybe_apply(lim_config_machine:op_behaviour(Config), fun marshal_op_behaviour/1)
    }.

marshal_op_behaviour(OpBehaviour) ->
    PaymentRefund = maps:get(invoice_payment_refund, OpBehaviour, undefined),
    #limiter_config_OperationLimitBehaviour{
        invoice_payment_refund = maybe_apply(PaymentRefund, fun marshal_behaviour/1)
    }.

marshal_behaviour(subtraction) ->
    {subtraction, #limiter_config_Subtraction{}};
marshal_behaviour(addition) ->
    {addition, #limiter_config_Addition{}}.

marshal_body_type(amount) ->
    {amount, #limiter_config_LimitBodyTypeAmount{}};
marshal_body_type({cash, Currency}) ->
    {cash, #limiter_config_LimitBodyTypeCash{currency = Currency}}.

marshal_time_range_type({calendar, CalendarType}) ->
    {calendar, marshal_calendar_time_range_type(CalendarType)};
marshal_time_range_type({interval, Amount}) ->
    {interval, #time_range_TimeRangeTypeInterval{amount = Amount}}.

marshal_calendar_time_range_type(day) ->
    {day, #time_range_TimeRangeTypeCalendarDay{}};
marshal_calendar_time_range_type(week) ->
    {week, #time_range_TimeRangeTypeCalendarWeek{}};
marshal_calendar_time_range_type(month) ->
    {month, #time_range_TimeRangeTypeCalendarMonth{}};
marshal_calendar_time_range_type(year) ->
    {year, #time_range_TimeRangeTypeCalendarYear{}}.

marshal_context_type(payment_processing) ->
    {payment_processing, #limiter_config_LimitContextTypePaymentProcessing{}}.

marshal_type(turnover) ->
    {turnover, #limiter_config_LimitTypeTurnover{}}.

marshal_scope({scope, Type}) ->
    {scope, marshal_scope_type(Type)};
marshal_scope(global) ->
    {scope_global, #limiter_config_LimitScopeGlobal{}}.

marshal_scope_type(party) ->
    {party, #limiter_config_LimitScopeTypeParty{}};
marshal_scope_type(shop) ->
    {shop, #limiter_config_LimitScopeTypeShop{}};
marshal_scope_type(wallet) ->
    {wallet, #limiter_config_LimitScopeTypeWallet{}};
marshal_scope_type(identity) ->
    {identity, #limiter_config_LimitScopeTypeIdentity{}}.

%%

-spec unmarshal(type_name(), encoded_value()) -> decoded_value().
unmarshal(timestamped_change, TimestampedChange) ->
    Timestamp = unmarshal_timestamp(TimestampedChange#limiter_config_TimestampedChange.occured_at),
    Change = unmarshal_change(TimestampedChange#limiter_config_TimestampedChange.change),
    {ev, Timestamp, Change}.

unmarshal_timestamp(Timestamp) when is_binary(Timestamp) ->
    try
        MicroSeconds = genlib_rfc3339:parse(Timestamp, microsecond),
        case genlib_rfc3339:is_utc(Timestamp) of
            false ->
                erlang:error({bad_timestamp, not_utc}, [Timestamp]);
            true ->
                USec = MicroSeconds rem 1000000,
                DateTime = calendar:system_time_to_universal_time(MicroSeconds, microsecond),
                {DateTime, USec}
        end
    catch
        error:Error:St ->
            erlang:raise(error, {bad_timestamp, Timestamp, Error}, St)
    end.

-spec unmarshal_params(encoded_value()) -> decoded_value().
unmarshal_params(#limiter_config_LimitConfigParams{
    id = ID,
    description = Description,
    body_type = BodyType,
    started_at = StartedAt,
    shard_size = ShardSize,
    time_range_type = TimeRangeType,
    context_type = ContextType,
    type = Type,
    scope = Scope,
    op_behaviour = OpBehaviour
}) ->
    genlib_map:compact(#{
        id => ID,
        body_type => unmarshal_body_type(BodyType),
        started_at => StartedAt,
        shard_size => ShardSize,
        time_range_type => unmarshal_time_range_type(TimeRangeType),
        context_type => unmarshal_context_type(ContextType),
        type => maybe_apply(Type, fun unmarshal_type/1),
        scope => maybe_apply(Scope, fun unmarshal_scope/1),
        description => Description,
        op_behaviour => maybe_apply(OpBehaviour, fun unmarshal_op_behaviour/1)
    }).

unmarshal_change({created, #limiter_config_CreatedChange{limit_config = Config}}) ->
    {created, unmarshal_config(Config)}.

unmarshal_config(#limiter_config_LimitConfig{
    id = ID,
    processor_type = ProcessorType,
    description = Description,
    body_type = BodyType,
    created_at = CreatedAt,
    started_at = StartedAt,
    shard_size = ShardSize,
    time_range_type = TimeRangeType,
    context_type = ContextType,
    type = Type,
    scope = Scope,
    op_behaviour = OpBehaviour
}) ->
    genlib_map:compact(#{
        id => ID,
        processor_type => ProcessorType,
        created_at => lim_time:from_rfc3339(CreatedAt),
        body_type => unmarshal_body_type(BodyType),
        started_at => StartedAt,
        shard_size => ShardSize,
        time_range_type => unmarshal_time_range_type(TimeRangeType),
        context_type => unmarshal_context_type(ContextType),
        type => maybe_apply(Type, fun unmarshal_type/1),
        scope => maybe_apply(Scope, fun unmarshal_scope/1),
        description => Description,
        op_behaviour => maybe_apply(OpBehaviour, fun unmarshal_op_behaviour/1)
    }).

-spec unmarshal_op_behaviour(encoded_value()) -> decoded_value().
unmarshal_op_behaviour(OpBehaviour) ->
    #limiter_config_OperationLimitBehaviour{
        invoice_payment_refund = Refund
    } = OpBehaviour,
    genlib_map:compact(#{
        invoice_payment_refund => maybe_apply(Refund, fun unmarshal_behaviour/1)
    }).

unmarshal_behaviour({subtraction, #limiter_config_Subtraction{}}) ->
    subtraction;
unmarshal_behaviour({addition, #limiter_config_Addition{}}) ->
    addition.

-spec unmarshal_body_type(encoded_value()) -> decoded_value().
unmarshal_body_type({amount, #limiter_config_LimitBodyTypeAmount{}}) ->
    amount;
unmarshal_body_type({cash, #limiter_config_LimitBodyTypeCash{currency = Currency}}) ->
    {cash, Currency}.

unmarshal_time_range_type({calendar, CalendarType}) ->
    {calendar, unmarshal_calendar_time_range_type(CalendarType)};
unmarshal_time_range_type({interval, #time_range_TimeRangeTypeInterval{amount = Amount}}) ->
    {interval, Amount}.

unmarshal_calendar_time_range_type({day, _}) ->
    day;
unmarshal_calendar_time_range_type({week, _}) ->
    week;
unmarshal_calendar_time_range_type({month, _}) ->
    month;
unmarshal_calendar_time_range_type({year, _}) ->
    year.

unmarshal_context_type({payment_processing, #limiter_config_LimitContextTypePaymentProcessing{}}) ->
    payment_processing.

unmarshal_type({turnover, #limiter_config_LimitTypeTurnover{}}) ->
    turnover.

unmarshal_scope({scope, Type}) ->
    {scope, unmarshal_scope_type(Type)};
unmarshal_scope({scope_global, #limiter_config_LimitScopeGlobal{}}) ->
    global.

unmarshal_scope_type({party, _}) ->
    party;
unmarshal_scope_type({shop, _}) ->
    shop;
unmarshal_scope_type({wallet, _}) ->
    wallet;
unmarshal_scope_type({identity, _}) ->
    identity.

%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-spec test() -> _.

-spec marshal_unmarshal_created_test() -> _.

marshal_unmarshal_created_test() ->
    Created =
        {created, #{
            id => <<"id">>,
            processor_type => <<"type">>,
            created_at => lim_time:now(),
            body_type => {cash, <<"RUB">>},
            started_at => <<"2000-01-01T00:00:00Z">>,
            shard_size => 7,
            time_range_type => {calendar, day},
            context_type => payment_processing,
            type => turnover,
            scope => {scope, party},
            description => <<"description">>
        }},
    Event = {ev, lim_time:machinery_now(), Created},
    ?assertEqual(Event, unmarshal(timestamped_change, marshal(timestamped_change, Event))).

-endif.
