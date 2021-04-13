-module(lim_range_codec).

-include_lib("limiter_proto/include/lim_limiter_range_thrift.hrl").

-export([marshal/2]).
-export([unmarshal/2]).
-export([parse_timestamp/1]).

%% Types

-type type_name() :: atom() | {list, atom()} | {set, atom()}.

-type encoded_value() :: encoded_value(any()).
-type encoded_value(T) :: T.

-type decoded_value() :: decoded_value(any()).
-type decoded_value(T) :: T.

%% API

-spec marshal(type_name(), decoded_value()) -> encoded_value().
marshal(timestamped_change, {ev, Timestamp, Change}) ->
    #limiter_range_TimestampedChange{
        change = marshal(change, Change),
        occured_at = marshal(timestamp, Timestamp)
    };
marshal(change, {created, Range}) ->
    {created, #limiter_range_CreatedChange{limit_range = marshal(range, Range)}};
marshal(change, {time_range_created, TimeRange}) ->
    {time_range_created, #limiter_range_TimeRangeCreatedChange{time_range = marshal(time_range, TimeRange)}};
marshal(
    range,
    Range = #{
        id := ID,
        type := Type,
        created_at := CreatedAt
    }
) ->
    #limiter_range_LimitRange{
        id = ID,
        type = marshal(time_range_type, Type),
        created_at = CreatedAt,
        currency = lim_range_machine:currency(Range)
    };
marshal(time_range, #{
    upper := Upper,
    lower := Lower,
    account_id_from := AccountIDFrom,
    account_id_to := AccountIDTo
}) ->
    #time_range_TimeRange{
        upper = Upper,
        lower = Lower,
        account_id_from = AccountIDFrom,
        account_id_to = AccountIDTo
    };
marshal(time_range_type, {calendar, SubType}) ->
    {calendar, marshal(time_range_sub_type, SubType)};
marshal(time_range_type, {interval, Interval}) ->
    {interval, #time_range_TimeRangeTypeInterval{amount = Interval}};
marshal(time_range_sub_type, year) ->
    {year, #time_range_TimeRangeTypeCalendarYear{}};
marshal(time_range_sub_type, month) ->
    {month, #time_range_TimeRangeTypeCalendarMonth{}};
marshal(time_range_sub_type, week) ->
    {week, #time_range_TimeRangeTypeCalendarWeek{}};
marshal(time_range_sub_type, day) ->
    {day, #time_range_TimeRangeTypeCalendarDay{}};
marshal(timestamp, {DateTime, USec}) ->
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

-spec unmarshal(type_name(), encoded_value()) -> decoded_value().
unmarshal(timestamped_change, TimestampedChange) ->
    Timestamp = unmarshal(timestamp, TimestampedChange#limiter_range_TimestampedChange.occured_at),
    Change = unmarshal(change, TimestampedChange#limiter_range_TimestampedChange.change),
    {ev, Timestamp, Change};
unmarshal(change, {created, #limiter_range_CreatedChange{limit_range = Range}}) ->
    {created, unmarshal(range, Range)};
unmarshal(change, {time_range_created, #limiter_range_TimeRangeCreatedChange{time_range = Range}}) ->
    {time_range_created, unmarshal(time_range, Range)};
unmarshal(range, #limiter_range_LimitRange{
    id = ID,
    type = Type,
    created_at = CreatedAt,
    currency = Currency
}) ->
    genlib_map:compact(#{
        id => ID,
        type => unmarshal(time_range_type, Type),
        created_at => CreatedAt,
        currency => Currency
    });
unmarshal(time_range, #time_range_TimeRange{
    upper = Upper,
    lower = Lower,
    account_id_from = AccountIDFrom,
    account_id_to = AccountIDTo
}) ->
    #{
        upper => Upper,
        lower => Lower,
        account_id_from => AccountIDFrom,
        account_id_to => AccountIDTo
    };
unmarshal(time_range_type, {calendar, SubType}) ->
    {calendar, unmarshal(time_range_sub_type, SubType)};
unmarshal(time_range_type, {interval, #time_range_TimeRangeTypeInterval{amount = Interval}}) ->
    {interval, Interval};
unmarshal(time_range_sub_type, {year, _}) ->
    year;
unmarshal(time_range_sub_type, {month, _}) ->
    month;
unmarshal(time_range_sub_type, {week, _}) ->
    week;
unmarshal(time_range_sub_type, {day, _}) ->
    day;
unmarshal(timestamp, Timestamp) when is_binary(Timestamp) ->
    parse_timestamp(Timestamp).

-spec parse_timestamp(binary()) -> machinery:timestamp().
parse_timestamp(Bin) ->
    try
        MicroSeconds = genlib_rfc3339:parse(Bin, microsecond),
        case genlib_rfc3339:is_utc(Bin) of
            false ->
                erlang:error({bad_timestamp, not_utc}, [Bin]);
            true ->
                USec = MicroSeconds rem 1000000,
                DateTime = calendar:system_time_to_universal_time(MicroSeconds, microsecond),
                {DateTime, USec}
        end
    catch
        error:Error:St ->
            erlang:raise(error, {bad_timestamp, Bin, Error}, St)
    end.

%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-spec test() -> _.

-spec marshal_unmarshal_created_test() -> _.

marshal_unmarshal_created_test() ->
    Created =
        {created, #{
            id => <<"id">>,
            type => {calendar, day},
            created_at => <<"2000-01-01T00:00:00Z">>,
            currency => <<"USD">>
        }},
    Event = {ev, lim_time:machinery_now(), Created},
    ?assertEqual(Event, unmarshal(timestamped_change, marshal(timestamped_change, Event))).

-spec marshal_unmarshal_time_range_created_test() -> _.
marshal_unmarshal_time_range_created_test() ->
    TimeRangeCreated =
        {time_range_created, #{
            account_id_from => 25,
            account_id_to => 175,
            upper => <<"2000-01-01T00:00:00Z">>,
            lower => <<"2000-01-01T00:00:00Z">>
        }},
    Event = {ev, lim_time:machinery_now(), TimeRangeCreated},
    ?assertEqual(Event, unmarshal(timestamped_change, marshal(timestamped_change, Event))).

-endif.
