-module(lim_range_codec).

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
marshal(timestamped_change, {ev, _Timestamp, _Change}) ->
    % #src_TimestampedChange{
    %     change = marshal(change, Change),
    %     occured_at = marshal(timestamp, Timestamp)
    % };
    {};
marshal(change, {created, _Range}) ->
    {created, marshal(range, _Range)};
marshal(range, _Range = #{}) ->
    {};
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
unmarshal(timestamped_change, _TimestampedChange) ->
%     Timestamp = unmarshal(timestamp, TimestampedChange#src_TimestampedChange.occured_at),
%     Change = unmarshal(change, TimestampedChange#src_TimestampedChange.change),
%     {ev, Timestamp, Change};
    {};
unmarshal(change, {created, _Range}) ->
    {created, unmarshal(range, _Range)};
unmarshal(range, {}) ->
    genlib_map:compact(#{});
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
