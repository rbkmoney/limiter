-module(lim_range_machinery_schema).

%% Storage schema behaviour
-behaviour(machinery_mg_schema).

-export([get_version/1]).
-export([marshal/3]).
-export([unmarshal/3]).

%% Constants

-define(CURRENT_EVENT_FORMAT_VERSION, 1).

%% Internal types

-type type() :: machinery_mg_schema:t().
-type value(T) :: machinery_mg_schema:v(T).
-type value_type() :: machinery_mg_schema:vt().
-type context() :: machinery_mg_schema:context().

-type event() :: lim_range_machine:timestamped_event(lim_range_machine:event()).
-type aux_state() :: term().
-type call_args() :: term().
-type call_response() :: term().

-type data() ::
    aux_state()
    | event()
    | call_args()
    | call_response().

%% machinery_mg_schema callbacks

-spec get_version(value_type()) -> machinery_mg_schema:version().
get_version(event) ->
    ?CURRENT_EVENT_FORMAT_VERSION;
get_version(aux_state) ->
    undefined.

-spec marshal(type(), value(data()), context()) -> {machinery_msgpack:t(), context()}.
marshal({event, FormatVersion}, TimestampedChange, Context) ->
    marshal_event(FormatVersion, TimestampedChange, Context);
marshal(T, V, C) when
    T =:= {args, init} orelse
        T =:= {args, call} orelse
        T =:= {args, repair} orelse
        T =:= {aux_state, undefined} orelse
        T =:= {response, call} orelse
        T =:= {response, {repair, success}} orelse
        T =:= {response, {repair, failure}}
->
    machinery_mg_schema_generic:marshal(T, V, C).

-spec unmarshal(type(), machinery_msgpack:t(), context()) -> {data(), context()}.
unmarshal({event, FormatVersion}, EncodedChange, Context) ->
    unmarshal_event(FormatVersion, EncodedChange, Context);
unmarshal(T, V, C) when
    T =:= {args, init} orelse
        T =:= {args, call} orelse
        T =:= {args, repair} orelse
        T =:= {aux_state, undefined} orelse
        T =:= {response, call} orelse
        T =:= {response, {repair, success}} orelse
        T =:= {response, {repair, failure}}
->
    machinery_mg_schema_generic:unmarshal(T, V, C).

%% Internals

-spec marshal_event(machinery_mg_schema:version(), event(), context()) -> {machinery_msgpack:t(), context()}.
marshal_event(1, TimestampedChange, Context) ->
    ThriftChange = lim_range_codec:marshal(timestamped_change, TimestampedChange),
    Type = {struct, struct, {lim_limiter_range_thrift, 'TimestampedChange'}},
    {{bin, lim_proto_utils:serialize(Type, ThriftChange)}, Context}.

-spec unmarshal_event(machinery_mg_schema:version(), machinery_msgpack:t(), context()) -> {event(), context()}.
unmarshal_event(1, EncodedChange, Context) ->
    {bin, EncodedThriftChange} = EncodedChange,
    Type = {struct, struct, {lim_limiter_range_thrift, 'TimestampedChange'}},
    ThriftChange = lim_proto_utils:deserialize(Type, EncodedThriftChange),
    {lim_range_codec:unmarshal(timestamped_change, ThriftChange), Context}.

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
    {Marshaled, _} = marshal_event(1, Event, {}),
    {Unmarshaled, _} = unmarshal_event(1, Marshaled, {}),
    ?assertEqual(Event, Unmarshaled).

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
    {Marshaled, _} = marshal_event(1, Event, {}),
    {Unmarshaled, _} = unmarshal_event(1, Marshaled, {}),
    ?assertEqual(Event, Unmarshaled).

-endif.
