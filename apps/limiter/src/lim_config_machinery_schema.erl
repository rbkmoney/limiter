-module(lim_config_machinery_schema).

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

-type event() :: {}.
-type aux_state() :: lim_config_machine:state().
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
    undefined;
get_version(aux_state) ->
    ?CURRENT_EVENT_FORMAT_VERSION.

-spec marshal(type(), value(data()), context()) -> {machinery_msgpack:t(), context()}.
marshal({aux_state, FormatVersion}, State, Context) ->
    marshal_aux_state(FormatVersion, State, Context);
marshal(T, V, C) when
    T =:= {args, init} orelse
        T =:= {args, call} orelse
        T =:= {args, repair} orelse
        T =:= {event, undefined} orelse
        T =:= {response, call} orelse
        T =:= {response, {repair, success}} orelse
        T =:= {response, {repair, failure}}
->
    machinery_mg_schema_generic:marshal(T, V, C).

-spec unmarshal(type(), machinery_msgpack:t(), context()) -> {data(), context()}.
unmarshal({aux_state, FormatVersion}, EncodedState, Context) ->
    unmarshal_aux_state(FormatVersion, EncodedState, Context);
unmarshal(T, V, C) when
    T =:= {args, init} orelse
        T =:= {args, call} orelse
        T =:= {args, repair} orelse
        T =:= {event, undefined} orelse
        T =:= {response, call} orelse
        T =:= {response, {repair, success}} orelse
        T =:= {response, {repair, failure}}
->
    machinery_mg_schema_generic:unmarshal(T, V, C).

%% Internals

-spec marshal_aux_state(machinery_mg_schema:version(), aux_state(), context()) -> {machinery_msgpack:t(), context()}.
marshal_aux_state(1, AuxState, Context) ->
    machinery_mg_schema_generic:marshal({aux_state, 1}, AuxState, Context).

-spec unmarshal_aux_state(machinery_mg_schema:version(), machinery_msgpack:t(), context()) -> {aux_state(), context()}.
unmarshal_aux_state(1, EncodedAuxState, Context) ->
    machinery_mg_schema_generic:unmarshal({aux_state, 1}, EncodedAuxState, Context).
