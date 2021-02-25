-module(lim_context).

-export([create/1]).
-export([woody_context/1]).
-export([operation_timestamp/1]).
-export([set_operation_timestamp/2]).

-type woody_context() :: woody_context:ctx().
-type timestamp() :: binary().
-type t() :: #{
    woody_context := woody_context(),
    operation_timestamp => timestamp()
}.

-export_type([t/0]).

-spec create(woody_context()) -> {ok, t()}.
create(WoodyContext) ->
    {ok, #{woody_context => WoodyContext}}.

-spec woody_context(t()) -> {ok, woody_context()}.
woody_context(Context) ->
    {ok, maps:get(woody_context, Context)}.

-spec operation_timestamp(t()) -> {ok, timestamp()} | {error, notfound}.
operation_timestamp(Context) ->
    case maps:get(operation_timestamp, Context, undefined) of
        undefined ->
            {error, notfound};
        Timestamp ->
            {ok, Timestamp}
    end.

-spec set_operation_timestamp(timestamp(), t()) -> {ok, t()}.
set_operation_timestamp(Timestamp, Context) ->
    {ok, Context#{operation_timestamp => Timestamp}}.
