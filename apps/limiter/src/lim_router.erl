-module(lim_router).

-export([get_handler/1]).

-type processor_type() :: binary().
-type processor() :: module().

-export_type([processor_type/0]).
-export_type([processor/0]).

-spec get_handler(processor_type()) ->
    {ok, processor()}
    | {error, notfound}.
get_handler(<<"TurnoverProcessor">>) ->
    {ok, lim_turnover_processor};
get_handler(_) ->
    {error, notfound}.
