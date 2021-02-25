%%%
%%% Persistent entity context
%%%

-module(lim_entity_context).

-type context() :: #{namespace() => md()}.

-type namespace() :: binary().
%% as stolen from `machinery_msgpack`
-type md() ::
    nil
    | boolean()
    | integer()
    | float()
    %% string
    | binary()
    %% binary
    | {binary, binary()}
    | [md()]
    | #{md() => md()}.

-export_type([context/0]).
-export_type([md/0]).

-export([new/0]).
-export([get/2]).

%%

-spec new() -> context().
new() ->
    #{}.

-spec get(namespace(), context()) ->
    {ok, md()}
    | error.
get(Ns, Ctx) ->
    maps:find(Ns, Ctx).
