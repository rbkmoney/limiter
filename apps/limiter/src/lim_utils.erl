-module(lim_utils).

-export([get_backend/2]).
-export([get_woody_client/1]).

-type woody_context() :: woody_context:ctx().

-spec get_backend(atom(), woody_context()) -> machinery_mg_backend:backend().
get_backend(NS, WoodyCtx) ->
    Backend = maps:get(NS, genlib_app:env(limiter, backends, #{})),
    {Mod, Opts} = machinery_utils:get_backend(Backend),
    {Mod, Opts#{
        woody_ctx => WoodyCtx
    }}.

%%% Internal functions

-spec get_woody_client(woody:url()) -> machinery_mg_client:woody_client().
get_woody_client(Url) ->
    genlib_map:compact(#{
        url => Url,
        event_handler => get_woody_event_handlers()
    }).

-spec get_woody_event_handlers() -> woody:ev_handlers().
get_woody_event_handlers() ->
    genlib_app:env(limiter, woody_event_handlers, [
        scoper_woody_event_handler,
        hay_woody_event_handler
    ]).
