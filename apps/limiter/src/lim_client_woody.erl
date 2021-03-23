-module(lim_client_woody).

-export([call/4]).
-export([call/5]).
-export([get_service_client_url/1]).

-define(APP, limiter).
-define(DEFAULT_DEADLINE, 5000).

%%
-type service_name() :: atom().

-spec call(service_name(), woody:func(), woody:args(), woody_context:ctx()) -> woody:result().
call(ServiceName, Function, Args, Context) ->
    EventHandler = scoper_woody_event_handler,
    call(ServiceName, Function, Args, Context, EventHandler).

-spec call(service_name(), woody:func(), woody:args(), woody_context:ctx(), woody:ev_handler()) -> woody:result().
call(ServiceName, Function, Args, Context0, EventHandler) ->
    Deadline = get_service_deadline(ServiceName),
    Context1 = set_deadline(Deadline, Context0),
    Url = get_service_client_url(ServiceName),
    Service = get_service_modname(ServiceName),
    Request = {Service, Function, Args},
    woody_client:call(
        Request,
        #{url => Url, event_handler => EventHandler},
        Context1
    ).

get_service_client_config(ServiceName) ->
    ServiceClients = genlib_app:env(?APP, service_clients, #{}),
    maps:get(ServiceName, ServiceClients, #{}).

-spec get_service_client_url(atom()) -> lim_maybe:maybe(woody:url()).
get_service_client_url(ServiceName) ->
    maps:get(url, get_service_client_config(ServiceName), undefined).

-spec get_service_modname(service_name()) -> woody:service().
get_service_modname(xrates) ->
    {xrates_rate_thrift, 'Rates'};
get_service_modname(accounter) ->
    {dmsl_accounter_thrift, 'Accounter'}.

-spec get_service_deadline(service_name()) -> undefined | woody_deadline:deadline().
get_service_deadline(ServiceName) ->
    ServiceClient = get_service_client_config(ServiceName),
    Timeout = maps:get(deadline, ServiceClient, ?DEFAULT_DEADLINE),
    woody_deadline:from_timeout(Timeout).

set_deadline(Deadline, Context) ->
    case woody_context:get_deadline(Context) of
        undefined ->
            woody_context:set_deadline(Deadline, Context);
        _AlreadySet ->
            Context
    end.
