-module(limiter).

%% Application callbacks

-behaviour(application).

-export([start/2]).
-export([stop/1]).

%% Supervisor callbacks

-behaviour(supervisor).

-export([init/1]).

%%

-spec start(normal, any()) -> {ok, pid()} | {error, any()}.
start(_StartType, _StartArgs) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec stop(any()) -> ok.
stop(_State) ->
    ok.

%%

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    ServiceOpts = genlib_app:env(?MODULE, services, #{}),
    Healthcheck = enable_health_logging(genlib_app:env(?MODULE, health_check, #{})),

    {Backends, MachineHandlers, ModernizerHandlers} = lists:unzip3([
        contruct_backend_childspec('lim/range_v1', lim_range_machine),
        contruct_backend_childspec('lim/config_v1', lim_config_machine)
    ]),
    ok = application:set_env(limiter, backends, maps:from_list(Backends)),

    RouteOptsEnv = genlib_app:env(?MODULE, route_opts, #{}),
    EventHandlers = genlib_app:env(?MODULE, woody_event_handlers, [woody_event_handler_default]),
    EventHandlerOpts = genlib_app:env(?MODULE, scoper_event_handler_options, #{}),
    RouteOpts = RouteOptsEnv#{event_handler => {scoper_woody_event_handler, EventHandlerOpts}},

    ChildSpec = woody_server:child_spec(
        ?MODULE,
        #{
            ip => get_ip_address(),
            port => get_port(),
            protocol_opts => get_protocol_opts(),
            transport_opts => get_transport_opts(),
            shutdown_timeout => get_shutdown_timeout(),
            event_handler => EventHandlers,
            handlers => get_handler_specs(ServiceOpts),
            additional_routes =>
                machinery_mg_backend:get_routes(MachineHandlers, RouteOpts) ++
                machinery_modernizer_mg_backend:get_routes(ModernizerHandlers, RouteOpts) ++
                [erl_health_handle:get_route(Healthcheck)] ++ get_prometheus_route()
        }
    ),
    {ok,
        {
            #{strategy => one_for_all, intensity => 6, period => 30},
            [ChildSpec]
        }}.

-spec get_ip_address() -> inet:ip_address().
get_ip_address() ->
    {ok, Address} = inet:parse_address(genlib_app:env(?MODULE, ip, "::")),
    Address.

-spec get_port() -> inet:port_number().
get_port() ->
    genlib_app:env(?MODULE, port, 8022).

-spec get_protocol_opts() -> woody_server_thrift_http_handler:protocol_opts().
get_protocol_opts() ->
    genlib_app:env(?MODULE, protocol_opts, #{}).

-spec get_transport_opts() -> woody_server_thrift_http_handler:transport_opts().
get_transport_opts() ->
    genlib_app:env(?MODULE, transport_opts, #{}).

-spec get_shutdown_timeout() -> timeout().
get_shutdown_timeout() ->
    genlib_app:env(?MODULE, shutdown_timeout, 0).

-spec get_handler_specs(map()) -> [woody:http_handler(woody:th_handler())].
get_handler_specs(ServiceOpts) ->
    LimiterService = maps:get(limiter, ServiceOpts, #{}),
    ConfiguratorService = maps:get(configurator, ServiceOpts, #{}),
    [
        {
            maps:get(path, LimiterService, <<"/v1/limiter">>),
            {{lim_limiter_thrift, 'Limiter'}, lim_handler}
        },
        {
            maps:get(path, ConfiguratorService, <<"/v1/configurator">>),
            {{lim_configurator_thrift, 'Configurator'}, lim_configurator}
        }
    ].

%%

-spec enable_health_logging(erl_health:check()) -> erl_health:check().
enable_health_logging(Check) ->
    EvHandler = {erl_health_event_handler, []},
    maps:map(
        fun(_, Runner) -> #{runner => Runner, event_handler => EvHandler} end,
        Check
    ).

-spec get_prometheus_route() -> [{iodata(), module(), _Opts :: any()}].
get_prometheus_route() ->
    [{"/metrics/[:registry]", prometheus_cowboy2_handler, []}].

contruct_backend_childspec(NS, Handler) ->
    Schema = get_namespace_schema(NS),
    {
        construct_machinery_backend_spec(NS, Schema),
        construct_machinery_handler_spec(NS, Handler, Schema),
        construct_machinery_modernizer_spec(NS, Schema)
    }.

construct_machinery_backend_spec(NS, Schema) ->
    {NS,
        {machinery_mg_backend, #{
            schema => Schema,
            client => get_service_client(automaton)
        }}}.

construct_machinery_handler_spec(NS, Handler, Schema) ->
    {Handler, #{
        path => lim_string:join(["/v1/stateproc/", NS]),
        backend_config => #{schema => Schema}
    }}.

construct_machinery_modernizer_spec(NS, Schema) ->
    #{
        path => lim_string:join(["/v1/modernizer/", NS]),
        backend_config => #{schema => Schema}
    }.

get_namespace_schema('lim/range_v1') ->
    lim_range_machinery_schema;
get_namespace_schema('lim/config_v1') ->
    lim_config_machinery_schema.

get_service_client(ServiceID) ->
    case lim_client_woody:get_service_client_url(ServiceID) of
        undefined ->
            error({unknown_service, ServiceID});
        Url ->
            lim_utils:get_woody_client(Url)
    end.
