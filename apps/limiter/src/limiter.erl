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
    EventHandlers = genlib_app:env(?MODULE, woody_event_handlers, [woody_event_handler_default]),
    Healthcheck = enable_health_logging(genlib_app:env(?MODULE, health_check, #{})),
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
            additional_routes => [erl_health_handle:get_route(Healthcheck)] ++ get_prometheus_route()
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
    [
        {
            maps:get(path, LimiterService, <<"/v1/limiter">>),
            {{dmsl_proto_limiter_thrift, 'Limiter'}, lim_handler}
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
