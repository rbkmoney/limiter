-module(lim_mock).

-include_lib("common_test/include/ct.hrl").

-export([start_mocked_service_sup/0]).
-export([stop_mocked_service_sup/1]).
-export([mock_services/2]).

-define(APP, limiter).

-spec start_mocked_service_sup() -> _.
start_mocked_service_sup() ->
    {ok, SupPid} = genlib_adhoc_supervisor:start_link(#{}, []),
    _ = unlink(SupPid),
    SupPid.

-spec stop_mocked_service_sup(pid()) -> _.
stop_mocked_service_sup(SupPid) ->
    exit(SupPid, shutdown).

-define(HOST_IP, "::").
-define(HOST_PORT, 8080).
-define(HOST_NAME, "localhost").
-define(HOST_URL, ?HOST_NAME ++ ":" ++ integer_to_list(?HOST_PORT)).

-spec mock_services(_, _) -> _.
mock_services(Services, SupOrConfig) ->
    maps:map(fun set_cfg/2, mock_services_(Services, SupOrConfig)).

set_cfg(Service, Url) ->
    {ok, Clients} = application:get_env(?APP, service_clients),
    #{Service := Cfg} = Clients,
    ok = application:set_env(
        ?APP,
        service_clients,
        Clients#{Service => Cfg#{url => Url}}
    ).

mock_services_(Services, Config) when is_list(Config) ->
    mock_services_(Services, ?config(test_sup, Config));
mock_services_(Services, SupPid) when is_pid(SupPid) ->
    Name = lists:map(fun get_service_name/1, Services),

    {ok, IP} = inet:parse_address(?HOST_IP),
    ServerID = {dummy, Name},
    Options = #{
        ip => IP,
        port => 0,
        event_handler => scoper_woody_event_handler,
        handlers => lists:map(fun mock_service_handler/1, Services),
        transport_opts => #{num_acceptors => 1}
    },
    ChildSpec = woody_server:child_spec(ServerID, Options),
    {ok, _} = supervisor:start_child(SupPid, ChildSpec),
    {IP, Port} = woody_server:get_addr(ServerID, Options),
    lists:foldl(
        fun(Service, Acc) ->
            ServiceName = get_service_name(Service),
            Acc#{ServiceName => make_url(ServiceName, Port)}
        end,
        #{},
        Services
    ).

get_service_name({ServiceName, _Fun}) ->
    ServiceName;
get_service_name({ServiceName, _WoodyService, _Fun}) ->
    ServiceName.

mock_service_handler({ServiceName, Fun}) ->
    mock_service_handler(ServiceName, get_service_modname(ServiceName), Fun);
mock_service_handler({ServiceName, WoodyService, Fun}) ->
    mock_service_handler(ServiceName, WoodyService, Fun).

mock_service_handler(ServiceName, WoodyService, Fun) ->
    {make_path(ServiceName), {WoodyService, {lim_mock_service, #{function => Fun}}}}.

get_service_modname(xrates) ->
    {xrates_rate_thrift, 'Rates'}.

make_url(ServiceName, Port) ->
    iolist_to_binary(["http://", ?HOST_NAME, ":", integer_to_list(Port), make_path(ServiceName)]).

make_path(ServiceName) ->
    "/" ++ atom_to_list(ServiceName).
