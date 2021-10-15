-module(lim_configurator_SUITE).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

-include_lib("limiter_proto/include/lim_configurator_thrift.hrl").

-export([all/0]).

-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_testcase/2]).
-export([end_per_testcase/2]).

-export([legacy_create_config/1]).
-export([create_config/1]).
-export([get_config/1]).

-type test_case_name() :: atom().

-define(RATE_SOURCE_ID, <<"dummy_source_id">>).

%% tests descriptions

-spec all() -> [test_case_name()].
all() ->
    [
        {group, default}
    ].

-spec groups() -> [{atom(), list(), [test_case_name()]}].
groups() ->
    [
        {default, [], [
            legacy_create_config,
            create_config,
            get_config
        ]}
    ].

-type config() :: [{atom(), any()}].

-spec init_per_suite(config()) -> config().
init_per_suite(Config) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({machinery, '_', '_'}, x),
    Apps =
        genlib_app:start_application_with(limiter, [
            {service_clients, #{
                accounter => #{
                    url => <<"http://shumway:8022/accounter">>
                },
                automaton => #{
                    url => <<"http://machinegun:8022/v1/automaton">>
                }
            }}
        ]),
    [{apps, Apps}] ++ Config.

-spec end_per_suite(config()) -> _.
end_per_suite(Config) ->
    [application:stop(App) || App <- proplists:get_value(apps, Config)],
    Config.

-spec init_per_testcase(test_case_name(), config()) -> config().
init_per_testcase(_Name, C) ->
    C.

-spec end_per_testcase(test_case_name(), config()) -> config().
end_per_testcase(_Name, _C) ->
    ok.

%%

-spec legacy_create_config(config()) -> _.
legacy_create_config(_C) ->
    Client = lim_client:new(),
    Params = #limiter_cfg_LimitCreateParams{
        id = <<"ID">>,
        name = <<"GlobalMonthTurnover">>,
        description = <<"description">>,
        started_at = <<"2000-01-01T00:00:00Z">>,
        body_type = {cash, #limiter_config_LimitBodyTypeCash{currency = <<"RUB">>}}
    },
    {ok, #limiter_config_LimitConfig{}} = lim_client:legacy_create_config(Params, Client).

-spec create_config(config()) -> _.
create_config(_C) ->
    Client = lim_client:new(),
    Params = #limiter_config_LimitConfigParams{
        id = <<"ID">>,
        description = <<"description">>,
        started_at = <<"2000-01-01T00:00:00Z">>,
        body_type = {cash, #limiter_config_LimitBodyTypeCash{currency = <<"RUB">>}},
        shard_size = 4,
        time_range_type = {calendar, {week, #time_range_TimeRangeTypeCalendarWeek{}}},
        type = {turnover, #limiter_config_LimitTypeTurnover{}},
        scope = {scope, {shop, #limiter_config_LimitScopeTypeShop{}}},
        op_behaviour = #limiter_config_OperationLimitBehaviour{
            invoice_payment_refund = {addition, #limiter_config_Addition{}}
        },
        context_type = {payment_processing, #limiter_config_LimitContextTypePaymentProcessing{}}
    },
    {ok, #limiter_config_LimitConfig{}} = lim_client:create_config(Params, Client).

-spec get_config(config()) -> _.
get_config(C) ->
    ID = <<"ID">>,
    #{client := Client} = prepare_environment(ID, <<"GlobalMonthTurnover">>, C),
    {ok, #limiter_config_LimitConfig{id = ID}} = lim_client:get_config(ID, Client).

%%

prepare_environment(ID, LimitName, _C) ->
    Client = lim_client:new(),
    Params = #limiter_cfg_LimitCreateParams{
        id = ID,
        name = LimitName,
        description = <<"description">>,
        started_at = <<"2000-01-01T00:00:00Z">>,
        body_type = {cash, #limiter_config_LimitBodyTypeCash{currency = <<"RUB">>}}
    },
    {ok, LimitConfig} = lim_client:legacy_create_config(Params, Client),
    #{config => LimitConfig, client => Client}.
