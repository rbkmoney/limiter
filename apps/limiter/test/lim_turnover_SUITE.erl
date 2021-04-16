-module(lim_turnover_SUITE).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

-include_lib("limiter_proto/include/lim_configurator_thrift.hrl").

-export([all/0]).

-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_testcase/2]).
-export([end_per_testcase/2]).

-export([get_limit_notfound/1]).
-export([hold_ok/1]).
-export([commit_ok/1]).
-export([rollback_ok/1]).

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
            get_limit_notfound,
            hold_ok,
            commit_ok,
            rollback_ok
        ]}
    ].

-type config() :: [{atom(), any()}].

-spec init_per_suite(config()) -> config().
init_per_suite(Config) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({lim_range_machine, '_', '_'}, x),
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

-spec get_limit_notfound(config()) -> _.
get_limit_notfound(C) ->
    ID = <<"ID">>,
    #{client := Client} = prepare_environment(ID, <<"GlobalMonthTurnover">>, C),
    Context = #limiter_context_LimitContext{
        payment_processing = #limiter_context_ContextPaymentProcessing{
            op = {invoice, #limiter_context_PaymentProcessingOperationInvoice{}},
            invoice = #limiter_context_Invoice{created_at = <<"2000-01-01T00:00:00Z">>}
    }},
    {exception, #limiter_LimitNotFound{}} = lim_client:get(ID, Context, Client).

-spec hold_ok(config()) -> _.
hold_ok(C) ->
    ID = <<"ID">>,
    #{client := Client} = prepare_environment(ID, <<"GlobalMonthTurnover">>, C),
    Context = #limiter_context_LimitContext{
        payment_processing = #limiter_context_ContextPaymentProcessing{
            op = {invoice, #limiter_context_PaymentProcessingOperationInvoice{}},
            invoice = #limiter_context_Invoice{
                created_at = <<"2000-01-01T00:00:00Z">>,
                cost = #limiter_base_Cash{
                    amount = 10,
                    currency = #limiter_base_CurrencyRef{symbolic_code = <<"RUB">>}
                }}
    }},
    Timestamp = lim_time:to_rfc3339(lim_time:now()),
    LimitChangeID = <<Timestamp/binary, "Hold">>,
    Change = #limiter_LimitChange{
        id = ID,
        change_id = LimitChangeID
    },
    {ok, {vector, _}} = lim_client:hold(Change, Context, Client).

-spec commit_ok(config()) -> _.
commit_ok(C) ->
    ID = <<"ID">>,
    #{client := Client} = prepare_environment(ID, <<"GlobalMonthTurnover">>, C),
    Context = #limiter_context_LimitContext{
        payment_processing = #limiter_context_ContextPaymentProcessing{
            op = {invoice, #limiter_context_PaymentProcessingOperationInvoice{}},
            invoice = #limiter_context_Invoice{
                created_at = <<"2000-01-01T00:00:00Z">>,
                cost = #limiter_base_Cash{
                    amount = 10,
                    currency = #limiter_base_CurrencyRef{symbolic_code = <<"RUB">>}
                }}
    }},
    Timestamp = lim_time:to_rfc3339(lim_time:now()),
    LimitChangeID = <<Timestamp/binary, "Commit">>,
    Change = #limiter_LimitChange{
        id = ID,
        change_id = LimitChangeID
    },
    {ok, {vector, _}} = lim_client:hold(Change, Context, Client),
    {ok, {vector, _}} = lim_client:commit(Change, Context, Client).

-spec rollback_ok(config()) -> _.
rollback_ok(C) ->
    ID = <<"ID">>,
    #{client := Client} = prepare_environment(ID, <<"GlobalMonthTurnover">>, C),
    Context = #limiter_context_LimitContext{
        payment_processing = #limiter_context_ContextPaymentProcessing{
            op = {invoice_payment, #limiter_context_PaymentProcessingOperationInvoicePayment{}},
            invoice = #limiter_context_Invoice{
                effective_payment = #limiter_context_InvoicePayment{
                    created_at = <<"2000-01-01T00:00:00Z">>,
                    cost = #limiter_base_Cash{
                        amount = 10,
                        currency = #limiter_base_CurrencyRef{symbolic_code = <<"RUB">>}
                    },
                    capture_cost = #limiter_base_Cash{
                        amount = 0,
                        currency = #limiter_base_CurrencyRef{symbolic_code = <<"RUB">>}
                    }
                }
            }
        }
    },
    Timestamp = lim_time:to_rfc3339(lim_time:now()),
    LimitChangeID = <<Timestamp/binary, "Rollback">>,
    Change = #limiter_LimitChange{
        id = ID,
        change_id = LimitChangeID
    },
    {ok, {vector, _}} = lim_client:hold(Change, Context, Client),
    {ok, {vector, _}} = lim_client:commit(Change, Context, Client).

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
    {ok, LimitConfig} = lim_client:create_config(Params, Client),
    #{config => LimitConfig, client => Client}.
