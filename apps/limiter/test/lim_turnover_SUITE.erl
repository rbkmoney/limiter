-module(lim_turnover_SUITE).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("lim_ct_helper.hrl").

-include_lib("limiter_proto/include/lim_configurator_thrift.hrl").
-include_lib("xrates_proto/include/xrates_rate_thrift.hrl").

-export([all/0]).

-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_testcase/2]).
-export([end_per_testcase/2]).

-export([commit_with_default_exchange/1]).
-export([partial_commit_with_exchange/1]).
-export([commit_with_exchange/1]).
-export([get_rate/1]).
-export([get_limit_notfound/1]).
-export([hold_ok/1]).
-export([commit_ok/1]).
-export([rollback_ok/1]).
-export([refund_ok/1]).
-export([get_config_ok/1]).

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
            commit_with_default_exchange,
            partial_commit_with_exchange,
            commit_with_exchange,
            get_rate,
            get_limit_notfound,
            hold_ok,
            commit_ok,
            rollback_ok,
            get_config_ok,
            refund_ok
        ]}
    ].

-type config() :: [{atom(), any()}].

-spec init_per_suite(config()) -> config().
init_per_suite(Config) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({lim_handler, '_', '_'}, x),
    Apps =
        genlib_app:start_application_with(limiter, [
            {service_clients, #{
                accounter => #{
                    url => <<"http://shumway:8022/accounter">>
                },
                automaton => #{
                    url => <<"http://machinegun:8022/v1/automaton">>
                },
                xrates => #{
                    url => <<"http://xrates:8022/xrates">>
                }
            }},
            {exchange_factors, #{
                <<"DEFAULT">> => {1, 1},
                <<"USD">> => {105, 100},
                <<"EUR">> => {12, 10}
            }}
        ]),
    [{apps, Apps}] ++ Config.

-spec end_per_suite(config()) -> _.
end_per_suite(Config) ->
    [application:stop(App) || App <- proplists:get_value(apps, Config)],
    Config.

-spec init_per_testcase(test_case_name(), config()) -> config().
init_per_testcase(_Name, C) ->
    [{test_sup, lim_mock:start_mocked_service_sup()} | C].

-spec end_per_testcase(test_case_name(), config()) -> config().
end_per_testcase(_Name, C) ->
    lim_mock:stop_mocked_service_sup(?config(test_sup, C)),
    ok.

%%

-spec commit_with_default_exchange(config()) -> _.
commit_with_default_exchange(C) ->
    Rational = #base_Rational{p = 1000000, q = 100},
    mock_exchange(Rational, C),
    ID = lim_time:to_rfc3339(lim_time:now()),
    #{client := Client} = prepare_environment(ID, <<"GlobalMonthTurnover">>, C),
    Context = #limiter_context_LimitContext{
        payment_processing = #limiter_context_ContextPaymentProcessing{
            op = {invoice, #limiter_context_PaymentProcessingOperationInvoice{}},
            invoice = #limiter_context_Invoice{
                created_at = <<"2000-01-01T00:00:00Z">>,
                cost = #limiter_base_Cash{
                    amount = 10000,
                    currency = #limiter_base_CurrencyRef{symbolic_code = <<"SOME_CURRENCY">>}
                }
            }
        }
    },
    Timestamp = lim_time:to_rfc3339(lim_time:now()),
    LimitChangeID = <<Timestamp/binary, "Commit">>,
    Change = #limiter_LimitChange{
        id = ID,
        change_id = LimitChangeID
    },
    {ok, {vector, _}} = hold_and_commit(Change, Context, Client),
    {ok, #limiter_Limit{amount = 10000}} = lim_client:get(ID, Context, Client).

-spec partial_commit_with_exchange(config()) -> _.
partial_commit_with_exchange(C) ->
    Rational = #base_Rational{p = 800000, q = 100},
    mock_exchange(Rational, C),
    ID = lim_time:to_rfc3339(lim_time:now()),
    #{client := Client} = prepare_environment(ID, <<"GlobalMonthTurnover">>, C),
    Context = #limiter_context_LimitContext{
        payment_processing = #limiter_context_ContextPaymentProcessing{
            op = {invoice_payment, #limiter_context_PaymentProcessingOperationInvoicePayment{}},
            invoice = #limiter_context_Invoice{
                effective_payment = #limiter_context_InvoicePayment{
                    created_at = <<"2000-01-01T00:00:00Z">>,
                    cost = #limiter_base_Cash{
                        amount = 10000,
                        currency = #limiter_base_CurrencyRef{symbolic_code = <<"USD">>}
                    },
                    capture_cost = #limiter_base_Cash{
                        amount = 8000,
                        currency = #limiter_base_CurrencyRef{symbolic_code = <<"USD">>}
                    }
                }
            }
        }
    },
    Timestamp = lim_time:to_rfc3339(lim_time:now()),
    LimitChangeID = <<Timestamp/binary, "PartialCommit">>,
    Change = #limiter_LimitChange{
        id = ID,
        change_id = LimitChangeID
    },
    {ok, {vector, _}} = hold_and_commit(Change, Context, Client),
    {ok, #limiter_Limit{amount = 8400}} = lim_client:get(ID, Context, Client).

-spec commit_with_exchange(config()) -> _.
commit_with_exchange(C) ->
    Rational = #base_Rational{p = 1000000, q = 100},
    mock_exchange(Rational, C),
    ID = lim_time:to_rfc3339(lim_time:now()),
    #{client := Client} = prepare_environment(ID, <<"GlobalMonthTurnover">>, C),
    Context = #limiter_context_LimitContext{
        payment_processing = #limiter_context_ContextPaymentProcessing{
            op = {invoice, #limiter_context_PaymentProcessingOperationInvoice{}},
            invoice = #limiter_context_Invoice{
                created_at = <<"2000-01-01T00:00:00Z">>,
                cost = #limiter_base_Cash{
                    amount = 10000,
                    currency = #limiter_base_CurrencyRef{symbolic_code = <<"USD">>}
                }
            }
        }
    },
    Timestamp = lim_time:to_rfc3339(lim_time:now()),
    LimitChangeID = <<Timestamp/binary, "Commit">>,
    Change = #limiter_LimitChange{
        id = ID,
        change_id = LimitChangeID
    },
    {ok, {vector, _}} = hold_and_commit(Change, Context, Client),
    {ok, #limiter_Limit{amount = 10500}} = lim_client:get(ID, Context, Client).

-spec get_rate(config()) -> _.
get_rate(C) ->
    Rational = #base_Rational{p = 10, q = 10},
    mock_exchange(Rational, C),
    Request = #rate_ConversionRequest{
        source = <<"RUB">>,
        destination = <<"USD">>,
        amount = 100,
        datetime = <<"Timestamp">>
    },
    WoodyContext = woody_context:new(),
    {ok, Rational} = lim_client_woody:call(
        xrates,
        'GetConvertedAmount',
        {?RATE_SOURCE_ID, Request},
        WoodyContext
    ).

-spec get_limit_notfound(config()) -> _.
get_limit_notfound(C) ->
    ID = lim_time:to_rfc3339(lim_time:now()),
    #{client := Client} = prepare_environment(ID, <<"GlobalMonthTurnover">>, C),
    Context = #limiter_context_LimitContext{
        payment_processing = #limiter_context_ContextPaymentProcessing{
            op = {invoice, #limiter_context_PaymentProcessingOperationInvoice{}},
            invoice = #limiter_context_Invoice{created_at = <<"2000-01-01T00:00:00Z">>}
        }
    },
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
                }
            }
        }
    },
    Timestamp = lim_time:to_rfc3339(lim_time:now()),
    LimitChangeID = <<Timestamp/binary, "Hold">>,
    Change = #limiter_LimitChange{
        id = ID,
        change_id = LimitChangeID
    },
    {ok, {vector, #limiter_VectorClock{}}} = lim_client:hold(Change, Context, Client),
    {ok, #limiter_Limit{}} = lim_client:get(ID, Context, Client).

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
                }
            }
        }
    },
    Timestamp = lim_time:to_rfc3339(lim_time:now()),
    LimitChangeID = <<Timestamp/binary, "Commit">>,
    Change = #limiter_LimitChange{
        id = ID,
        change_id = LimitChangeID
    },
    {ok, {vector, _}} = hold_and_commit(Change, Context, Client),
    {ok, #limiter_Limit{}} = lim_client:get(ID, Context, Client).

-spec rollback_ok(config()) -> _.
rollback_ok(C) ->
    ID = <<"ID">>,
    #{client := Client} = prepare_environment(ID, <<"GlobalMonthTurnover">>, C),
    Context0 = ?ctx_invoice_payment(?cash(10), ?cash(10)),
    Context1 = ?ctx_invoice_payment(?cash(10), ?cash(0)),

    Timestamp = lim_time:to_rfc3339(lim_time:now()),
    LimitChangeID = <<Timestamp/binary, "Rollback">>,
    Change = #limiter_LimitChange{
        id = ID,
        change_id = LimitChangeID
    },
    {ok, {vector, _}} = lim_client:hold(Change, Context0, Client),
    {ok, {vector, _}} = lim_client:commit(Change, Context1, Client).

-spec refund_ok(config()) -> _.
refund_ok(C) ->
    ID = lim_time:to_rfc3339(lim_time:now()),
    OwnerID = <<"WWWcool Ltd">>,
    ShopID = <<"shop">>,
    #{client := Client} = _LimitConfig = prepare_environment(ID, <<"ShopDayTurnover">>, C),
    Context0 = ?ctx_invoice_payment(OwnerID, ShopID, ?cash(15), ?cash(15)),
    RefundContext1 = ?ctx_invoice_payment_refund(OwnerID, ShopID, ?cash(10), ?cash(10), ?cash(10)),
    Timestamp = lim_time:to_rfc3339(lim_time:now()),
    LimitChangeID = <<Timestamp/binary, "Payment">>,

    Change = #limiter_LimitChange{
        id = ID,
        change_id = LimitChangeID
    },
    {ok, {vector, _}} = hold_and_commit(Change, Context0, Client),

    Timestamp2 = lim_time:to_rfc3339(lim_time:now()),
    LimitChangeID2 = <<Timestamp2/binary, "Refund">>,
    Change2 = #limiter_LimitChange{
        id = ID,
        change_id = LimitChangeID2
    },

    {ok, {vector, _}} = hold_and_commit(Change2, RefundContext1, Client),
    {ok, #limiter_Limit{} = Limit2} = lim_client:get(ID, RefundContext1, Client),
    ?assertEqual(Limit2#limiter_Limit.amount, 5).

-spec get_config_ok(config()) -> _.
get_config_ok(C) ->
    ID = <<"ID">>,
    #{client := Client} = prepare_environment(ID, <<"GlobalMonthTurnover">>, C),
    {ok, #limiter_config_LimitConfig{}} = lim_client:get_config(ID, Client).

%%

hold_and_commit(Change, Context, Client) ->
    {ok, {vector, _}} = lim_client:hold(Change, Context, Client),
    {ok, {vector, _}} = lim_client:commit(Change, Context, Client).

mock_exchange(Rational, C) ->
    lim_mock:mock_services([{xrates, fun('GetConvertedAmount', _) -> {ok, Rational} end}], C).

prepare_environment(ID, LimitName, _C) ->
    Client = lim_client:new(),
    Params = #limiter_cfg_LimitCreateParams{
        id = ID,
        name = LimitName,
        description = <<"description">>,
        started_at = <<"2000-01-01T00:00:00Z">>,
        body_type = {cash, #limiter_config_LimitBodyTypeCash{currency = <<"RUB">>}},
        op_behaviour = #limiter_config_OperationLimitBehaviour{
            invoice_payment_refund = {subtraction, #limiter_config_Subtraction{}}
        }
    },
    {ok, LimitConfig} = lim_client:legacy_create_config(Params, Client),
    #{config => LimitConfig, client => Client}.
