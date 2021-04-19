-module(lim_body).

-include_lib("limiter_proto/include/lim_base_thrift.hrl").
-include_lib("limiter_proto/include/lim_limiter_thrift.hrl").

-export([get_body/3]).
-export([create_body_from_cash/2]).

-type t() :: {amount, amount()} | {cash, cash()}.

-type amount() :: integer().
-type cash() :: #{
    amount := amount(),
    currency := currency()
}.

-type currency() :: lim_base_thrift:'CurrencySymbolicCode'().
-type config() :: lim_config_machine:config().
-type body_type() :: full | partial.
-type get_body_error() :: notfound | lim_rates:convertation_error().

-export_type([t/0]).
-export_type([cash/0]).
-export_type([get_body_error/0]).

-spec get_body(body_type(), config(), lim_context:t()) -> {ok, t()} | {error, get_body_error()}.
get_body(BodyType, Config = #{body_type := {cash, ConfigCurrency}}, LimitContext) ->
    ContextType = lim_config_machine:context_type(Config),
    {ok, Operation} = lim_context:get_operation(ContextType, LimitContext),
    case get_body_for_operation(BodyType, Operation, Config, LimitContext) of
        {ok, {cash, #{currency := ConfigCurrency}}} = Result ->
            Result;
        {ok, {cash, #{amount := Amount, currency := Currency}}} ->
            case lim_rates:get_converted_amount({Amount, Currency}, Config, LimitContext) of
                {ok, ConvertedAmount} ->
                    {ok, create_body_from_cash(ConvertedAmount, ConfigCurrency)};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

-spec get_body_for_operation(body_type(), lim_context:context_operation(), config(), lim_context:t()) ->
    {ok, t()} | {error, notfound}.
get_body_for_operation(full, invoice = Operation, Config, LimitContext) ->
    ContextType = lim_config_machine:context_type(Config),
    lim_context:get_from_context(ContextType, cost, Operation, LimitContext);
get_body_for_operation(full, invoice_adjustment, Config, LimitContext) ->
    ContextType = lim_config_machine:context_type(Config),
    lim_context:get_from_context(ContextType, cost, invoice, LimitContext);
get_body_for_operation(full, invoice_payment = Operation, Config, LimitContext) ->
    ContextType = lim_config_machine:context_type(Config),
    lim_context:get_from_context(ContextType, cost, Operation, LimitContext);
get_body_for_operation(full, invoice_payment_adjustment, Config, LimitContext) ->
    ContextType = lim_config_machine:context_type(Config),
    lim_context:get_from_context(ContextType, cost, invoice_payment, LimitContext);
get_body_for_operation(full, invoice_payment_refund, Config, LimitContext) ->
    ContextType = lim_config_machine:context_type(Config),
    lim_context:get_from_context(ContextType, cost, invoice_payment, LimitContext);
get_body_for_operation(full, invoice_payment_chargeback = Operation, Config, LimitContext) ->
    ContextType = lim_config_machine:context_type(Config),
    lim_context:get_from_context(ContextType, body, Operation, LimitContext);
get_body_for_operation(partial, invoice, _Config, _LimitContext) ->
    {error, notfound};
get_body_for_operation(partial, invoice_adjustment, _Config, _LimitContext) ->
    {error, notfound};
get_body_for_operation(partial, invoice_payment = Operation, Config, LimitContext) ->
    ContextType = lim_config_machine:context_type(Config),
    lim_context:get_from_context(ContextType, capture_cost, Operation, LimitContext);
get_body_for_operation(partial, invoice_payment_adjustment, _Config, _LimitContext) ->
    {error, notfound};
get_body_for_operation(partial, invoice_payment_refund = Operation, Config, LimitContext) ->
    ContextType = lim_config_machine:context_type(Config),
    lim_context:get_from_context(ContextType, cost, Operation, LimitContext);
get_body_for_operation(partial, invoice_payment_chargeback, _Config, _LimitContext) ->
    {error, notfound}.

-spec create_body_from_cash(amount(), currency()) -> t().
create_body_from_cash(Amount, Currency) ->
    {cash, #{amount => Amount, currency => Currency}}.
