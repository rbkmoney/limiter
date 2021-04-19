-module(lim_rates).

-include_lib("xrates_proto/include/xrates_rate_thrift.hrl").
-include_lib("limiter_proto/include/lim_base_thrift.hrl").
-include_lib("limiter_proto/include/lim_limiter_thrift.hrl").

-export([get_converted_amount/3]).

-type amount() :: dmsl_domain_thrift:'Amount'().
-type currency() :: dmsl_domain_thrift:'CurrencySymbolicCode'().
-type limit_context() :: lim_context:t().
-type config() :: lim_config_machine:config().

-type convertation_error() :: quote_not_found | currency_not_found.

-export_type([convertation_error/0]).

-define(APP, limiter).
-define(DEFAULT_FACTOR, 1.1).
-define(DEFAULT_FACTOR_NAME, <<"DEFAULT">>).

-spec get_converted_amount({amount(), currency()}, config(), limit_context()) ->
    {ok, amount()}
    | {error, convertation_error()}.
get_converted_amount(Cash = {_Amount, Currency}, Config, LimitContext) ->
    Factor = get_exchange_factor(Currency),
    case
        call_rates(
            'GetConvertedAmount',
            {<<"CBR">>, construct_conversion_request(Cash, Config, LimitContext)},
            LimitContext
        )
    of
        {ok, #base_Rational{p = P, q = Q}} ->
            Rational = genlib_rational:new(P, Q),
            {ok, genlib_rational:round(genlib_rational:mul(Rational, Factor))};
        {exception, #rate_QuoteNotFound{}} ->
            {error, quote_not_found};
        {exception, #rate_CurrencyNotFound{}} ->
            {error, currency_not_found}
    end.

construct_conversion_request({Amount, Currency}, Config = #{body_type := {cash, DestinationCurrency}}, LimitContext) ->
    ContextType = lim_config_machine:context_type(Config),
    {ok, Timestamp} = lim_context:get_from_context(ContextType, created_at, LimitContext),
    #rate_ConversionRequest{
        source = Currency,
        destination = DestinationCurrency,
        amount = Amount,
        datetime = Timestamp
    }.

get_exchange_factor(Currency) ->
    Factors = genlib_app:env(?APP, exchange_factors, #{}),
    case maps:get(Currency, Factors, undefined) of
        undefined ->
            case maps:get(?DEFAULT_FACTOR_NAME, Factors, undefined) of
                undefined ->
                    ?DEFAULT_FACTOR;
                DefaultFactor ->
                    DefaultFactor
            end;
        Factor ->
            Factor
    end.

%%

call_rates(Function, Args, LimitContext) ->
    {ok, WoodyContext} = lim_context:woody_context(LimitContext),
    lim_client_woody:call(xrates, Function, Args, WoodyContext).
