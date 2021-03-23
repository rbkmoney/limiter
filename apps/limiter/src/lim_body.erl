-module(lim_body).

-include_lib("limiter_proto/include/lim_base_thrift.hrl").
-include_lib("limiter_proto/include/lim_limiter_thrift.hrl").

-export([extract_and_validate_body/2]).

-type t() :: {amount, integer()} | {cash, {integer(), currency()}}.

-type thrift_body() :: lim_limiter_thrift:'LimitBody'().
-type currency() :: lim_base_thrift:'CurrencySymbolicCode'().
-type config() :: lim_config_machine:config().
-type body_type() :: lim_config_machine:body_type().

-type validate_error() :: {invalid_body_type, Current :: body_type(), Config :: body_type()}.

-export_type([validate_error/0]).
-export_type([t/0]).

-spec extract_and_validate_body(thrift_body(), config()) -> {ok, t()} | {error, validate_error()}.
extract_and_validate_body(
    {cash, #limiter_base_Cash{
        amount = Amount,
        currency = #limiter_base_CurrencyRef{symbolic_code = Currency}
    }},
    #{body_type := cash}
) ->
    {ok, {cash, {Amount, Currency}}};
extract_and_validate_body({amount, Amount}, #{body_type := amount}) ->
    {ok, {amount, Amount}};
extract_and_validate_body({Current, _}, #{body_type := Config}) ->
    {error, {invalid_body_type, Current, Config}}.
