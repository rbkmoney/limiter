-module(lim_p_transfer).

-include_lib("damsel/include/dmsl_accounter_thrift.hrl").
-include_lib("damsel/include/dmsl_base_thrift.hrl").

-export([construct_postings/3]).
-export([reverse_postings/1]).
-export([assert_partial_posting_amount/2]).

-type amount() :: integer().
-type currency() :: binary().
-type account_id() :: lim_accounting:account_id().
-type posting() :: lim_accounting:posting().
-type body() :: lim_body:t().

-type forbidden_operation_amount_error() :: #{
    type := positive | negative,
    partial := amount(),
    full := amount(),
    currency := currency()
}.

-export_type([forbidden_operation_amount_error/0]).

-spec construct_postings(account_id(), account_id(), body()) -> [posting()].
construct_postings(AccountFrom, AccountTo, {cash, #{amount := Amount, currency := Currency}}) ->
    [
        #accounter_Posting{
            from_id = AccountFrom,
            to_id = AccountTo,
            amount = Amount,
            currency_sym_code = Currency,
            description = <<>>
        }
    ];
construct_postings(AccountFrom, AccountTo, {amount, Amount}) ->
    [
        #accounter_Posting{
            from_id = AccountFrom,
            to_id = AccountTo,
            amount = Amount,
            currency_sym_code = lim_accounting:get_default_currency(),
            description = <<>>
        }
    ].

-spec reverse_postings([posting()]) -> [posting()].
reverse_postings(Postings) ->
    [
        Posting#accounter_Posting{
            from_id = AccountTo,
            to_id = AccountFrom
        }
        || Posting = #accounter_Posting{from_id = AccountFrom, to_id = AccountTo} <- Postings
    ].

-spec assert_partial_posting_amount([posting()], [posting()]) -> ok | {error, forbidden_operation_amount_error()}.
assert_partial_posting_amount(
    [#accounter_Posting{amount = Partial, currency_sym_code = Currency} | _Rest],
    [#accounter_Posting{amount = Full, currency_sym_code = Currency} | _Rest]
) ->
    compare_amount(Partial, Full, Currency);
assert_partial_posting_amount(
    [#accounter_Posting{amount = Partial, currency_sym_code = PartialCurrency} | _Rest],
    [#accounter_Posting{amount = Full, currency_sym_code = FullCurrency} | _Rest]
) ->
    erlang:error({invalid_partial_cash, {Partial, PartialCurrency}, {Full, FullCurrency}}).

compare_amount(Partial, Full, Currency) when Full > 0 ->
    case Partial =< Full of
        true ->
            ok;
        false ->
            {error,
                {forbidden_operation_amount, #{
                    type => positive,
                    partial => Partial,
                    full => Full,
                    currency => Currency
                }}}
    end;
compare_amount(Partial, Full, Currency) when Full < 0 ->
    case Partial >= Full of
        true ->
            ok;
        false ->
            {error,
                {forbidden_operation_amount, #{
                    type => negative,
                    partial => Partial,
                    full => Full,
                    currency => Currency
                }}}
    end.
