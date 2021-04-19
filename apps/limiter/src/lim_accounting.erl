-module(lim_accounting).

-include_lib("damsel/include/dmsl_accounter_thrift.hrl").
-include_lib("damsel/include/dmsl_base_thrift.hrl").

-export([plan/3]).
-export([hold/3]).
-export([commit/3]).
-export([rollback/3]).
-export([get_plan/2]).
-export([get_balance/2]).
-export([get_default_currency/0]).
-export([create_account/2]).

-type currency() :: dmsl_domain_thrift:'CurrencySymbolicCode'().
-type amount() :: dmsl_domain_thrift:'Amount'().
-type plan_id() :: dmsl_accounter_thrift:'PlanID'().
-type batch_id() :: dmsl_accounter_thrift:'BatchID'().
-type posting() :: dmsl_accounter_thrift:'Posting'().
-type batch() :: {batch_id(), [posting()]}.
-type account_id() :: dmsl_accounter_thrift:'AccountID'().
-type lim_context() :: lim_context:t().

-type balance() :: #{
    account_id := account_id(),
    own_amount := amount(),
    min_available_amount := amount(),
    max_available_amount := amount(),
    currency := currency()
}.

-type invalid_request_error() :: {invalid_request, list(binary())}.

-export_type([account_id/0]).
-export_type([amount/0]).
-export_type([balance/0]).
-export_type([plan_id/0]).
-export_type([batch/0]).
-export_type([posting/0]).
-export_type([batch_id/0]).
-export_type([invalid_request_error/0]).

-define(DEFAULT_CURRENCY, <<"RUB">>).

-spec plan(plan_id(), [batch()], lim_context()) -> ok | {error, invalid_request_error()}.
plan(_PlanID, [], _LimitContext) ->
    error(badarg);
plan(_PlanID, Batches, _LimitContext) when not is_list(Batches) ->
    error(badarg);
plan(PlanID, Batches, LimitContext) ->
    lists:foldl(
        fun(Batch, _) -> hold(PlanID, Batch, LimitContext) end,
        undefined,
        Batches
    ).

-spec hold(plan_id(), batch(), lim_context()) -> ok | {error, invalid_request_error()}.
hold(PlanID, Batch, LimitContext) ->
    do('Hold', construct_plan_change(PlanID, Batch), LimitContext).

-spec commit(plan_id(), [batch()], lim_context()) -> ok | {error, invalid_request_error()}.
commit(PlanID, Batches, LimitContext) ->
    do('CommitPlan', construct_plan(PlanID, Batches), LimitContext).

-spec rollback(plan_id(), [batch()], lim_context()) -> ok | {error, invalid_request_error()}.
rollback(PlanID, Batches, LimitContext) ->
    do('RollbackPlan', construct_plan(PlanID, Batches), LimitContext).

-spec get_plan(plan_id(), lim_context()) -> {ok, [batch()]} | {error, notfound}.
get_plan(PlanID, LimitContext) ->
    case call_accounter('GetPlan', {PlanID}, LimitContext) of
        {ok, #accounter_PostingPlan{batch_list = BatchList}} ->
            {ok, decode_batch_list(BatchList)};
        {exception, #accounter_PlanNotFound{}} ->
            {error, notfound}
    end.

-spec get_balance(account_id(), lim_context()) -> {ok, balance()} | {error, notfound}.
get_balance(AccountID, LimitContext) ->
    case call_accounter('GetAccountByID', {AccountID}, LimitContext) of
        {ok, Result} ->
            {ok, construct_balance(AccountID, Result)};
        {exception, #accounter_AccountNotFound{}} ->
            {error, notfound}
    end.

do(Op, Plan, LimitContext) ->
    case call_accounter(Op, {Plan}, LimitContext) of
        {ok, _Clock} ->
            ok;
        {exception, Exception} ->
            {error, {invalid_request, convert_exception(Exception)}}
    end.

construct_plan_change(PlanID, {BatchID, Postings}) ->
    #accounter_PostingPlanChange{
        id = PlanID,
        batch = #accounter_PostingBatch{
            id = BatchID,
            postings = Postings
        }
    }.

construct_plan(PlanID, Batches) ->
    #accounter_PostingPlan{
        id = PlanID,
        batch_list = [
            #accounter_PostingBatch{
                id = BatchID,
                postings = Postings
            }
            || {BatchID, Postings} <- Batches
        ]
    }.

decode_batch_list(BatchList) ->
    [{BatchID, Postings} || #accounter_PostingBatch{id = BatchID, postings = Postings} <- BatchList].

construct_balance(
    AccountID,
    #accounter_Account{
        own_amount = OwnAmount,
        min_available_amount = MinAvailableAmount,
        max_available_amount = MaxAvailableAmount,
        currency_sym_code = Currency
    }
) ->
    #{
        account_id => AccountID,
        own_amount => OwnAmount,
        min_available_amount => MinAvailableAmount,
        max_available_amount => MaxAvailableAmount,
        currency => Currency
    }.

-spec get_default_currency() -> currency().
get_default_currency() ->
    ?DEFAULT_CURRENCY.

-spec create_account(currency(), lim_context()) -> {ok, account_id()}.
create_account(CurrencyCode, LimitContext) ->
    create_account(CurrencyCode, undefined, LimitContext).

create_account(CurrencyCode, Description, LimitContext) ->
    call_accounter(
        'CreateAccount',
        {construct_prototype(CurrencyCode, Description)},
        LimitContext
    ).

construct_prototype(CurrencyCode, Description) ->
    #accounter_AccountPrototype{
        currency_sym_code = CurrencyCode,
        description = Description
    }.

%%

call_accounter(Function, Args, LimitContext) ->
    {ok, WoodyContext} = lim_context:woody_context(LimitContext),
    lim_client_woody:call(accounter, Function, Args, WoodyContext).

convert_exception(#'InvalidRequest'{errors = Errors}) ->
    Errors;
convert_exception(#accounter_InvalidPostingParams{wrong_postings = Errors}) ->
    maps:fold(fun(_, Error, Acc) -> [Error | Acc] end, [], Errors).
