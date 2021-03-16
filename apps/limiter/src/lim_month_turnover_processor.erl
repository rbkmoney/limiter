-module(lim_month_turnover_processor).

-include_lib("limiter_proto/include/lim_base_thrift.hrl").
-include_lib("limiter_proto/include/lim_limiter_thrift.hrl").
-include_lib("damsel/include/dmsl_accounter_thrift.hrl").

-behaviour(lim_config_machine).

-export([get_limit/3]).
-export([hold/3]).
-export([commit/3]).
-export([rollback/3]).

-type lim_context() :: lim_context:t().
-type lim_id() :: lim_config_machine:lim_id().
-type lim_change() :: lim_config_machine:lim_change().
-type limit() :: lim_config_machine:limit().
-type config() :: lim_config_machine:config().
-type currency() :: lim_base_thrift:'CurrencySymbolicCode'().
-type amount() :: integer().

-type forbidden_operation_amount_error() :: #{
    type := positive | negative,
    partial := amount(),
    full := amount(),
    currency := currency()
}.

-type get_limit_error() :: {limit | range | account | timestamp, notfound}.
-type hold_error() :: {timestamp, notfound} | lim_accounting:invalid_request_error().
-type commit_error() ::
    forbidden_operation_amount_error()
    | {plan | timestamp, notfound}
    | lim_accounting:invalid_request_error().
-type rollback_error() :: {plan, notfound} | lim_accounting:invalid_request_error().

-export_type([get_limit_error/0]).
-export_type([hold_error/0]).
-export_type([commit_error/0]).
-export_type([rollback_error/0]).

-import(lim_pipeline, [do/1, unwrap/1, unwrap/2]).

-spec get_limit(lim_id(), config(), lim_context()) ->
    {ok, limit()} | {error, get_limit_error()}.
get_limit(LimitID, Config, LimitContext) ->
    do(fun() ->
        Timestamp = unwrap(timestamp, lim_context:operation_timestamp(LimitContext)),
        LimitRangeID = construct_range_id(LimitID),
        LimitRange = unwrap(limit, lim_range_machine:get(LimitRangeID, LimitContext)),
        #{own_amount := Amount, currency := Currency} =
            unwrap(lim_range_machine:get_range_balance(Timestamp, LimitRange, LimitContext)),
        #limiter_Limit{
            id = LimitRangeID,
            body = {cash, #limiter_base_Cash{
                amount = Amount,
                currency = #limiter_base_CurrencyRef{symbolic_code = Currency}
            }},
            creation_time = lim_config_machine:created_at(Config),
            description = lim_config_machine:description(Config)
        }
    end).

-spec hold(lim_change(), config(), lim_context()) ->
    ok | {error, hold_error()}.
hold(#limiter_LimitChange{id = LimitID, body = Body}, _Config, LimitContext) ->
    do(fun() ->
        Timestamp = unwrap(timestamp, lim_context:operation_timestamp(LimitContext)),
        CreateParams = #{
            id => construct_range_id(LimitID),
            type => month,
            created_at => Timestamp
        },
        %% TODO: get body type from config and validate body and pass unmarshaled body to p transfer
        {ok, LimitRangeState} = lim_range_machine:ensure_exist(CreateParams, LimitContext),
        {ok, #{account_id_from := AccountIDFrom, account_id_to := AccountIDTo}} =
            lim_range_machine:ensure_range_exist_in_state(Timestamp, LimitRangeState, LimitContext),
        Postings = lim_p_transfer:construct_postings(AccountIDFrom, AccountIDTo, Body),
        lim_accounting:hold(construct_plan_id(LimitID), {1, Postings}, LimitContext)
    end).

-spec commit(lim_change(), config(), lim_context()) ->
    ok | {error, commit_error()}.
commit(LimitChange, _Config, LimitContext) ->
    do(fun() ->
        case lim_context:partial_body(LimitContext) of
            {ok, _} ->
                unwrap(partial_commit(LimitChange, _Config, LimitContext));
            {error, _} ->
                PlanID = construct_plan_id(LimitChange),
                [Batch] = unwrap(plan, lim_accounting:get_plan(PlanID, LimitContext)),
                unwrap(lim_accounting:commit(PlanID, [Batch], LimitContext))
        end
    end).

-spec rollback(lim_change(), config(), lim_context()) ->
    ok | {error, rollback_error()}.
rollback(LimitChange, _Config, LimitContext) ->
    do(fun() ->
        PlanID = construct_plan_id(LimitChange),
        BatchList = unwrap(plan, lim_accounting:get_plan(PlanID, LimitContext)),
        unwrap(lim_accounting:rollback(PlanID, BatchList, LimitContext))
    end).

construct_plan_id(#limiter_LimitChange{change_id = ChangeID}) ->
    ChangeID;
construct_plan_id(ChangeID) ->
    ChangeID.

construct_range_id(LimitID) ->
    <<"Global/Month/Turnover/", LimitID/binary>>.

partial_commit(#limiter_LimitChange{id = LimitID, body = FullBody}, _Config, LimitContext) ->
    do(fun() ->
        PlanID = construct_plan_id(LimitID),
        Timestamp = unwrap(timestamp, lim_context:operation_timestamp(LimitContext)),
        {ok, PartialBody} = lim_context:partial_body(LimitContext),
        {ok, LimitRangeState} = lim_range_machine:get(construct_range_id(LimitID), LimitContext),
        {ok, #{account_id_from := AccountIDFrom, account_id_to := AccountIDTo}} =
            lim_range_machine:get_range(Timestamp, LimitRangeState),
        PartialPostings = lim_p_transfer:construct_postings(AccountIDFrom, AccountIDTo, PartialBody),
        FullPostings = lim_p_transfer:construct_postings(AccountIDFrom, AccountIDTo, FullBody),
        ok = unwrap(assert_partial_posting_amount(PartialPostings, FullPostings)),
        NewBatchList = [{2, lim_p_transfer:reverse_postings(FullPostings)} | [{3, PartialPostings}]],
        unwrap(lim_accounting:plan(PlanID, NewBatchList, LimitContext)),
        unwrap(lim_accounting:commit(PlanID, [{1, FullPostings} | NewBatchList], LimitContext))
    end).

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
