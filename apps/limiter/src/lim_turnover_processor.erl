-module(lim_turnover_processor).

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
-type amount() :: integer().

-type forbidden_operation_amount_error() :: #{
    type := positive | negative,
    partial := amount(),
    full := amount()
}.

-type get_limit_error() :: {limit | range, notfound}.

-type hold_error() ::
    lim_body:get_body_error()
    | lim_accounting:invalid_request_error().

-type commit_error() ::
    {forbidden_operation_amount, forbidden_operation_amount_error()}
    | {plan, notfound}
    | {full | partial, lim_body:get_body_error()}
    | lim_accounting:invalid_request_error().

-type rollback_error() :: {plan, notfound} | lim_accounting:invalid_request_error().

-export_type([get_limit_error/0]).
-export_type([hold_error/0]).
-export_type([commit_error/0]).
-export_type([rollback_error/0]).

-import(lim_pipeline, [do/1, unwrap/1, unwrap/2]).

-spec get_limit(lim_id(), config(), lim_context()) -> {ok, limit()} | {error, get_limit_error()}.
get_limit(LimitID, Config, LimitContext) ->
    do(fun() ->
        {ok, Timestamp} = lim_context:get_from_context(payment_processing, created_at, LimitContext),
        LimitRangeID = construct_range_id(LimitID, Timestamp, Config, LimitContext),
        LimitRange = unwrap(limit, lim_range_machine:get(LimitRangeID, LimitContext)),
        TimeRange = lim_config_machine:calculate_time_range(Timestamp, Config),
        #{max_available_amount := Amount} =
            unwrap(lim_range_machine:get_range_balance(TimeRange, LimitRange, LimitContext)),
        #limiter_Limit{
            id = LimitRangeID,
            amount = Amount,
            creation_time = lim_config_machine:created_at(Config),
            description = lim_config_machine:description(Config)
        }
    end).

-spec hold(lim_change(), config(), lim_context()) -> ok | {error, hold_error()}.
hold(LimitChange = #limiter_LimitChange{id = LimitID}, Config = #{body_type := BodyType}, LimitContext) ->
    do(fun() ->
        {ok, Timestamp} = lim_context:get_from_context(payment_processing, created_at, LimitContext),
        {ok, Body} = lim_body:get_body(full, Config, LimitContext),
        LimitRangeID = construct_range_id(LimitID, Timestamp, Config, LimitContext),
        Currency =
            case BodyType of
                {cash, CashCurrency} -> CashCurrency;
                amount -> undefined
            end,
        CreateParams = genlib_map:compact(#{
            id => LimitRangeID,
            type => lim_config_machine:time_range_type(Config),
            created_at => Timestamp,
            currency => Currency
        }),
        {ok, LimitRangeState} = lim_range_machine:ensure_exist(CreateParams, LimitContext),
        TimeRange = lim_config_machine:calculate_time_range(Timestamp, Config),
        {ok, #{account_id_from := AccountIDFrom, account_id_to := AccountIDTo}} =
            lim_range_machine:ensure_range_exist_in_state(TimeRange, LimitRangeState, LimitContext),
        Postings = lim_p_transfer:construct_postings(AccountIDFrom, AccountIDTo, Body),
        Postings1 = apply_op_behaviour(Postings, LimitContext, Config),
        lim_accounting:hold(construct_plan_id(LimitChange), {1, Postings1}, LimitContext)
    end).

-spec commit(lim_change(), config(), lim_context()) -> ok | {error, commit_error()}.
commit(LimitChange, Config, LimitContext) ->
    do(fun() ->
        case lim_body:get_body(partial, Config, LimitContext) of
            {ok, Body} ->
                unwrap(partial_commit(Body, LimitChange, Config, LimitContext));
            {error, notfound} ->
                PlanID = construct_plan_id(LimitChange),
                [Batch] = unwrap(plan, lim_accounting:get_plan(PlanID, LimitContext)),
                unwrap(lim_accounting:commit(PlanID, [Batch], LimitContext))
        end
    end).

-spec rollback(lim_change(), config(), lim_context()) -> ok | {error, rollback_error()}.
rollback(LimitChange, _Config, LimitContext) ->
    do(fun() ->
        PlanID = construct_plan_id(LimitChange),
        BatchList = unwrap(plan, lim_accounting:get_plan(PlanID, LimitContext)),
        unwrap(lim_accounting:rollback(PlanID, BatchList, LimitContext))
    end).

construct_plan_id(#limiter_LimitChange{change_id = ChangeID}) ->
    ChangeID.

construct_range_id(LimitID, Timestamp, Config, LimitContext) ->
    {ok, Prefix} = lim_config_machine:mk_scope_prefix(Config, LimitContext),
    ShardID = lim_config_machine:calculate_shard_id(Timestamp, Config),
    <<LimitID/binary, Prefix/binary, "/", ShardID/binary>>.

partial_commit(PartialBody, LimitChange = #limiter_LimitChange{id = LimitID}, Config, LimitContext) ->
    do(fun() ->
        {ok, Timestamp} = lim_context:get_from_context(payment_processing, created_at, LimitContext),
        {ok, FullBody} = lim_body:get_body(full, Config, LimitContext),
        ok = unwrap(assert_partial_body(PartialBody, FullBody)),

        LimitRangeID = construct_range_id(LimitID, Timestamp, Config, LimitContext),
        {ok, LimitRangeState} = lim_range_machine:get(
            LimitRangeID,
            LimitContext
        ),
        TimeRange = lim_config_machine:calculate_time_range(Timestamp, Config),
        {ok, #{account_id_from := AccountIDFrom, account_id_to := AccountIDTo}} =
            lim_range_machine:get_range(TimeRange, LimitRangeState),
        PartialPostings0 = lim_p_transfer:construct_postings(AccountIDFrom, AccountIDTo, PartialBody),
        FullPostings0 = lim_p_transfer:construct_postings(AccountIDFrom, AccountIDTo, FullBody),
        PartialPostings1 = apply_op_behaviour(PartialPostings0, LimitContext, Config),
        FullPostings1 = apply_op_behaviour(FullPostings0, LimitContext, Config),
        NewBatchList = [{2, lim_p_transfer:reverse_postings(FullPostings1)} | [{3, PartialPostings1}]],
        PlanID = construct_plan_id(LimitChange),
        unwrap(lim_accounting:plan(PlanID, NewBatchList, LimitContext)),
        unwrap(lim_accounting:commit(PlanID, [{1, FullPostings1} | NewBatchList], LimitContext))
    end).

apply_op_behaviour(Posting, LimitContext, #{op_behaviour := ComputationConfig}) ->
    {ok, Operation} = lim_context:get_operation(payment_processing, LimitContext),
    case maps:get(Operation, ComputationConfig, undefined) of
        subtraction ->
            lim_p_transfer:reverse_postings(Posting);
        Type when Type =:= undefined orelse Type =:= additional ->
            Posting
    end;
apply_op_behaviour(Body, _LimitContext, _Config) ->
    Body.

assert_partial_body(
    {cash, #{amount := Partial, currency := Currency}},
    {cash, #{amount := Full, currency := Currency}}
) ->
    compare_amount(Partial, Full, Currency);
assert_partial_body(
    {cash, #{amount := Partial, currency := PartialCurrency}},
    {cash, #{amount := Full, currency := FullCurrency}}
) ->
    erlang:error({invalid_partial_cash, {Partial, PartialCurrency}, {Full, FullCurrency}}).

compare_amount(Partial, Full, Currency) when Full > 0 ->
    case Partial =< Full of
        true ->
            ok;
        false ->
            {error,
                {forbidden_operation_amount,
                    genlib_map:compact(#{
                        type => positive,
                        partial => Partial,
                        full => Full,
                        currency => Currency
                    })}}
    end;
compare_amount(Partial, Full, Currency) when Full < 0 ->
    case Partial >= Full of
        true ->
            ok;
        false ->
            {error,
                {forbidden_operation_amount,
                    genlib_map:compact(#{
                        type => negative,
                        partial => Partial,
                        full => Full,
                        currency => Currency
                    })}}
    end.
