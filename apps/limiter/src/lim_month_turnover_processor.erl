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
    body_type := lim_config_machine:body_type(),
    type := positive | negative,
    partial := amount(),
    full := amount(),
    currency => currency()
}.

-type get_limit_error() :: {limit | range | account | timestamp, notfound}.
-type hold_error() ::
    {timestamp, notfound}
    | lim_body:validate_error()
    | lim_accounting:invalid_request_error().
-type commit_error() ::
    {forbidden_operation_amount, forbidden_operation_amount_error()}
    | {plan | timestamp, notfound}
    | {full | partial, lim_body:validate_error()}
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
        LimitRangeID = construct_range_id(LimitID, Timestamp, Config),
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
hold(LimitChange = #limiter_LimitChange{id = LimitID, body = ThriftBody}, Config, LimitContext) ->
    do(fun() ->
        Timestamp = unwrap(timestamp, lim_context:operation_timestamp(LimitContext)),
        Body = unwrap(lim_body:extract_and_validate_body(ThriftBody, Config)),
        CreateParams = #{
            id => construct_range_id(LimitID, Timestamp, Config),
            type => lim_config_machine:time_range_type(Config),
            created_at => Timestamp
        },
        {ok, LimitRangeState} = lim_range_machine:ensure_exist(CreateParams, LimitContext),
        TimeRange = lim_config_machine:calculate_time_range(Timestamp, Config),
        {ok, #{account_id_from := AccountIDFrom, account_id_to := AccountIDTo}} =
            lim_range_machine:ensure_range_exist_in_state(TimeRange, LimitRangeState, LimitContext),
        Postings = lim_p_transfer:construct_postings(AccountIDFrom, AccountIDTo, Body),
        lim_accounting:hold(construct_plan_id(LimitChange), {1, Postings}, LimitContext)
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
    ChangeID.

construct_range_id(LimitID, Timestamp, Config) ->
    ShardID = lim_config_machine:calculate_shard_id(Timestamp, Config),
    <<LimitID/binary, "/", ShardID/binary>>.

partial_commit(LimitChange = #limiter_LimitChange{id = LimitID, body = ThriftFullBody}, Config, LimitContext) ->
    do(fun() ->
        Timestamp = unwrap(timestamp, lim_context:operation_timestamp(LimitContext)),
        {ok, ThriftPartialBody} = lim_context:partial_body(LimitContext),
        PartialBody = unwrap(partial, lim_body:extract_and_validate_body(ThriftPartialBody, Config)),
        FullBody = unwrap(full, lim_body:extract_and_validate_body(ThriftFullBody, Config)),
        ok = unwrap(assert_partial_body(PartialBody, FullBody)),

        {ok, LimitRangeState} = lim_range_machine:get(construct_range_id(LimitID, Timestamp, Config), LimitContext),
        {ok, #{account_id_from := AccountIDFrom, account_id_to := AccountIDTo}} =
            lim_range_machine:get_range(Timestamp, LimitRangeState),

        PartialPostings = lim_p_transfer:construct_postings(AccountIDFrom, AccountIDTo, PartialBody),
        FullPostings = lim_p_transfer:construct_postings(AccountIDFrom, AccountIDTo, FullBody),
        NewBatchList = [{2, lim_p_transfer:reverse_postings(FullPostings)} | [{3, PartialPostings}]],

        PlanID = construct_plan_id(LimitChange),
        unwrap(lim_accounting:plan(PlanID, NewBatchList, LimitContext)),
        unwrap(lim_accounting:commit(PlanID, [{1, FullPostings} | NewBatchList], LimitContext))
    end).

assert_partial_body({cash, {Partial, Currency}}, {cash, {Full, Currency}}) ->
    compare_amount(cash, Partial, Full, Currency);
assert_partial_body({amount, Partial}, {amount, Full}) ->
    compare_amount(amount, Partial, Full);
assert_partial_body({cash, {Partial, PartialCurrency}}, {cash, {Full, FullCurrency}}) ->
    erlang:error({invalid_partial_cash, {Partial, PartialCurrency}, {Full, FullCurrency}}).

compare_amount(BodyType, Partial, Full) ->
    compare_amount(BodyType, Partial, Full, undefined).

compare_amount(BodyType, Partial, Full, Currency) when Full > 0 ->
    case Partial =< Full of
        true ->
            ok;
        false ->
            {error,
                {forbidden_operation_amount, genlib_map:compact(#{
                    body_type => BodyType,
                    type => positive,
                    partial => Partial,
                    full => Full,
                    currency => Currency
                })}}
    end;
compare_amount(BodyType, Partial, Full, Currency) when Full < 0 ->
    case Partial >= Full of
        true ->
            ok;
        false ->
            {error,
                {forbidden_operation_amount, genlib_map:compact(#{
                    body_type => BodyType,
                    type => negative,
                    partial => Partial,
                    full => Full,
                    currency => Currency
                })}}
    end.
