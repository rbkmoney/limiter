-module(lim_month_turnover_processor).

-include_lib("limiter_proto/include/lim_base_thrift.hrl").
-include_lib("limiter_proto/include/lim_limiter_thrift.hrl").

-behaviour(lim_config_machine).

-export([get_limit/3]).
-export([hold/3]).
-export([commit/3]).
-export([rollback/3]).

-type lim_context() :: lim_context:t().
-type lim_id() :: lim_config_machine:lim_id().
-type lim_change() :: lim_config_machine:lim_change().
-type limit() :: lim_config_machine:limit().
-type timestamp() :: lim_config_machine:timestamp().
-type config() :: lim_config_machine:config().

-define(SOURCE_NAME, <<"lim_month_turnover_processor/v1">>).

-import(lim_pipeline, [do/1, unwrap/1, unwrap/2]).

-spec get_limit(lim_id(), config(), lim_context()) ->
    {ok, limit()}
    | {error,
        {limit | range, notfound}
        | {balance, not_found}}.
get_limit(LimitID, Config, LimitContext) ->
    do(fun() ->
        Timestamp = unwrap(timestamp, lim_context:operation_timestamp(LimitContext)),
        %% for month limit we can use LimitId as range id
        LimitRange = unwrap(limit, lim_range_machine:get(LimitID, LimitContext)),
        #{own_amount := Amount, currency := Currency} =
            unwrap(lim_range_machine:get_range_balance(Timestamp, LimitRange, LimitContext)),
        #limiter_Limit{
            id = LimitID,
            body = #limiter_LimitBody{
                cash = #limiter_base_Cash{
                    amount = Amount,
                    currency = #limiter_base_CurrencyRef{symbolic_code = Currency}
                }
            },
            creation_time = lim_config_machine:created_at(Config)
            % description = lim_config_machine:description(Config)
        }
    end).

-spec hold(lim_change(), config(), lim_context()) ->
    ok
    | {error,
        {limit, {not_found, {lim_id(), timestamp()}}}
        | lim_accounting:invalid_request_error()}.
hold(
    LimitChange = #limiter_LimitChange{
        id = LimitID,
        operation_timestamp = Timestamp,
        body = #limiter_LimitBody{
            cash = #limiter_base_Cash{
                amount = Amount,
                currency = #limiter_base_CurrencyRef{symbolic_code = Currency}
            }
        }
    },
    _Config,
    LimitContext
) ->
    do(fun() ->
        CreateParams = #{
            id => LimitID,
            type => month,
            created_at => Timestamp
        },
        {ok, LimitRangeState} = lim_range_machine:ensure_exist(CreateParams, LimitContext),
        #{account_id := AccountID} =
            unwrap(lim_range_machine:ensure_range_exist_(Timestamp, LimitRangeState, LimitContext)),
        AccountIDSource = unwrap(lim_source:ensure_exist(?SOURCE_NAME, LimitContext)),
        Postings = lim_p_transfer:construct_postings(AccountIDSource, AccountID, Amount, Currency),
        unwrap(lim_accounting:hold(generate_id(LimitChange), {1, Postings}, LimitContext))
    end).

-spec commit(lim_change(), config(), lim_context()) ->
    ok | {error, {plan, not_found} | lim_accounting:invalid_request_error()}.
commit(LimitChange, _Config, LimitContext) ->
    do(fun() ->
        PlanID = generate_id(LimitChange),
        [Batch] = unwrap(plan, lim_accounting:get_plan(PlanID, LimitContext)),
        unwrap(lim_accounting:commit(PlanID, [Batch], LimitContext))
    end).

-spec rollback(lim_change(), config(), lim_context()) ->
    ok | {error, {plan, not_found} | lim_accounting:invalid_request_error()}.
rollback(LimitChange, _Config, LimitContext) ->
    do(fun() ->
        PlanID = generate_id(LimitChange),
        BatchList = unwrap(plan, lim_accounting:get_plan(PlanID, LimitContext)),
        unwrap(lim_accounting:rollback(PlanID, BatchList, LimitContext))
    end).

generate_id(#limiter_LimitChange{change_id = ChangeID}) ->
    ChangeID.
