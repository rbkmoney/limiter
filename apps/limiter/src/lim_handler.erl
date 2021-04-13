-module(lim_handler).

-include_lib("limiter_proto/include/lim_limiter_thrift.hrl").

%% Woody handler

-behaviour(woody_server_thrift_handler).

-export([handle_function/4]).

%%

-type lim_context() :: lim_context:t().

-define(LIMIT_CHANGE(ID), #limiter_LimitChange{id = ID}).

-define(CASH(
    Amount,
    Currency
),
    #limiter_base_Cash{amount = Amount, currency = #limiter_base_CurrencyRef{symbolic_code = Currency}}
).

%%

-spec handle_function(woody:func(), woody:args(), woody_context:ctx(), woody:options()) -> {ok, woody:result()}.
handle_function(Fn, Args, WoodyCtx, Opts) ->
    {ok, LimitContext} = lim_context:create(WoodyCtx),
    scoper:scope(
        limiter,
        fun() -> handle_function_(Fn, Args, LimitContext, Opts) end
    ).

-spec handle_function_(woody:func(), woody:args(), lim_context(), woody:options()) -> {ok, woody:result()}.
handle_function_('Get', {LimitID, Clock, Context}, LimitContext, _Opts) ->
    scoper:add_meta(#{limit_id => LimitID}),
    case
        lim_config_machine:get_limit(
            LimitID,
            lim_context:set_context(Context, lim_context:set_clock(Clock, LimitContext))
        )
    of
        {ok, Limit} ->
            {ok, Limit};
        {error, Error} ->
            handle_get_error(Error)
    end;
handle_function_('Hold', {LimitChange = ?LIMIT_CHANGE(LimitID), Clock, Context}, LimitContext, _Opts) ->
    scoper:add_meta(#{limit_id => LimitID}),
    case
        lim_config_machine:hold(
            LimitChange,
            lim_context:set_context(Context, lim_context:set_clock(Clock, LimitContext))
        )
    of
        ok ->
            {ok, {vector, #limiter_VectorClock{state = <<>>}}};
        {error, Error} ->
            handle_hold_error(Error)
    end;
handle_function_('Commit', {LimitChange = ?LIMIT_CHANGE(LimitID), Clock, Context}, LimitContext, _Opts) ->
    scoper:add_meta(#{limit_id => LimitID}),
    case
        lim_config_machine:commit(
            LimitChange,
            lim_context:set_context(Context, lim_context:set_clock(Clock, LimitContext))
        )
    of
        ok ->
            {ok, {vector, #limiter_VectorClock{state = <<>>}}};
        {error, Error} ->
            handle_commit_error(Error)
    end;
handle_function_('Rollback', {LimitChange = ?LIMIT_CHANGE(LimitID), Clock, Context}, LimitContext, _Opts) ->
    scoper:add_meta(#{limit_id => LimitID}),
    case
        lim_config_machine:rollback(
            LimitChange,
            lim_context:set_context(Context, lim_context:set_clock(Clock, LimitContext))
        )
    of
        ok ->
            {ok, {vector, #limiter_VectorClock{state = <<>>}}};
        {error, Error} ->
            handle_rollback_error(Error)
    end.

-spec handle_get_error(_) -> no_return().
handle_get_error({_, {limit, notfound}}) ->
    woody_error:raise(business, #limiter_LimitNotFound{});
handle_get_error({_, {range, notfound}}) ->
    woody_error:raise(business, #limiter_LimitNotFound{});
handle_get_error(Error) ->
    handle_default_error(Error).

-spec handle_hold_error(_) -> no_return().
handle_hold_error({_, {invalid_request, Errors}}) ->
    woody_error:raise(business, #limiter_base_InvalidRequest{errors = Errors});
handle_hold_error(Error) ->
    handle_default_error(Error).

-spec handle_commit_error(_) -> no_return().
handle_commit_error({_, {forbidden_operation_amount, Error}}) ->
    handle_forbidden_operation_amount_error(Error);
handle_commit_error({_, {plan, notfound}}) ->
    woody_error:raise(business, #limiter_LimitChangeNotFound{});
handle_commit_error({_, {invalid_request, Errors}}) ->
    woody_error:raise(business, #limiter_base_InvalidRequest{errors = Errors});
handle_commit_error(Error) ->
    handle_default_error(Error).

-spec handle_rollback_error(_) -> no_return().
handle_rollback_error({_, {plan, notfound}}) ->
    woody_error:raise(business, #limiter_LimitChangeNotFound{});
handle_rollback_error({_, {invalid_request, Errors}}) ->
    woody_error:raise(business, #limiter_base_InvalidRequest{errors = Errors});
handle_rollback_error(Error) ->
    handle_default_error(Error).

-spec handle_default_error(_) -> no_return().
handle_default_error({config, notfound}) ->
    woody_error:raise(business, #limiter_LimitNotFound{});
handle_default_error(Error) ->
    handle_unknown_error(Error).

-spec handle_unknown_error(_) -> no_return().
handle_unknown_error(Error) ->
    erlang:error({unknown_error, Error}).

-spec handle_forbidden_operation_amount_error(_) -> no_return().
handle_forbidden_operation_amount_error(#{
    type := Type,
    partial := Partial,
    full := Full
}) ->
    case Type of
        positive ->
            woody_error:raise(business, #limiter_ForbiddenOperationAmount{
                amount = Partial,
                allowed_range = #limiter_base_AmountRange{
                    upper = {inclusive, Full},
                    lower = {inclusive, 0}
                }
            });
        negative ->
            woody_error:raise(business, #limiter_ForbiddenOperationAmount{
                amount = Partial,
                allowed_range = #limiter_base_AmountRange{
                    upper = {inclusive, 0},
                    lower = {inclusive, Full}
                }
            })
    end.
