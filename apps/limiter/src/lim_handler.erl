-module(lim_handler).

-include_lib("limiter_proto/include/lim_limiter_thrift.hrl").

%% Woody handler

-behaviour(woody_server_thrift_handler).

-export([handle_function/4]).

%%

-type lim_context() :: lim_context:t().

-define(LIMIT_CHANGE(ID, Timestamp), #limiter_LimitChange{id = ID, operation_timestamp = Timestamp}).

-define(CASH(
    Amount,
    Currency
),
    #'Cash'{amount = Amount, currency = #'CurrencyRef'{symbolic_code = Currency}}
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
handle_function_('Get', {LimitID, Timestamp}, LimitContext0, _Opts) ->
    scoper:add_meta(#{
        limit_id => LimitID,
        timestamp => Timestamp
    }),
    {ok, LimitContext1} = lim_context:set_operation_timestamp(Timestamp, LimitContext0),
    case lim_config_machine:get_limit(LimitID, LimitContext1) of
        {ok, Limit} ->
            {ok, Limit};
        {error, {limit, {not_found, _}}} ->
            woody_error:raise(business, #limiter_LimitNotFound{});
        {error, {balance, not_found}} ->
            _ = logger:warning("Requested limit account doesn't exist in accounter"),
            woody_error:raise(business, #limiter_LimitNotFound{})
    end;
handle_function_('Hold', {LimitChange = ?LIMIT_CHANGE(LimitID, Timestamp)}, LimitContext, _Opts) ->
    scoper:add_meta(#{
        limit_id => LimitID,
        timestamp => Timestamp
    }),
    case lim_config_machine:hold(LimitChange, LimitContext) of
        ok ->
            {ok, ok};
        {error, {limit, {not_found, _}}} ->
            woody_error:raise(business, #limiter_LimitNotFound{});
        {error, {invalid_request, Errors}} ->
            woody_error:raise(business, #limiter_base_InvalidRequest{errors = Errors})
    end;
handle_function_('Commit', {LimitChange = ?LIMIT_CHANGE(LimitID, Timestamp)}, LimitContext, _Opts) ->
    scoper:add_meta(#{
        limit_id => LimitID,
        timestamp => Timestamp
    }),
    case lim_config_machine:commit(LimitChange, LimitContext) of
        ok ->
            {ok, ok};
        {error, {plan, not_found}} ->
            woody_error:raise(business, #limiter_LimitChangeNotFound{});
        {error, {invalid_request, Errors}} ->
            woody_error:raise(business, #limiter_base_InvalidRequest{errors = Errors})
    end;
handle_function_('Rollback', {LimitChange = ?LIMIT_CHANGE(LimitID, Timestamp)}, LimitContext, _Opts) ->
    scoper:add_meta(#{
        limit_id => LimitID,
        timestamp => Timestamp
    }),
    case lim_config_machine:rollback(LimitChange, LimitContext) of
        ok ->
            {ok, ok};
        {error, {plan, not_found}} ->
            woody_error:raise(business, #limiter_LimitChangeNotFound{});
        {error, {invalid_request, Errors}} ->
            woody_error:raise(business, #limiter_base_InvalidRequest{errors = Errors})
    end.
