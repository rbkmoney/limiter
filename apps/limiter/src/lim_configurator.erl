-module(lim_configurator).

-include_lib("limiter_proto/include/lim_configurator_thrift.hrl").

%% Woody handler

-behaviour(woody_server_thrift_handler).

-export([handle_function/4]).

%%

-type lim_context() :: lim_context:t().

%%

-spec handle_function(woody:func(), woody:args(), woody_context:ctx(), woody:options()) -> {ok, woody:result()}.
handle_function(Fn, Args, WoodyCtx, Opts) ->
    {ok, LimitContext} = lim_context:create(WoodyCtx),
    scoper:scope(
        configurator,
        fun() -> handle_function_(Fn, Args, LimitContext, Opts) end
    ).

-spec handle_function_(woody:func(), woody:args(), lim_context(), woody:options()) -> {ok, woody:result()}.
handle_function_('Create', {#limiter_cfg_LimitCreateParams{name = Name}}, LimitContext, _Opts) ->
    case mk_limit_config(Name) of
        {ok, Config} ->
            case lim_config_machine:start(Config, LimitContext) of
                {ok, Limit} ->
                    Limit;
                {error, notfound} ->
                    woody_error:raise(business, #limiter_LimitNotFound{})
            end;
        {error, {inconsistent_request, {name, notfound}}} ->
            woody_error:raise(
                business,
                #limiter_cfg_InconsistentRequest{limit_name_not_found = #limiter_cfg_LimitNameNotFound{}}
            )
    end;
handle_function_('Get', {LimitID, Timestamp}, LimitContext0, _Opts) ->
    scoper:add_meta(#{
        limit_id => LimitID,
        timestamp => Timestamp
    }),
    {ok, LimitContext1} = lim_context:set_operation_timestamp(Timestamp, LimitContext0),
    case lim_config_machine:get_limit(LimitID, LimitContext1) of
        {ok, Limit} ->
            {ok, Limit};
        {error, {limit, notfound}} ->
            woody_error:raise(business, #limiter_LimitNotFound{})
    end.

mk_limit_config(<<"GlobalMonthTurnover">>) ->
    {ok, #{
        processor_type => <<"TurnoverProcessor">>,
        type => turnover,
        scope => global,
        body_type => cash,
        time_range => month
    }};
mk_limit_config(_) ->
    {error, {inconsistent_request,  {name, notfound}}}.