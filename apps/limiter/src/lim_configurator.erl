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
handle_function_(
    'Create',
    {#limiter_cfg_LimitCreateParams{
        id = ID,
        name = Name,
        description = Description,
        started_at = StartedAt
    }},
    LimitContext,
    _Opts
) ->
    case mk_limit_config(Name) of
        {ok, Config} ->
            {ok, LimitConfig} = lim_config_machine:start(
                ID,
                Config#{description => Description, started_at => StartedAt},
                LimitContext
            ),
            {ok, marshal(limit_config, LimitConfig)};
        {error, {name, notfound}} ->
            woody_error:raise(
                business,
                #limiter_cfg_LimitConfigNameNotFound{}
            )
    end;
handle_function_('Get', {LimitID}, LimitContext, _Opts) ->
    scoper:add_meta(#{limit_config_id => LimitID}),
    case lim_config_machine:get(LimitID, LimitContext) of
        {ok, LimitConfig} ->
            {ok, marshal(limit_config, LimitConfig)};
        {error, notfound} ->
            woody_error:raise(business, #limiter_cfg_LimitConfigNotFound{})
    end.

mk_limit_config(<<"IdentityMonthTurnover">>) ->
    {ok, #{
        processor_type => <<"TurnoverProcessor">>,
        type => turnover,
        scope => {scope, identity},
        body_type => cash,
        shard_size => 12,
        time_range_type => {calendar, month}
    }};
mk_limit_config(<<"WalletMonthTurnover">>) ->
    {ok, #{
        processor_type => <<"TurnoverProcessor">>,
        type => turnover,
        scope => {scope, wallet},
        body_type => cash,
        shard_size => 12,
        time_range_type => {calendar, month}
    }};
mk_limit_config(<<"ShopMonthTurnover">>) ->
    {ok, #{
        processor_type => <<"TurnoverProcessor">>,
        type => turnover,
        scope => {scope, shop},
        body_type => cash,
        shard_size => 12,
        time_range_type => {calendar, month}
    }};
mk_limit_config(<<"PartyMonthTurnover">>) ->
    {ok, #{
        processor_type => <<"TurnoverProcessor">>,
        type => turnover,
        scope => {scope, party},
        body_type => cash,
        shard_size => 12,
        time_range_type => {calendar, month}
    }};
mk_limit_config(<<"GlobalMonthTurnover">>) ->
    {ok, #{
        processor_type => <<"TurnoverProcessor">>,
        type => turnover,
        scope => global,
        body_type => cash,
        shard_size => 12,
        time_range_type => {calendar, month}
    }};
mk_limit_config(_) ->
    {error, {name, notfound}}.

marshal(limit_config, Config) ->
    #limiter_cfg_LimitConfig{
        id = lim_config_machine:id(Config),
        description = lim_config_machine:description(Config),
        created_at = lim_config_machine:created_at(Config),
        started_at = lim_config_machine:started_at(Config),
        shard_size = lim_config_machine:shard_size(Config)
    }.
