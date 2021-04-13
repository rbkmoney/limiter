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
        started_at = StartedAt,
        body_type = BodyType
    }},
    LimitContext,
    _Opts
) ->
    case mk_limit_config(Name) of
        {ok, Config} ->
            {ok, LimitConfig} = lim_config_machine:start(
                ID,
                Config#{description => Description, started_at => StartedAt, body_type => unmarshal_body_type(BodyType)},
                LimitContext
            ),
            {ok, marshal_config(LimitConfig)};
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
            {ok, marshal_config(LimitConfig)};
        {error, notfound} ->
            woody_error:raise(business, #limiter_cfg_LimitConfigNotFound{})
    end.

mk_limit_config(<<"ShopMonthTurnover">>) ->
    {ok, #{
        processor_type => <<"TurnoverProcessor">>,
        type => turnover,
        scope => {scope, shop},
        shard_size => 12,
        context_type => payment_processing,
        time_range_type => {calendar, month}
    }};
mk_limit_config(<<"PartyMonthTurnover">>) ->
    {ok, #{
        processor_type => <<"TurnoverProcessor">>,
        type => turnover,
        scope => {scope, party},
        shard_size => 12,
        context_type => payment_processing,
        time_range_type => {calendar, month}
    }};
mk_limit_config(<<"GlobalMonthTurnover">>) ->
    {ok, #{
        processor_type => <<"TurnoverProcessor">>,
        type => turnover,
        scope => global,
        shard_size => 12,
        context_type => payment_processing,
        time_range_type => {calendar, month}
    }};
mk_limit_config(_) ->
    {error, {name, notfound}}.

marshal_config(Config) ->
    #limiter_config_LimitConfig{
        id = lim_config_machine:id(Config),
        processor_type = lim_config_machine:processor_type(Config),
        description = lim_config_machine:description(Config),
        body_type = marshal_body_type(lim_config_machine:body_type(Config)),
        created_at = lim_config_machine:created_at(Config),
        started_at = lim_config_machine:started_at(Config),
        shard_size = lim_config_machine:shard_size(Config),
        time_range_type = marshal_time_range_type(lim_config_machine:time_range_type(Config)),
        context_type = marshal_context_type(lim_config_machine:context_type(Config)),
        type = marshal_type(lim_config_machine:type(Config)),
        scope = marshal_scope(lim_config_machine:scope(Config))
    }.

marshal_body_type(amount) ->
    {amount, #limiter_config_LimitBodyTypeAmount{}};
marshal_body_type({cash, Currency}) ->
    {cash, #limiter_config_LimitBodyTypeCash{currency = Currency}}.

marshal_time_range_type({calendar, CalendarType}) ->
    {calendar, marshal_calendar_time_range_type(CalendarType)};
marshal_time_range_type({interval, Amount}) ->
    {interval, #time_range_TimeRangeTypeInterval{amount = Amount}}.

marshal_context_type(payment_processing) ->
    {payment_processing, #limiter_config_LimitContextTypePaymentProcessing{}}.

marshal_calendar_time_range_type(day) ->
    {day, #time_range_TimeRangeTypeCalendarDay{}};
marshal_calendar_time_range_type(week) ->
    {week, #time_range_TimeRangeTypeCalendarWeek{}};
marshal_calendar_time_range_type(month) ->
    {month, #time_range_TimeRangeTypeCalendarMonth{}};
marshal_calendar_time_range_type(year) ->
    {year, #time_range_TimeRangeTypeCalendarYear{}}.

marshal_type(turnover) ->
    {turnover, #limiter_config_LimitTypeTurnover{}}.

marshal_scope({scope, Type}) ->
    {scope, marshal_scope_type(Type)};
marshal_scope(global) ->
    {scope_global, #limiter_config_LimitScopeGlobal{}}.

marshal_scope_type(party) ->
    {party, #limiter_config_LimitScopeTypeParty{}};
marshal_scope_type(shop) ->
    {shop, #limiter_config_LimitScopeTypeShop{}};
marshal_scope_type(wallet) ->
    {wallet, #limiter_config_LimitScopeTypeWallet{}};
marshal_scope_type(identity) ->
    {identity, #limiter_config_LimitScopeTypeIdentity{}}.

%%

unmarshal_body_type({amount, _}) ->
    amount;
unmarshal_body_type({cash, #limiter_config_LimitBodyTypeCash{currency = Currency}}) ->
    {cash, Currency}.
