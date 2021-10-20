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
    'CreateLegacy',
    {#limiter_cfg_LimitCreateParams{
        id = ID,
        name = Name,
        description = Description,
        started_at = StartedAt,
        body_type = BodyType,
        op_behaviour = OpBehaviour
    }},
    LimitContext,
    _Opts
) ->
    case mk_limit_config(Name) of
        {ok, Config} ->
            {ok, LimitConfig} = lim_config_machine:start(
                ID,
                genlib_map:compact(Config#{
                    description => Description,
                    started_at => StartedAt,
                    body_type => lim_config_codec:unmarshal_body_type(BodyType),
                    op_behaviour => lim_config_codec:maybe_apply(
                        OpBehaviour,
                        fun lim_config_codec:unmarshal_op_behaviour/1
                    )
                }),
                LimitContext
            ),
            {ok, lim_config_codec:marshal_config(LimitConfig)};
        {error, {name, notfound}} ->
            woody_error:raise(
                business,
                #limiter_cfg_LimitConfigNameNotFound{}
            )
    end;
handle_function_('Create', {Params}, LimitContext, _Opts) ->
    Config = #{id := ID, type := Type} = lim_config_codec:unmarshal_params(Params),
    {ok, LimitConfig} = lim_config_machine:start(
        ID,
        genlib_map:compact(Config#{
            processor_type => map_type(Type)
        }),
        LimitContext
    ),
    {ok, lim_config_codec:marshal_config(LimitConfig)};
handle_function_('Get', {LimitID}, LimitContext, _Opts) ->
    scoper:add_meta(#{limit_config_id => LimitID}),
    case lim_config_machine:get(LimitID, LimitContext) of
        {ok, LimitConfig} ->
            {ok, lim_config_codec:marshal_config(LimitConfig)};
        {error, notfound} ->
            woody_error:raise(business, #limiter_cfg_LimitConfigNotFound{})
    end.

map_type(turnover) ->
    <<"TurnoverProcessor">>;
map_type(_) ->
    woody_error:raise(
        business,
        #limiter_base_InvalidRequest{errors = [<<"Config type not found.">>]}
    ).

mk_limit_config(<<"ShopDayTurnover">>) ->
    {ok, #{
        processor_type => <<"TurnoverProcessor">>,
        type => turnover,
        scope => {scope, shop},
        shard_size => 7,
        context_type => payment_processing,
        time_range_type => {calendar, day}
    }};
mk_limit_config(<<"PartyDayTurnover">>) ->
    {ok, #{
        processor_type => <<"TurnoverProcessor">>,
        type => turnover,
        scope => {scope, party},
        shard_size => 7,
        context_type => payment_processing,
        time_range_type => {calendar, day}
    }};
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
