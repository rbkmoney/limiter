-module(lim_source).

%% API

-export([ensure_exist/2]).

-type name() :: binary().
-type lim_context() :: lim_context:t().

-import(lim_pipeline, [do/1, unwrap/1]).

-spec ensure_exist(name(), lim_context()) -> {ok, lim_accounting:account_id()} | {error, _TODO}.
ensure_exist(Name, LimitContext) ->
    do(fun() ->
        Timestamp = lim_time:to_rfc3339(lim_time:now()),
        CreateParams = #{
            id => Name,
            type => infinity,
            created_at => Timestamp
        },
        {ok, LimitRangeState} = lim_range_machine:ensure_exist(CreateParams, LimitContext),
        #{account_id := AccountID} =
            unwrap(lim_range_machine:ensure_range_exist_(Timestamp, LimitRangeState, LimitContext)),
        AccountID
    end).
