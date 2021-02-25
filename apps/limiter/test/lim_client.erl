-module(lim_client).

-include_lib("damsel/include/dmsl_proto_limiter_thrift.hrl").

-export([new/0]).
-export([get/3]).
-export([hold/2]).
-export([commit/2]).
-export([partial_commit/2]).
-export([rollback/2]).

-type client() :: woody_context:ctx().

-type limit_id() :: dmsl_proto_limiter_thrift:'LimitID'().
-type limit_change() :: dmsl_proto_limiter_thrift:'LimitChange'().
-type timestamp() :: dmsl_base_thrift:'Timestamp'().

%%% API

-spec new() -> client().
new() ->
    woody_context:new().

-spec get(limit_id(), timestamp(), client()) -> woody:result() | no_return().
get(LimitID, Timestamp, Client) ->
    call('Get', {LimitID, Timestamp}, Client).

-spec hold(limit_change(), client()) -> woody:result() | no_return().
hold(LimitChange, Client) ->
    call('Hold', {LimitChange}, Client).

-spec commit(limit_change(), client()) -> woody:result() | no_return().
commit(LimitChange, Client) ->
    call('Commit', {LimitChange}, Client).

-spec partial_commit(limit_change(), client()) -> woody:result() | no_return().
partial_commit(LimitChange, Client) ->
    call('PartialCommit', {LimitChange}, Client).

-spec rollback(limit_change(), client()) -> woody:result() | no_return().
rollback(LimitChange, Client) ->
    call('Rollback', {LimitChange}, Client).

%%% Internal functions

-spec call(atom(), tuple(), client()) -> woody:result() | no_return().
call(Function, Args, Client) ->
    Call = {{lim_limiter_thrift, 'Limiter'}, Function, Args},
    Opts = #{
        url => <<"http://limiter:8022/v1/limiter">>,
        event_handler => scoper_woody_event_handler,
        transport_opts => #{
            max_connections => 10000
        }
    },
    woody_client:call(Call, Opts, Client).
