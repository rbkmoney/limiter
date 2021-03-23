-module(lim_context).

-include_lib("limiter_proto/include/lim_limiter_thrift.hrl").

-export([create/1]).
-export([woody_context/1]).
-export([operation_timestamp/1]).
-export([partial_body/1]).
-export([party_id/1]).
-export([shop_id/1]).
-export([wallet_id/1]).
-export([identity_id/1]).
-export([clock/1]).

-export([set_context/2]).
-export([set_clock/2]).

-type woody_context() :: woody_context:ctx().
-type timestamp() :: binary().
-type context() :: lim_limiter_thrift:'LimitContext'().
-type body() :: lim_limiter_thrift:'LimitBody'().
-type clock() :: lim_limiter_thrift:'Clock'().
-type party_id() :: lim_limiter_thrift:'PartyID'().
-type shop_id() :: lim_limiter_thrift:'ShopID'().
-type wallet_id() :: lim_limiter_thrift:'WalletID'().
-type identity_id() :: lim_limiter_thrift:'IdentityID'().

-type t() :: #{
    woody_context := woody_context(),
    operation_timestamp => timestamp(),
    context => context(),
    clock => clock()
}.

-export_type([t/0]).

-spec create(woody_context()) -> {ok, t()}.
create(WoodyContext) ->
    {ok, #{woody_context => WoodyContext}}.

-spec woody_context(t()) -> {ok, woody_context()}.
woody_context(Context) ->
    {ok, maps:get(woody_context, Context)}.

-spec operation_timestamp(t()) -> {ok, timestamp()} | {error, notfound}.
operation_timestamp(#{context := #limiter_LimitContext{operation_timestamp = Timestamp}}) when
    Timestamp =/= undefined
->
    {ok, Timestamp};
operation_timestamp(_) ->
    {error, notfound}.

-spec partial_body(t()) -> {ok, body()} | {error, notfound}.
partial_body(#{context := #limiter_LimitContext{partial_body = PartialBody}}) when PartialBody =/= undefined ->
    {ok, PartialBody};
partial_body(_) ->
    {error, notfound}.

-spec party_id(t()) -> {ok, party_id()} | {error, notfound}.
party_id(#{context := #limiter_LimitContext{party_id = Value}}) when Value =/= undefined ->
    {ok, Value};
party_id(_) ->
    {error, notfound}.

-spec shop_id(t()) -> {ok, shop_id()} | {error, notfound}.
shop_id(#{context := #limiter_LimitContext{shop_id = Value}}) when Value =/= undefined ->
    {ok, Value};
shop_id(_) ->
    {error, notfound}.

-spec wallet_id(t()) -> {ok, wallet_id()} | {error, notfound}.
wallet_id(#{context := #limiter_LimitContext{wallet_id = Value}}) when Value =/= undefined ->
    {ok, Value};
wallet_id(_) ->
    {error, notfound}.

-spec identity_id(t()) -> {ok, identity_id()} | {error, notfound}.
identity_id(#{context := #limiter_LimitContext{identity_id = Value}}) when Value =/= undefined ->
    {ok, Value};
identity_id(_) ->
    {error, notfound}.

-spec clock(t()) -> {ok, clock()} | {error, notfound}.
clock(#{clock := Clock}) ->
    {ok, Clock};
clock(_) ->
    {error, notfound}.

-spec set_context(context(), t()) -> t().
set_context(Context, LimContext) ->
    LimContext#{context => Context}.

-spec set_clock(clock(), t()) -> t().
set_clock(Clock, LimContext) ->
    LimContext#{clock => Clock}.
