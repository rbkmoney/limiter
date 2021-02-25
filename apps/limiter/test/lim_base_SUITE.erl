-module(lim_base_SUITE).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

-include_lib("damsel/include/dmsl_domain_thrift.hrl").
-include_lib("damsel/include/dmsl_accounter_thrift.hrl").
-include_lib("damsel/include/dmsl_proto_limiter_thrift.hrl").

-export([all/0]).

-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_testcase/2]).
-export([end_per_testcase/2]).

-export([rollback/1]).
-export([partial_commit_operation_amount_error/1]).
-export([partial_commit/1]).
-export([commit/1]).
-export([get_limit/1]).
-export([get_limit_not_found/1]).
-export([get_account/1]).

-type test_case_name() :: atom().

-define(RATE_SOURCE_ID, <<"dummy_source_id">>).
-define(APP, limiter).

%% tests descriptions

-spec all() -> [test_case_name()].
all() ->
    [
        {group, default}
    ].

-spec groups() -> [{atom(), list(), [test_case_name()]}].
groups() ->
    [
        {default, [], [
            get_account,
            get_limit_not_found,
            get_limit,
            commit,
            partial_commit,
            partial_commit_operation_amount_error,
            rollback
        ]}
    ].

-type config() :: [{atom(), any()}].

-spec init_per_suite(config()) -> config().
init_per_suite(Config) ->
    Apps =
        genlib_app:start_application_with(limiter, [
            {service_clients, #{
                accounter => #{
                    url => <<"http://shumway:8022/accounter">>
                }
            }}
        ]),
    AccountID = create_account(<<"RUB">>),
    set_cfg_default_limit(AccountID),
    [{apps, Apps}] ++ Config.

-spec end_per_suite(config()) -> _.
end_per_suite(Config) ->
    [application:stop(App) || App <- proplists:get_value(apps, Config)],
    Config.

-spec init_per_testcase(test_case_name(), config()) -> config().
init_per_testcase(_Name, C) ->
    C.

-spec end_per_testcase(test_case_name(), config()) -> config().
end_per_testcase(_Name, _C) ->
    ok.

%%

-spec rollback(config()) -> _.
rollback(C) ->
    #{limit_id := LimitID, client := Client} = prepare_environment(<<"RollbackID">>, C),
    LimitChange = #proto_limiter_LimitChange{
        id = LimitID,
        change_id = LimitID,
        cash = #domain_Cash{amount = 10000, currency = #domain_CurrencyRef{symbolic_code = <<"RUB">>}},
        operation_timestamp = <<"2021-02-02T00:00:00Z">>
    },
    {ok, ok} = lim_client:hold(LimitChange, Client),
    {ok, ok} = lim_client:rollback(LimitChange, Client).

-spec partial_commit_operation_amount_error(config()) -> _.
partial_commit_operation_amount_error(C) ->
    #{limit_id := LimitID, client := Client} = prepare_environment(<<"PartialCommitIDError">>, C),
    LimitChange0 = #proto_limiter_LimitChange{
        id = LimitID,
        change_id = LimitID,
        cash = #domain_Cash{amount = 10000, currency = #domain_CurrencyRef{symbolic_code = <<"RUB">>}},
        operation_timestamp = <<"2021-02-02T00:00:00Z">>
    },
    {ok, ok} = lim_client:hold(LimitChange0, Client),
    LimitChange1 = #proto_limiter_LimitChange{
        id = LimitID,
        change_id = LimitID,
        cash = #domain_Cash{amount = 12000, currency = #domain_CurrencyRef{symbolic_code = <<"RUB">>}},
        operation_timestamp = <<"2021-02-02T00:00:00Z">>
    },
    {exception, #proto_limiter_ForbiddenOperationAmount{}} = lim_client:partial_commit(LimitChange1, Client).

-spec partial_commit(config()) -> _.
partial_commit(C) ->
    #{limit_id := LimitID, client := Client} = prepare_environment(<<"PartialCommitID">>, C),
    LimitChange0 = #proto_limiter_LimitChange{
        id = LimitID,
        change_id = LimitID,
        cash = #domain_Cash{amount = 10000, currency = #domain_CurrencyRef{symbolic_code = <<"RUB">>}},
        operation_timestamp = <<"2021-02-02T00:00:00Z">>
    },
    {ok, ok} = lim_client:hold(LimitChange0, Client),
    LimitChange1 = #proto_limiter_LimitChange{
        id = LimitID,
        change_id = LimitID,
        cash = #domain_Cash{amount = 8000, currency = #domain_CurrencyRef{symbolic_code = <<"RUB">>}},
        operation_timestamp = <<"2021-02-02T00:00:00Z">>
    },
    {ok, ok} = lim_client:partial_commit(LimitChange1, Client).

-spec commit(config()) -> _.
commit(C) ->
    #{limit_id := LimitID, client := Client} = prepare_environment(<<"CommitID">>, C),
    LimitChange = #proto_limiter_LimitChange{
        id = LimitID,
        change_id = LimitID,
        cash = #domain_Cash{amount = 10000, currency = #domain_CurrencyRef{symbolic_code = <<"RUB">>}},
        operation_timestamp = <<"2021-02-02T00:00:00Z">>
    },
    {ok, ok} = lim_client:hold(LimitChange, Client),
    {ok, ok} = lim_client:commit(LimitChange, Client).

-spec get_limit(config()) -> _.
get_limit(C) ->
    #{limit_id := LimitID, client := Client} = prepare_environment(<<"GetLimitID">>, C),
    {ok, _Limit} = lim_client:get(LimitID, <<"2021-02-02T00:00:00Z">>, Client).

-spec get_limit_not_found(config()) -> _.
get_limit_not_found(_C) ->
    Client = lim_client:new(),
    {exception, #proto_limiter_LimitNotFound{}} = lim_client:get(<<"SomeID">>, <<"Timestamp">>, Client).

-spec get_account(config()) -> _.
get_account(_C) ->
    AccountID = create_account(<<"RUB">>),
    {ok, _Balance} = call_accounter('GetAccountByID', {AccountID}).

%%

prepare_environment(LimitName, _C) ->
    Client = lim_client:new(),
    AccountID = create_account(<<"RUB">>),
    Limit = #{
        id => AccountID,
        currency => <<"RUB">>,
        created_at => <<"2021-01-01T00:00:00Z">>,
        description => <<"dummy_limit">>,
        time_range => #{
            upper => <<"2021-03-01T00:00:00Z">>,
            lower => <<"2021-02-01T00:00:00Z">>
        }
    },
    AccountIDBinary = list_to_binary(integer_to_list(AccountID)),
    LimitID = <<AccountIDBinary/binary, LimitName/binary>>,
    set_cfg_limit(LimitID, Limit),
    #{
        client => Client,
        limit => Limit,
        limit_id => LimitID,
        account_id => AccountID
    }.

create_account(CurrencyCode) ->
    create_account(CurrencyCode, undefined).

create_account(CurrencyCode, Description) ->
    case call_accounter('CreateAccount', {construct_prototype(CurrencyCode, Description)}) of
        {ok, Result} ->
            Result;
        {exception, Exception} ->
            error({accounting, Exception})
    end.

construct_prototype(CurrencyCode, Description) ->
    #accounter_AccountPrototype{
        currency_sym_code = CurrencyCode,
        description = Description
    }.

call_accounter(Function, Args) ->
    WoodyContext = woody_context:new(),
    lim_client_woody:call(
        accounter,
        Function,
        Args,
        WoodyContext
    ).

set_cfg_default_limit(AccountID) ->
    {ok, Source} = application:get_env(?APP, limit_source),
    ok = application:set_env(
        ?APP,
        limit_source,
        Source#{id => AccountID}
    ).

set_cfg_limit(LimitID, Limit) ->
    {ok, Limits} = application:get_env(?APP, limits),
    ok = application:set_env(
        ?APP,
        limits,
        Limits#{LimitID => [Limit]}
    ).
