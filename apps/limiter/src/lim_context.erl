-module(lim_context).

-include_lib("limiter_proto/include/lim_limiter_context_thrift.hrl").

-export([create/1]).
-export([woody_context/1]).
-export([get_operation/2]).
-export([get_from_context/3]).
-export([get_from_context/4]).

-export([set_context/2]).
-export([set_clock/2]).

-export([clock/1]).

-type woody_context() :: woody_context:ctx().
-type timestamp() :: binary().
-type thrift_context() :: lim_limiter_thrift:'LimitContext'().
-type clock() :: lim_limiter_thrift:'Clock'().
-type id() :: binary().
-type cash() :: lim_body:cash().

-type t() :: #{
    woody_context := woody_context(),
    context => context(),
    clock => clock()
}.

-type context_type() :: payment_processing.
-type context_operation() :: payment_processing_operation().

-type context() :: #{
    payment_processing => payment_processing_context()
}.

-type payment_processing_context() :: #{
    op := payment_processing_operation(),
    invoice => payment_processing_invoice()
}.

-type payment_processing_operation() ::
    invoice
    | invoice_adjustment
    | invoice_payment
    | invoice_payment_adjustment
    | invoice_payment_refund
    | invoice_payment_chargeback.

-type payment_processing_invoice() :: #{
    id => id(),
    owner_id => id(),
    shop_id => id(),
    cost => cash(),
    created_at => timestamp(),
    effective_adjustment => payment_processing_adjustment(),
    effective_payment => payment_processing_payment()
}.

-type payment_processing_adjustment() :: #{
    id => id()
}.

-type payment_processing_payment() :: #{
    id => id(),
    owner_id => id(),
    shop_id => id(),
    cost => cash(),
    capture_cost => cash(),
    created_at => timestamp(),
    flow => instant | hold,
    payer => payment_resource | customer | recurrent,
    effective_adjustment => payment_processing_payment_adjustment(),
    effective_refund => payment_processing_payment_refund(),
    effective_chargeback => payment_processing_payment_chargeback()
}.

-type payment_processing_payment_adjustment() :: #{
    id => id(),
    created_at => timestamp()
}.

-type payment_processing_payment_refund() :: #{
    id => id(),
    cost => cash(),
    created_at => timestamp()
}.

-type payment_processing_payment_chargeback() :: #{
    id => id(),
    levy => cash(),
    body => cash(),
    created_at => timestamp()
}.

-export_type([t/0]).
-export_type([context_type/0]).
-export_type([context_operation/0]).

-spec create(woody_context()) -> {ok, t()}.
create(WoodyContext) ->
    {ok, #{woody_context => WoodyContext}}.

-spec woody_context(t()) -> {ok, woody_context()}.
woody_context(Context) ->
    {ok, maps:get(woody_context, Context)}.

-spec clock(t()) -> {ok, clock()} | {error, notfound}.
clock(#{clock := Clock}) ->
    {ok, Clock};
clock(_) ->
    {error, notfound}.

-spec set_context(thrift_context(), t()) -> t().
set_context(Context, LimContext) ->
    LimContext#{context => unmarshal_context(Context)}.

-spec set_clock(clock(), t()) -> t().
set_clock(Clock, LimContext) ->
    LimContext#{clock => Clock}.

-spec get_operation(context_type(), t()) -> {ok, atom()} | {error, notfound}.
get_operation(Type, #{context := Context}) ->
    case maps:get(Type, Context, undefined) of
        undefined ->
            {error, notfound};
        #{op := Operation} ->
            {ok, Operation}
    end.

-spec get_from_context(context_type(), atom(), t()) -> {ok, term()} | {error, notfound}.
get_from_context(payment_processing, ValueName, LimContext = #{context := Context}) ->
    case maps:get(payment_processing, Context, undefined) of
        undefined ->
            {error, notfound};
        #{op := Operation} ->
            get_from_context(payment_processing, ValueName, Operation, LimContext)
    end;
get_from_context(_, _ValueName, _LimContext) ->
    {error, notfound}.

-spec get_from_context(context_type(), atom(), context_operation(), t()) -> {ok, term()} | {error, notfound}.
get_from_context(payment_processing, ValueName, Op, #{context := #{payment_processing := Context}}) ->
    case get_payment_processing_operation_context(Op, Context) of
        {ok, OperationContext} ->
            case maps:get(ValueName, OperationContext, undefined) of
                undefined ->
                    {error, notfound};
                Value ->
                    {ok, Value}
            end;
        Error ->
            Error
    end;
get_from_context(_, _ValueName, _Op, _LimContext) ->
    {error, notfound}.

get_payment_processing_operation_context(invoice, #{invoice := Invoice}) ->
    {ok, Invoice};
get_payment_processing_operation_context(invoice_adjustment, #{invoice := #{effective_adjustment := Adjustment}}) ->
    {ok, Adjustment};
get_payment_processing_operation_context(invoice_payment, #{invoice := #{effective_payment := Payment}}) ->
    {ok, Payment};
get_payment_processing_operation_context(
    invoice_payment_adjustment,
    #{invoice := #{effective_payment := #{effective_adjustment := Adjustment}}}
) ->
    {ok, Adjustment};
get_payment_processing_operation_context(
    invoice_payment_refund,
    #{invoice := #{effective_payment := #{effective_refund := Refund}}}
) ->
    {ok, Refund};
get_payment_processing_operation_context(
    invoice_payment_chargeback,
    #{invoice := #{effective_payment := #{effective_chargeback := Chargeback}}}
) ->
    {ok, Chargeback};
get_payment_processing_operation_context(_, _) ->
    {error, notfound}.

%%

unmarshal_context(#limiter_context_LimitContext{payment_processing = PaymentProcessing}) ->
    #{payment_processing => unmarshal_payment_processing_context(PaymentProcessing)};
unmarshal_context(_) ->
    #{}.

unmarshal_payment_processing_context(#limiter_context_ContextPaymentProcessing{
    op = {Operation, _},
    invoice = Invoice
}) ->
    genlib_map:compact(#{
        op => Operation,
        invoice => maybe_unmarshal(Invoice, fun unmarshal_payment_processing_invoice/1)
    }).

unmarshal_payment_processing_invoice(#limiter_context_Invoice{
    id = ID,
    owner_id = OwnerID,
    shop_id = ShopID,
    cost = Cost,
    created_at = CreatedAt,
    effective_payment = EffectivePayment,
    effective_adjustment = EffectiveAdjustment
}) ->
    genlib_map:compact(#{
        id => maybe_unmarshal(ID, fun unmarshal_string/1),
        owner_id => maybe_unmarshal(OwnerID, fun unmarshal_string/1),
        shop_id => maybe_unmarshal(ShopID, fun unmarshal_string/1),
        cost => maybe_unmarshal(Cost, fun unmarshal_cash/1),
        created_at => maybe_unmarshal(CreatedAt, fun unmarshal_string/1),
        effective_adjustment => maybe_unmarshal(
            EffectiveAdjustment,
            fun unmarshal_payment_processing_invoice_adjustment/1
        ),
        effective_payment => maybe_unmarshal(EffectivePayment, fun unmarshal_payment_processing_invoice_payment/1)
    }).

unmarshal_payment_processing_invoice_adjustment(#limiter_context_InvoiceAdjustment{id = ID}) ->
    genlib_map:compact(#{
        id => maybe_unmarshal(ID, fun unmarshal_string/1)
    }).

unmarshal_payment_processing_invoice_payment(#limiter_context_InvoicePayment{
    id = ID,
    owner_id = OwnerID,
    shop_id = ShopID,
    cost = Cost,
    capture_cost = CaptureCost,
    created_at = CreatedAt,
    flow = Flow,
    payer = Payer,
    effective_adjustment = EffectiveAdjustment,
    effective_refund = EffectiveRefund,
    effective_chargeback = EffectiveChargeback
}) ->
    genlib_map:compact(#{
        id => maybe_unmarshal(ID, fun unmarshal_string/1),
        owner_id => maybe_unmarshal(OwnerID, fun unmarshal_string/1),
        shop_id => maybe_unmarshal(ShopID, fun unmarshal_string/1),
        cost => maybe_unmarshal(Cost, fun unmarshal_cash/1),
        capture_cost => maybe_unmarshal(CaptureCost, fun unmarshal_cash/1),
        created_at => maybe_unmarshal(CreatedAt, fun unmarshal_string/1),
        flow => maybe_unmarshal(Flow, fun unmarshal_payment_processing_invoice_payment_flow/1),
        payer => maybe_unmarshal(Payer, fun unmarshal_payment_processing_invoice_payment_payer/1),
        effective_adjustment => maybe_unmarshal(
            EffectiveAdjustment,
            fun unmarshal_payment_processing_invoice_payment_adjustment/1
        ),
        effective_refund => maybe_unmarshal(EffectiveRefund, fun unmarshal_payment_processing_invoice_payment_refund/1),
        effective_chargeback => maybe_unmarshal(
            EffectiveChargeback,
            fun unmarshal_payment_processing_invoice_payment_chargeback/1
        )
    }).

unmarshal_payment_processing_invoice_payment_flow({Flow, _}) ->
    Flow.

unmarshal_payment_processing_invoice_payment_payer({Payer, _}) ->
    Payer.

unmarshal_payment_processing_invoice_payment_adjustment(#limiter_context_InvoicePaymentAdjustment{
    id = ID,
    created_at = CreatedAt
}) ->
    genlib_map:compact(#{
        id => maybe_unmarshal(ID, fun unmarshal_string/1),
        created_at => maybe_unmarshal(CreatedAt, fun unmarshal_string/1)
    }).

unmarshal_payment_processing_invoice_payment_refund(#limiter_context_InvoicePaymentRefund{
    id = ID,
    cost = Cost,
    created_at = CreatedAt
}) ->
    genlib_map:compact(#{
        id => maybe_unmarshal(ID, fun unmarshal_string/1),
        cost => maybe_unmarshal(Cost, fun unmarshal_cash/1),
        created_at => maybe_unmarshal(CreatedAt, fun unmarshal_string/1)
    }).

unmarshal_payment_processing_invoice_payment_chargeback(#limiter_context_InvoicePaymentChargeback{
    id = ID,
    levy = Levy,
    body = Body,
    created_at = CreatedAt
}) ->
    genlib_map:compact(#{
        id => maybe_unmarshal(ID, fun unmarshal_string/1),
        levy => maybe_unmarshal(Levy, fun unmarshal_cash/1),
        body => maybe_unmarshal(Body, fun unmarshal_cash/1),
        created_at => maybe_unmarshal(CreatedAt, fun unmarshal_string/1)
    }).

unmarshal_cash(#limiter_base_Cash{amount = Amount, currency = #limiter_base_CurrencyRef{symbolic_code = Currency}}) ->
    lim_body:create_body_from_cash(Amount, Currency).

unmarshal_string(Value) ->
    Value.

maybe_unmarshal(undefined, _) ->
    undefined;
maybe_unmarshal(Value, UnmarshalFun) ->
    UnmarshalFun(Value).
