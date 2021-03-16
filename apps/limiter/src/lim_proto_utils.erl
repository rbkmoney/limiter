-module(lim_proto_utils).

-export([serialize/2]).
-export([deserialize/2]).

-type thrift_type() ::
    thrift_base_type()
    | thrift_collection_type()
    | thrift_enum_type()
    | thrift_struct_type().

-type thrift_base_type() ::
    bool
    | double
    | i8
    | i16
    | i32
    | i64
    | string.

-type thrift_collection_type() ::
    {list, thrift_type()}
    | {set, thrift_type()}
    | {map, thrift_type(), thrift_type()}.

-type thrift_enum_type() ::
    {enum, thrift_type_ref()}.

-type thrift_struct_type() ::
    {struct, thrift_struct_flavor(), thrift_type_ref() | thrift_struct_def()}.

-type thrift_struct_flavor() :: struct | union | exception.

-type thrift_type_ref() :: {module(), Name :: atom()}.

-type thrift_struct_def() :: list({
    Tag :: pos_integer(),
    Requireness :: required | optional | undefined,
    Type :: thrift_struct_type(),
    Name :: atom(),
    Default :: any()
}).

-spec serialize(thrift_type(), term()) -> binary().
serialize(Type, Data) ->
    Codec0 = thrift_strict_binary_codec:new(),
    case thrift_strict_binary_codec:write(Codec0, Type, Data) of
        {ok, Codec1} ->
            thrift_strict_binary_codec:close(Codec1);
        {error, Reason} ->
            erlang:error({thrift, {protocol, Reason}})
    end.

-spec deserialize(thrift_type(), binary()) -> term().
deserialize(Type, Data) ->
    Codec0 = thrift_strict_binary_codec:new(Data),
    case thrift_strict_binary_codec:read(Codec0, Type) of
        {ok, Result, Codec1} ->
            case thrift_strict_binary_codec:close(Codec1) of
                <<>> ->
                    Result;
                Leftovers ->
                    erlang:error({thrift, {protocol, {excess_binary_data, Leftovers}}})
            end;
        {error, Reason} ->
            erlang:error({thrift, {protocol, Reason}})
    end.
