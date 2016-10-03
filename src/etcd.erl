-module(etcd).

-export([start/0, stop/0]).
-export([set/4, set/5]).
-export([test_and_set/5, test_and_set/6]).
-export([get/3]).
-export([delete/3]).
-export([watch/3, watch/4]).

-include("etcd_types.hrl").

%% @doc Start application with all depencies
-spec start() -> ok | {error, term()}.
start() ->
  case application:ensure_all_started(etcd) of
    {ok, _} ->
      ok;
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Stop application
-spec stop() -> ok | {error, term()}.
stop() ->
  application:stop(etcd).

%% @spec (Url, Key, Value, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   Value = binary() | string()
%%   Timeout = pos_integer() | 'infinity'
%%   Result = {ok, response() | [response()]} | {error, atom()}.
%% @end
-spec set(url(), key(), value(), pos_timeout()) -> result().
set(Url, Key, Value, Timeout) ->
  FullUrl = url_prefix(Url) ++ "/keys" ++ convert_to_string(Key),
  Result = post_request(put, FullUrl, [{"value", Value}], Timeout),
  handle_request_result(Result).

%% @spec (Url, Key, Value, TTL, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   Value = binary() | string()
%%   TTL = pos_integer()
%%   Timeout = pos_integer() | infinity
%%   Result = {ok, response() | [response()]} | {error, atom()}.
%% @end
-spec set(url(), key(), prev_value(), pos_integer(), pos_timeout()) -> result().
set(Url, Key, Value, TTL, Timeout) ->
  FullUrl = url_prefix(Url) ++ "/keys" ++ convert_to_string(Key),
  Result = post_request(put, FullUrl, [{"value", Value}, {"ttl", TTL}], Timeout),
  handle_request_result(Result).

%% @spec (Url, Key, PrevValue, Value, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   PrevValue = binary() | {prevValue, binary()} | {prevIndex, integer()} | {prevExist, boolean()}
%%   Value = binary() | string()
%%   Timeout = pos_integer() | 'infinity'
%%   Result = {ok, response() | [response()]} | {error, atom()}.
%% @end
-spec test_and_set(url(), key(), prev_value(), value(), pos_timeout()) -> result().
test_and_set(Url, Key, PrevValue, Value, Timeout) when is_binary(PrevValue) ->
  test_and_set(Url, Key, {prevValue, PrevValue}, Value, Timeout);
test_and_set(Url, Key, {PrevWhat, Prev}, Value, Timeout) when PrevWhat == prevValue ; PrevWhat == prevIndex ;  PrevWhat == prevExist ->
  FullUrl = url_prefix(Url) ++ "/keys" ++ convert_to_string(Key),
  Result = post_request(put, FullUrl, [{value, Value}, {PrevWhat, Prev}], Timeout),
  handle_request_result(Result).

%% @spec (Url, Key, PrevValue, Value, TTL, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   PrevValue = binary() | {prevValue, binary()} | {prevIndex, integer()} | {prevExist, boolean()}
%%   Value = binary() | string()
%%   TTL = pos_integer()
%%   Timeout = pos_integer() | infinity
%%   Result = {ok, response() | [response()]} | {error, atom()}.
%% @end
-spec test_and_set(url(), key(), prev_value(), value(), pos_integer(), pos_timeout()) -> result().
test_and_set(Url, Key, PrevValue, Value, TTL, Timeout) when is_binary(PrevValue) ->
  test_and_set(Url, Key, {prevValue, PrevValue}, Value, TTL, Timeout);
test_and_set(Url, Key, {PrevWhat, Prev}, Value, TTL, Timeout) when PrevWhat == prevValue ; PrevWhat == prevIndex ;  PrevWhat == prevExist  ->
  FullUrl = url_prefix(Url) ++ "/keys" ++ convert_to_string(Key),
  Result = post_request(put, FullUrl, [{value, Value}, {PrevWhat, Prev}, {ttl, TTL}], Timeout),
  handle_request_result(Result).

%% @spec (Url, Key, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   Timeout = pos_integer() | infinity
%%   Result = {ok, response() | [response()]} | {error, atom()}.
%% @end
-spec get(url(), key(), pos_timeout()) -> result().
get(Url, Key, Timeout) ->
  FullUrl = url_prefix(Url) ++ "/keys" ++ convert_to_string(Key),
  Result = ibrowse:send_req(FullUrl, [], get, "", [], Timeout),
  handle_request_result(Result).

%% @spec (Url, Key, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   Timeout = pos_integer() | infinity
%%   Result = {ok, response() | [response()]} | {error, atom()}.
%% @end
-spec delete(url(), key(), pos_timeout()) -> result().
delete(Url, Key, Timeout) ->
  FullUrl = url_prefix(Url) ++ "/keys" ++ convert_to_string(Key),
  Result = ibrowse:send_req(FullUrl, [], delete, "", [], Timeout),
  handle_request_result(Result).

%% @spec (Url, Key, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   Timeout = pos_integer() | infinity
%%   Result = {ok, response() | [response()]} | {error, atom()}.
%% @end
-spec watch(url(), key(), pos_timeout()) -> result().
watch(Url, Key, Timeout) ->
  Params = encode_params([{wait, true}]),
  FullUrl = url_prefix(Url) ++ "/keys" ++ convert_to_string(Key) ++ "?" ++ Params,
  Result = ibrowse:send_req(FullUrl, [], get, "", [], Timeout),
  handle_request_result(Result).

%% @spec (Url, Key, Index, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   Index = pos_integer()
%%   Timeout = pos_integer() | infinity
%%   Result = {ok, response() | [response()]} | {error, atom()}.
%% @end
-spec watch(url(), key(), pos_integer(), pos_timeout()) -> result().
watch(Url, Key, Index, Timeout) when is_integer(Index) ->
  Params = encode_params([{wait, true}, {waitIndex, Index}]),
  FullUrl = url_prefix(Url) ++ "/keys" ++ convert_to_string(Key) ++ "?" ++ Params,
  Result = ibrowse:send_req(FullUrl, [], get, "", [], Timeout),
  handle_request_result(Result).


%% @private
convert_to_string(Value) when is_integer(Value) ->
  integer_to_list(Value);
convert_to_string(Value) when is_binary(Value) ->
  binary_to_list(Value);
convert_to_string(Value) when is_atom(Value) ->
  atom_to_list(Value);
convert_to_string(Value) when is_list(Value) ->
  Value.


%% @private
encode_params(Pairs) ->
  List = [ http_uri:encode(convert_to_string(Key)) ++ "=" ++ http_uri:encode(convert_to_string(Value)) || {Key, Value} <- Pairs ],
  string:join(List, "&").


%% @private
url_prefix(Url) ->
  Url ++ "/v2".

%% @private
post_request(Method, Url, Pairs, Timeout) when Method == post ; Method == put ->
  Body = encode_params(Pairs),
  Headers = [{"Content-Type", "application/x-www-form-urlencoded"}],
  ibrowse:send_req(Url, Headers, Method, Body, [], Timeout).

%% @private
parse_response(Decoded) -> %% TODO: return {ok, {ID, Meta, Value}} and {error, not_found} cases?
  Decoded.

%% @private
handle_request_result(Result) ->
  case Result of
    {ok, StatusCode, _Headers, ResponseBody} when (StatusCode == "200" orelse StatusCode == "201") andalso is_list(ResponseBody) ->
      Decoded = decode_json(ResponseBody),
      {ok, parse_response(Decoded)};
    {ok, StatusCode, Headers, ResponseBody} ->
      case is_json_response(Headers) of
        true ->
          {error, {invalid_status, {StatusCode, Headers, decode_json(ResponseBody)}}};
        false ->
          {error, {invalid_status, {StatusCode, Headers, ResponseBody}}}
      end;
    {error, Reason} ->
      {error, Reason}
  end.


%% @private
decode_json(Json) when is_list(Json) ->
  jsx:decode(list_to_binary(Json), [return_maps, {labels, atom}]).


%% @private
is_json_response(Headers) ->
  proplists:get_value("content-type", to_lower(Headers)) == "application/json".

%% @private
to_lower(Props) ->
  lists:map(fun({K,V}) -> {string:to_lower(K), string:to_lower(V)} end, Props).