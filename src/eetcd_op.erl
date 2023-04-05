-module(eetcd_op).
-export([put/1, get/1, delete/1, txn/1]).

put({_, PutRequest}) ->
    #{request => {request_put, PutRequest}}.

get({_, RangeRequest}) ->
    #{request => {request_range, RangeRequest}}.

delete({_, DeleteRangeRequest}) ->
    #{request => {request_delete_range, DeleteRangeRequest}}.

txn({_, TxnRequest}) ->
    #{request => {request_txn, TxnRequest}}.
