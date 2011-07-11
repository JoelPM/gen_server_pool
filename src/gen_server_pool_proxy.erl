%%  Copyright (c) 2011, OpenX
%%  All rights reserved.
%%  
%%  Redistribution and use in source and binary forms, with or without
%%  modification, are permitted provided that the following conditions are met:
%%      * Redistributions of source code must retain the above copyright
%%        notice, this list of conditions and the following disclaimer.
%%      * Redistributions in binary form must reproduce the above copyright
%%        notice, this list of conditions and the following disclaimer in the
%%        documentation and/or other materials provided with the distribution.
%%      * Neither the name of the <organization> nor the
%%        names of its contributors may be used to endorse or promote products
%%        derived from this software without specific prior written permission.
%%  
%%  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
%%  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
%%  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
%%  DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
%%  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
%%  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
%%  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
%%  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
%%  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
%%  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


%%%-------------------------------------------------------------------
%%% File    : gen_server_pool_proxy.erl
%%% Author  : Joel Meyer <joel@openx.org>
%%% Description : 
%%%   Serves as a proxy to gen_server.
%%%
%%% Created :  5 May 2011
%%%-------------------------------------------------------------------
-module(gen_server_pool_proxy).

-behaviour(gen_server).

%% API
-export([ start_link/4,
          stop/1 ]).

%% gen_server callbacks
-export([ init/1,
          handle_call/3,
          handle_cast/2,
          handle_info/2,
          terminate/2,
          code_change/3 ]).

-record( state, { manager_pid,
                  module,
                  state } ).

%%====================================================================
%% API
%%====================================================================
start_link( MgrPid, Module, Args, Options ) ->
  gen_server:start_link( ?MODULE, [ MgrPid, Module, Args ], Options ).

stop( Pid ) ->
  gen_server:call( Pid, { gen_server_pool_proxy, stop } ).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init( [ MgrPid, Module, Args ] ) ->
  PState = #state{ manager_pid = MgrPid,
                   module      = Module },

  case Module:init( Args ) of
    { ok, State } ->
      gen_server_pool:available( MgrPid, self() ),
      { ok, state( PState, State ) };
    { ok, State, Extra } ->
      gen_server_pool:available( MgrPid, self() ),
      { ok, state( PState, State ), Extra };
    Other ->
      Other
  end.


handle_call( { gen_server_pool_proxy, stop }, _From, PState ) ->
  { stop, normal, ok, PState };

handle_call( Msg,
             From,
             #state{ manager_pid = MgrPid,
                     module      = M,
                     state       = S } = PState ) ->
  case M:handle_call( Msg, From, S ) of
    { reply, Reply, NewS } ->
      gen_server_pool:available( MgrPid, self() ),
      { reply, Reply, state( PState, NewS ) };
    { reply, Reply, NewS, Extra } ->
      gen_server_pool:available( MgrPid, self() ),
      { reply, Reply, state( PState, NewS ), Extra };
    { noreply, NewS } ->
      gen_server_pool:available( MgrPid, self() ),
      { noreply, state( PState, NewS ) };
    { noreply, NewS, Extra } ->
      gen_server_pool:available( MgrPid, self() ),
      { noreply, state( PState, NewS ), Extra };
    { stop, Reason, Reply, NewS } ->
      { stop, Reason, Reply, state( PState, NewS ) };
    { stop, Reason, NewS } ->
      { stop, Reason, state( PState, NewS ) }
  end.


handle_cast( Msg,
             #state{ manager_pid = MgrPid,
                     module      = M,
                     state       = S } = PState ) ->
  case M:handle_cast( Msg, S ) of
    { noreply, NewS } ->
      gen_server_pool:available( MgrPid, self() ),
      { noreply, state( PState, NewS ) };
    { noreply, NewS, Extra } ->
      gen_server_pool:available( MgrPid, self() ),
      { noreply, state( PState, NewS ), Extra };
    { stop, Reason, NewS } ->
      { stop, Reason, state( PState, NewS ) }
  end.


handle_info( Msg,
             #state{ manager_pid = MgrPid,
                     module      = M,
                     state       = S } = PState ) ->
  case M:handle_info( Msg, S ) of
    { noreply, NewS } ->
      gen_server_pool:available( MgrPid, self() ),
      { noreply, state( PState, NewS ) };
    { noreply, NewS, Extra } ->
      gen_server_pool:available( MgrPid, self() ),
      { noreply, state( PState, NewS ), Extra };
    { stop, Reason, NewS } ->
      { stop, Reason, state( PState, NewS ) }
  end.


terminate( Reason,
           #state{ module = M,
                   state  = S } ) ->
  M:terminate( Reason, S ).


code_change( OldVsn,
             #state{ module = M, state = S } = PState,
             Extra ) ->
  { ok, NewS } = M:code_change( OldVsn, S, Extra ),
  { ok, state( PState, NewS ) }.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
state( ProxyState, State ) ->
  ProxyState#state{ state = State }.


%%--------------------------------------------------------------------
%%% Tests
%%--------------------------------------------------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.
