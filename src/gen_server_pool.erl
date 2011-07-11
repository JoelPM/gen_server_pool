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
%%% File    : gen_server_pool.erl
%%% Author  : Joel Meyer <joel@openx.org>
%%% Description : 
%%%   A pool for gen_servers.
%%%
%%% Created :  4 May 2011
%%%-------------------------------------------------------------------
-module(gen_server_pool).

-behaviour(gen_server).

%% API
-export([ start_link/4,
          start_link/5,
          get_stats/1,
          available/2 ]).

%% gen_server callbacks
-export([ init/1,
          handle_call/3,
          handle_cast/2,
          handle_info/2,
          terminate/2,
          code_change/3 ]).

-record(state, { sup_pid,
                 sup_max_r,
                 sup_max_t = 1,
                 available = [],
                 requests  = { [], [] },
                 min_size  = 0,
                 max_size  = 10,
                 idle_secs = infinity,
                 module,
                 pool_id }).

%%====================================================================
%% API
%%====================================================================
start_link( Module, Args, Options, PoolOpts ) ->
  gen_server:start_link( ?MODULE,
                         [ Module, Args, Options, PoolOpts ],
                         Options ).


start_link( Name, Module, Args, Options, PoolOpts ) ->
  gen_server:start_link( Name,
                         ?MODULE,
                         [ Module, Args, Options, PoolOpts ],
                         Options ).

get_stats( MgrPid ) ->
  gen_server:call( MgrPid, { gen_server_pool, stats } ).


available( MgrPid, WorkerPid ) ->
  gen_server:cast( MgrPid, { gen_server_pool,
                             worker_available,
                             { WorkerPid, now() } } ).


%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init( [ Module, Args, Options, PoolOpts ]  ) ->
  % Init state from opts
  S = parse_opts( PoolOpts, #state{ module = Module } ),

  % Get config from state
  #state{ sup_max_r = MaxR, sup_max_t = MaxT } = S,

  % Start supervisor for pool members
  { ok, SupPid } = gen_server_pool_sup:start_link( self(),
                                                   Module, Args, Options,
                                                   MaxR, MaxT ),

  State = S#state{ sup_pid = SupPid },

  case assure_min_pool_size( State ) of
    ok ->
      schedule_idle_check( State ),
      { ok, State };
    Error ->
      { stop, Error }
  end.


%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call( gen_server_pool_stats, _From, State ) ->
  { reply, stats( State ), State };

handle_call( Call, From, State ) ->
  NState = handle_request( { '$gen_call', From, Call }, State ),
  { noreply, NState }.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast( { gen_server_pool, worker_available, PidTime }, State ) ->
  { noreply, worker_available( PidTime, State ) };

handle_cast( Cast, State ) ->
  { noreply, handle_request( { '$gen_cast', Cast }, State ) }.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info( { gen_server_pool, check_idle_timeouts }, State ) ->
  schedule_idle_check( State ),
  { noreply, check_idle_timeouts( State ) }; 

handle_info( Info, State ) ->
  { noreply, handle_request( Info, State ) }.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate( Reason, State ) ->
  terminate_pool( Reason, State ),
  ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change( _OldVsn, State, _Extra ) ->
  { ok, State }.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
handle_request( Req, State = #state{ requests = { Push, Pop } } ) ->
  do_work( State#state{ requests = { [ Req | Push ], Pop } } ).


worker_available( { Pid, Time }, State = #state{ available = Workers } ) ->
  % If a child sent a message to itself then it could already be in the list
  case proplists:is_defined( Pid, Workers ) of
    true  -> do_work( State );
    false -> do_work( State#state{ available = [ { Pid, Time } | Workers ] } )
  end.


stats( #state{ sup_pid   = SupPid,
               available = Workers,
               requests  = { Push, Pop } } ) ->
  Size   = proplists:get_value( active, supervisor:count_children( SupPid ) ),
  Idle   = length( Workers ),
  Active = Size - Idle,
  Tasks  = length( Push ) + length( Pop ),
  [ { size, Size }, { active, Active }, { idle, Idle }, { tasks, Tasks } ].



terminate_pool( _Reason, _State ) ->
  ok.


do_work( State = #state{ requests  = { [], [] } } ) ->
  % No requests - do nothing.
  State;

do_work( State = #state{ available = [],
                         requests  = { _Push, _Pop },
                         max_size  = MaxSize,
                         sup_pid   = SupPid } ) ->
  % Requests, but no workers - check if we can start a worker.
  PoolSize = proplists:get_value( active, supervisor:count_children( SupPid ) ),
  case PoolSize < MaxSize of
    true  -> gen_server_pool_sup:add_child( SupPid );
    false -> ok
  end,
  State;

do_work( State = #state{ requests  = { Push, [] } } ) ->
  do_work( State#state{ requests = { [], lists:reverse( Push ) } } );

do_work( State = #state{ available = [ { Pid, _ } | Workers ],
                         requests  = { Push, [ Req | Pop ] } } ) ->
  case is_process_alive(Pid) of
    false ->
      do_work( State#state{ available = Workers } );
    true  ->
      erlang:send( Pid, Req, [noconnect] ),
      State#state{ available = Workers,
                   requests  = { Push, Pop } }
  end.


assure_min_pool_size( #state{ min_size = MinSize, sup_pid = SupPid } = S ) ->
  PoolSize = proplists:get_value( active, supervisor:count_children( SupPid ) ),
  add_children( MinSize - PoolSize, S ).


add_children( N, #state{} ) when N =< 0 ->
  ok;
add_children( N, #state{ sup_pid = SupPid } = S ) ->
  case gen_server_pool_sup:add_child( SupPid ) of
    { error, Error } -> Error;
    _                -> add_children( N-1, S )
  end.


schedule_idle_check( #state{ idle_secs = infinity } = S ) ->
  S;
schedule_idle_check( #state{ idle_secs = IdleSecs } = S ) ->
  erlang:send_after( IdleSecs * 1000, self(), { gen_server_pool, check_idle_timeouts } ),
  S.


check_idle_timeouts( #state{ available = [] } = S ) ->
  S;
check_idle_timeouts( #state{ idle_secs = IdleSecs,
                             available = Available } = S ) ->
  Now = now(),
  F = fun( { Pid, Time }, Acc ) ->
        IdleTime = seconds_between( Now, Time ),
        case IdleTime >= IdleSecs of
          true ->
            gen_server_pool_proxy:stop( Pid ),
            Acc;
          false ->
            [ { Pid, Time } | Acc ]
        end
      end,

  State = S#state{ available = lists:foldl( F, [], Available ) },

  assure_min_pool_size( State ),

  State.


seconds_between( { MS1, S1, _ }, { MS1, S2, _ } ) ->
  S1 - S2;
seconds_between( { MS1, S1, _ }, { MS2, S2, _ } ) ->
  ( MS1 * 1000000 + S1 ) - ( MS2 * 1000000 + S2 ).


parse_opts( [], State ) ->
  finalize( State );
parse_opts( [ { pool_id, V } | Opts ], State ) when is_atom( V ) ->
  parse_opts( [ { pool_id, atom_to_list( V ) } | Opts ], State );
parse_opts( [ { pool_id, V } | Opts ], State ) ->
  parse_opts( Opts, State#state{ pool_id = V } );
parse_opts( [ { min_pool_size, V } | Opts ], State ) ->
  parse_opts( Opts, State#state{ min_size = V } );
parse_opts( [ { max_pool_size, V } | Opts ], State ) ->
  parse_opts( Opts, State#state{ max_size = V } );
parse_opts( [ { idle_timeout, V } | Opts ], State ) ->
  parse_opts( Opts, State#state{ idle_secs = V } );
parse_opts( [ { sup_max_r, V } | Opts ], State ) ->
  parse_opts( Opts, State#state{ sup_max_r = V } );
parse_opts( [ { sup_max_t, V } | Opts ], State ) ->
  parse_opts( Opts, State#state{ sup_max_t = V } ).


finalize( #state{ sup_max_r = undefined, max_size = Sz } = S ) ->
  % Set max_r to max pool size if not set
  finalize( S#state{ sup_max_r = Sz } );
finalize( State ) ->
  State.


%%--------------------------------------------------------------------
%%% Tests
%%--------------------------------------------------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.
