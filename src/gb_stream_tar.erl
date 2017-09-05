%%%===================================================================
%% @author Jonas Falkevik
%% @copyright 2017 Pundun Labs AB
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
%% implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%% -------------------------------------------------------------------
%% @title
%% @doc
%% Module Description: stream tar over tcp.
%% using open_port that might not be the best fit for flow control
%% @end
%%%===================================================================

-module(gb_stream_tar).

-export([start_receiver/2,
	 start_sender/3]).

-include_lib("gb_log/include/gb_log.hrl").

start_receiver(ReportTo, Dir) ->
    {ok, LS} = gen_tcp:listen(0, [{active, false}, binary]),
    Tar = open_port({spawn, "tar xz"}, [binary, eof, {cd, Dir}]),
    Pid = proc_lib:spawn_link(fun() -> 
		    rec_loop_s(#{report_to=>ReportTo, lsocket=>LS, tar=>Tar}) 
			end),
    {ok, Port} = inet:port(LS),
    {ok, Pid, Port}.

rec_loop_s(#{lsocket := LS} = State) ->
    {ok, S} = gen_tcp:accept(LS),
    rec_loop(State#{socket=>S, size=>0}).

rec_loop(#{status := done} = State) ->
    ?info("done. closing port."),
    port_close(maps:get(tar, State)),
    catch gen_tcp:close(maps:get(socket, State)),
    catch gen_tcp:close(maps:get(lsocket, State)),
    maps:get(report_to, State) ! {self(), done};
    
rec_loop(#{socket:=Socket} = State) ->
    case gen_tcp:recv(Socket, maps:get(size, State), 120000) of 
    	{ok, Data} ->
	    ?debug("known rem bytes: ~p; received size ~p", [maps:get(size, State), size(Data)]),
	    gen_tcp:send(maps:get(socket, State), "k"),
	    NewState = handle_rcv_data(State, Data),
	    rec_loop(NewState);
	{error, closed} ->
	    port_close(maps:get(tar, State)),
	    gen_tcp:close(maps:get(lsocket, State)),
	    maps:get(report_to, State) ! {self(), {error, closed}};
	{error, timeout} ->
	    maps:get(report_to, State) ! {self(), {error, timeout}},
	    {error, timeout}
    end.

handle_rcv_data(State = #{size:=0}, <<0:32>>) ->
    State#{size=>0, status=>done};
handle_rcv_data(State = #{size:=0}, <<Size:32, Data/binary>>) ->
    ?debug("size of incoming data: ~p", [Size]),
    port_command(maps:get(tar, State), Data),
    State#{size=>Size - size(Data)};
handle_rcv_data(State = #{size:=Size}, Data) ->
    port_command(maps:get(tar, State), Data),
    State#{size=>Size - size(Data)}. 

start_sender(Dir, Host, Port) ->
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary]),
    Tar = open_port({spawn, "tar cz ."}, [binary, eof, in, {cd, Dir}]),
    send_loop(#{socket=>Socket, tar=>Tar}).

send_loop(#{tar:=Tar, socket:=Socket} = State) ->
    receive 
	{Tar, {data, Data}} ->
	    gen_tcp:send(Socket, <<(size(Data)):32, Data/binary>>),
	    send_loop(State);
	{Tar, eof} ->
	    ?debug("eof sending 0 size"),
	    gen_tcp:send(Socket, <<0:32>>),
	    port_close(Tar),
	    close_loop(State);
	{tcp, _, Data} ->
	    ?debug("received ~p", [Data]),
	    send_loop(State);
	{tcp_closed, Socket} ->
	    ?error("premature close from peer"),
	    {error, premature_close}
    end.

close_loop(State) ->
    receive
	{tcp, _, Data} ->
	    ?debug("received ~p~n", [Data]),
	    close_loop(State);
	{tcp_closed, _} ->
	    catch gen_tcp:close(maps:get(socket, State)),
	    done
    after
	120000 ->
	    ?error("timed out"),
	    {error, timeout}
    end.
