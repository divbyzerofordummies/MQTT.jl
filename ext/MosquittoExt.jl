"""
[Mosquitto.jl](https://github.com/denglerchr/Mosquitto.jl) is an interface to the C and C++ library [Mosquitto](https://mosquitto.org/).

This extension should make handling the loops easier, i.e., MQTT.jl - style
"""
module MosquittoExt

using Mosquitto
using MQTT: MQTT

import Distributed

function mosquittoQOS(qos::MQTT.QOS)
    # TODO: Check if the meaning is the same
    qos_mosquitto = if qos == MQTT.AT_MOST_ONCE
        0
    elseif qos == MQTT.AT_LEAST_ONCE
        1
    elseif qos == MQTT.EXACTLY_ONCE
        2
    else
        error("Unknown QOS: $qos")
    end
    return qos_mosquitto
end

"""
Asynchronous functions always wait for a `Distributed.Future` so `_resolve` can work with the results.
Mosquitto does not work that way, everything is synchronous
"""
function encapsulateInFuture(value)
    future = Distributed.Future()
    put!(future, value)
    return future
end

@kwdef struct MosquittoConnectionConfig
    host::String
    port::Int
    username::String=""
    password::String=""
    keepalive::Int=60
    certfile_server::String="" # Path to server certificate file
    certfile_client::String="" # Path to server certificate file
    keyfile_client::String="" # Path to server certificate file
end

"""
    findmatch(topic::AbstractString, topics::AbstractVector{String})

Finds a match for a topic of a message in a list of topics which an contain wildcards.

Topics are separated in levels "/"; wildcards are: '+' for all elements in a level, '#' for any levels afterwards.

Returns `nothing` if no match is found, or the name of the topic with wildcards.

```julia
topics = ["v1/devices/me/rpc/request/+"]
topic = "v1/devices/me/rpc/request/23"
@test findmatch(topic, topics) == "v1/devices/me/rpc/request/+"
````
"""
function findmatch(topic::AbstractString, topics::AbstractVector{String})
    levels = split(topic, '/')
    for sub_topic in topics
        # number of levels in "topics" can only be smaller than number of levels in "topic" (the specific one)
        sub_levels = split(sub_topic, '/')
        length(sub_levels) > length(levels) && continue
        matches = true
        for (i_level, topic_level) in enumerate(levels)
            i_level > length(sub_levels) && continue
            sub_levels[i_level] == "#" && return sub_topic # we have reached a complete wildcard
            sub_levels[i_level] == "+" && continue # matches everything
            if topic_level != sub_levels[i_level] 
                matches = false
                break
            end
        end
        matches && (return sub_topic)
    end
    return nothing
end

"""
    MosquittoClientConfig

Structure containing all information for encapsulating the conncetion to the Mosquitto library

# Fields

  - `connection_config::MosquittoConnectionConfig`: Configuration required to establish a connection
  - `pub_inflight::Dict{Cint, Distributed.Future}`: Dict from message ID to future somebody is waiting on when publishing synchronously
  - `connect_callback::Union{Nothing,Distributed.Future}`: Future that should be set once the CONNACK message was received from the broker
  - `callbacks::Dict{String, OnMessage}`: Dict from topic one has subscribed to to callback that should be called with the data
  - `loop_task::Union{Nothing, Task}`: Task storing the loop that checks for new subscriptions
  - `is_running::Ref{Bool}`: Reference shared with the task to stop it when client is disconnected
"""
mutable struct MosquittoClientConfig <: MQTT.AbstractConnection
    client::Mosquitto.Client
    connection_config::MosquittoConnectionConfig
    pub_inflight::Dict{Cint,Distributed.Future}
    connect_callback::Union{Nothing,Distributed.Future}
    disconnect_callback::Union{Nothing,Distributed.Future}
    callbacks::Dict{String, MQTT.OnMessage}
    loop_task::Union{Nothing,Task}
    is_running::Ref{Bool}
    is_loop_running::Bool
end
function MosquittoClientConfig(client::Mosquitto.Client; kwargs...)
    return MosquittoClientConfig(
        client, 
        MosquittoConnectionConfig(; kwargs...), 
        Dict{Cint,Distributed.Future}(), # pub_inflight
        nothing, # connect_callback
        nothing, # disconnect_callback
        Dict{String, MQTT.OnMessage}(), # callbacks
        nothing, # loop_task
        Ref{Bool}(false), # is_running
        false, # is_loop_running
    )
end
function MosquittoClientConfig(; kwargs...)
    client = Mosquitto.Client()
    return MosquittoClientConfig(client; kwargs...)
end

"""
    MQTT.MQTTConnection(client::Mosquitto.Client; kwargs...)

Create an MosquittoClientConfig object (an `AbstractConnection` for MQTT using `Mosquitto.jl` as a backend).

# Keywords

  - `host::String`: Hostname or IP address of the broker
  - `port::Int`: Port. Typically 1883 for non-encrypted and 8883 for encrypted communication
  - `username::String`: Username for username / password login. Default: ""
  - `password::String`: Password. Default: ""
  - `certfile_server::String`: Path to a CA certificate of the server for TLS/SSL connection (*.crt or *.pem, in PEM format)
  - `certfile_client::String`: Path to a CA certificate of the client for TLS/SSL connection (*.crt or *.pem, in PEM format). Must also provide `keyfile_client` if this is provided.
  - `keyfile_client::String`: Path to a CA key file of the client for TLS/SSL connection. Must also provide `certfile_client` if this is provided.
  - `keepalive::Int`: Time in seconds before connection to broker is disconnected. Default: 60.
"""
function MQTT.MQTTConnection(client::Mosquitto.Client; kwargs...)
    return MosquittoClientConfig(client; kwargs...)
end

function MQTT._resolve(future::Distributed.Future)
    return resolve(future)
end

function MQTT._connect(c::MosquittoClientConfig) # future contains a Mosuitto.MosquittoCwrapper.mosq_err_t
    if !isempty(c.connection_config.certfile_server)
        ret = if !isempty(c.connection_config.certfile_client)
            Mosquitto.tls_set(c.client, c.connection_config.certfile_server; certfile=c.connection_config.certfile_client, keyfile=c.connection_config.keyfile_client)
        else    
            Mosquitto.tls_set(c.client, c.connection_config.certfile_server)
        end
        if ret != Mosquitto.MosquittoCwrapper.MOSQ_ERR_SUCCESS
            error("Error while trying to establish encrypted communication: $ret")
        end
    end
    flag = Mosquitto.connect(
        c.client, c.connection_config.host, c.connection_config.port; 
        username=c.connection_config.username, 
        password=c.connection_config.password, 
        keepalive=c.connection_config.keepalive,
    )

    if flag == Mosquitto.MosquittoCwrapper.MOSQ_ERR_SUCCESS
        c.is_loop_running && @error("MQTT loop is already running. Something must have gone wrong.")
        c.pub_inflight = Dict{Cint, Distributed.Future}()
        c.callbacks = Dict{String, MQTT.OnMessage}() # get rid of invalid callbacks
        c.is_running[] = true # must be set before we spawn the thread
        c.connect_callback = Distributed.Future() # will be called in the loop if CONNACK is received
        c.disconnect_callback = nothing
        c.loop_task = Threads.@spawn loop(c)
        return c.connect_callback
    end

    return encapsulateInFuture(flag)
end

function MQTT._subscribe(callback::MQTT.OnMessage, c::MosquittoClientConfig, topic::AbstractString, qos::MQTT.QOS) # returns a tuple {Mosquitto.MosquittoCwrapper.mosq_err_t, Cint}
    # Sometimes we listen to a general topic "topic/+" --> restrict it to "topic"
    rv, message_id = Mosquitto.subscribe(c.client, String(topic); qos=mosquittoQOS(qos))
    if rv == Mosquitto.MosquittoCwrapper.MOSQ_ERR_SUCCESS
        c.callbacks[topic] = callback
    end
    return encapsulateInFuture((rv, message_id))
end

function MQTT._publish(c::MosquittoClientConfig, topic::AbstractString, payload, qos::MQTT.QOS, retain) # returns a tuple {Mosquitto.MosquittoCwrapper.mosq_err_t, Cint}
    rv, message_id = Mosquitto.publish(c.client, String(topic), payload; qos=mosquittoQOS(qos), retain)

    # One can wait for the result if one is interested; will be put on the get_pub_channel(client)
    if rv == Mosquitto.MosquittoCwrapper.MOSQ_ERR_SUCCESS
        # @info "Adding inflight future waiting for message ID $message_id"
        # future = Distributed.Future()
        # c.pub_inflight[message_id] = future
        future = encapsulateInFuture((rv, message_id))
        return future
    end

    return encapsulateInFuture((rv, message_id))
end

function MQTT._unsubscribe(c::MosquittoClientConfig, topic::AbstractString)
    rv, message_id = Mosquitto.unsubscribe(c.client, topic)
    return encapsulateInFuture((rv, message_id))
end

function MQTT._disconnect(c::MosquittoClientConfig)
    flag = Mosquitto.disconnect(c.client)
    c.is_running[] = false

    if flag == Mosquitto.MosquittoCwrapper.MOSQ_ERR_SUCCESS && c.is_loop_running
        # Return a future that will be called from the main loop as soon as it stops
        c.disconnect_callback = Distributed.Future()
        return c.disconnect_callback
    end

    return encapsulateInFuture(flag)
end

function loop(c::MosquittoClientConfig)
    @info "MQTT.MosquittoExt.loop: Starting Mosquitto loop"
    c.is_loop_running = true

    while c.is_running[]
        # @info "Running mosquitto loop..."
        Mosquitto.loop(c.client; timeout=500, ntimes=10, autoreconnect=true)

        pub_channel = Mosquitto.get_pub_channel(c.client)
        msg_channel = Mosquitto.get_messages_channel(c.client)
        connect_channel = Mosquitto.get_connect_channel(c.client)

        # Check if publishing has finished --> somehow this never reacted :-(
        while isready(connect_channel)
            connect_cb = take!(connect_channel) # a Mosquitto.ConnectionCB object
            @info "MQTT.MosquittoExt.loop: Received connect message with content: $connect_cb"
            if connect_cb.val == 1 && !isnothing(c.connect_callback) # 0 on disconnect, 1 on connect
                put!(c.connect_callback, connect_cb.returncode)
                c.connect_callback = nothing
            end
            if connect_cb.val == 0 && !isnothing(c.disconnect_callback)
                put!(c.disconnect_callback, connect_cb.returncode)
                c.disconnect_callback = nothing
            end
        end
            
        # Check if publishing has finished --> somehow this never reacted :-(
        while isready(pub_channel)
            message_id = take!(pub_channel)
            # @info "Received message with ID $message_id"
            haskey(c.pub_inflight, message_id) || continue
            # @info "Putting on future for message with ID $message_id"
            put!(c.pub_inflight[message_id], (Mosquitto.MosquittoCwrapper.MOSQ_ERR_SUCCESS, message_id)) # same message as would have been returned directly
            delete!(c.pub_inflight, message_id)
        end

        while isready(msg_channel)
            message = take!(msg_channel) # a Mosquitto.MessageCB object
            # for searching, take the more general topic without the number
            topic_match = findmatch(message.topic, collect(keys(c.callbacks)))
            if isnothing(topic_match) || !haskey(c.callbacks, topic_match)
                @info "Received message for topic $(message.topic), but found no available match"
                continue
            end
            @info "Received message for topic $(message.topic), matching with $topic_match"
            @invokelatest c.callbacks[topic_match](message.topic, message.payload) # not sure why invokelatest is necessary??? Gives me "The applicable method may be too new: running in world age 31547, while current world is 31548" without
        end

        sleep(0.25)
    end

    c.is_loop_running = false
    # If the connection was lost before, the disconnect callback is not called
    if !isnothing(c.disconnect_callback)
        put!(c.disconnect_callback, Mosquitto.MosquittoCwrapper.MOSQ_ERR_CONN_LOST)
        c.disconnect_callback = nothing
    end
    @info "MQTT.MosquittoExt.loop: Stopped Mosquitto loop"
    return nothing
end

"""
    resolve(future::Distributed.Future)

Fetch the result of a `Future` object and return it. If the result is an exception, throw the exception, otherwise return the result.

# Arguments

  - `future`: The `Future` object to fetch the result from.

# Returns

  - The result of the `Future`, or throws an exception if the result is an exception.
"""
function resolve(future::Distributed.Future)
    r = fetch(future)
    return (typeof(r) <: Exception) ? throw(r) : r
end

end # MosquittoExt