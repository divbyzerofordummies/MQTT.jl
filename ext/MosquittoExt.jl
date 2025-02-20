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
  - `callbacks::Dict{String, OnMessage}`: Dict from topic one has subscribed to to callback that should be called with the data
  - `loop_task::Union{Nothing, Task}`: Task storing the loop that checks for new subscriptions
  - `is_running::Ref{Bool}`: Reference shared with the task to stop it when client is disconnected
"""
mutable struct MosquittoClientConfig <: MQTT.AbstractConnection
    client::Mosquitto.Client
    connection_config::MosquittoConnectionConfig
    pub_inflight::Dict{Cint,Distributed.Future}
    callbacks::Dict{String, MQTT.OnMessage}
    loop_task::Union{Nothing,Task}
    is_running::Ref{Bool}
end
function MosquittoClientConfig(; kwargs...)
    client = Mosquitto.Client()
    return MosquittoClientConfig(client, MosquittoConnectionConfig(; kwargs...), Dict{Cint,Distributed.Future}(), Dict{String, MQTT.OnMessage}(), nothing, Ref{Bool}(false))
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
    return MosquittoClientConfig(client, MosquittoConnectionConfig(; kwargs...), Dict{Cint,Distributed.Future}(), Dict{String, MQTT.OnMessage}(), nothing, Ref{Bool}(false))
end

function MQTT._resolve(future::Distributed.Future)
    return resolve(future)
end

function MQTT._connect(c::MosquittoClientConfig)
    if !isempty(c.connection_config.certfile_server)
        ret = if !isempty(c.connection_config.certfile_client)
            Mosquitto.tls_set(c.client, c.connection_config.certfile_server; certfile=c.connection_config.certfile_client, keyfile=c.connection_config.keyfile_client)
        else    
            Mosquitto.tls_set(c.client, c.connection_config.certfile_server)
        end
        if ret != Mosquitto.MosquittoCwrapper.MOSQ_ERR_SUCCESS
            error("Error while trying to establish encrypted communication: $ret")
        else
            @info "Successfully set TLS"
        end
    end
    flag = Mosquitto.connect(
        c.client, c.connection_config.host, c.connection_config.port; 
        username=c.connection_config.username, 
        password=c.connection_config.password, 
        keepalive=c.connection_config.keepalive,
    )

    if flag == Mosquitto.MosquittoCwrapper.MOSQ_ERR_SUCCESS
        @info "Spawning the loop task"
        c.pub_inflight = Dict{Cint, Distributed.Future}()
        c.callbacks = Dict{String, MQTT.OnMessage}() # get rid of invalid callbacks
        c.is_running[] = true # must be set before we spawn the thread
        c.loop_task = Threads.@spawn loop(c)
    end
    # TODO: Is there a method in Mosquitto to wait until the CONNACK was received?
    future = encapsulateInFuture(flag)

    # TODO: Start the loop asynchronously
    return future
end

function MQTT._subscribe(callback::MQTT.OnMessage, c::MosquittoClientConfig, topic::AbstractString, qos::MQTT.QOS)
    # Sometimes we listen to a general topic "topic/+" --> restrict it to "topic"
    rv, message_id = Mosquitto.subscribe(c.client, String(topic); qos=mosquittoQOS(qos))
    if rv == Mosquitto.MosquittoCwrapper.MOSQ_ERR_SUCCESS
        c.callbacks[topic] = callback
    end
    return encapsulateInFuture((rv, message_id))
end

function MQTT._publish(c::MosquittoClientConfig, topic::AbstractString, payload, qos::MQTT.QOS, retain)
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
    # TODO: future does not really work well with actually stopping everything --> should contain wait(task)
    return encapsulateInFuture(flag)
end

function loop(c::MosquittoClientConfig)
    @info "Starting Mosquitto loop"

    while c.is_running[]
        # @info "Running mosquitto loop..."
        Mosquitto.loop(c.client; timeout=500, ntimes=10, autoreconnect=true)

        pub_channel = Mosquitto.get_pub_channel(c.client)
        msg_channel = Mosquitto.get_messages_channel(c.client)
        connect_channel = Mosquitto.get_connect_channel(c.client)

        # Check if publishing has finished --> somehow this never reacted :-(
        while isready(pub_channel)
            message_id = take!(pub_channel)
            @info "Received message with ID $message_id"
            haskey(c.pub_inflight, message_id) || continue
            @info "Putting on future for message with ID $message_id"
            put!(c.pub_inflight[message_id], (Mosquitto.MosquittoCwrapper.MOSQ_ERR_SUCCESS, message_id)) # same message as would have been returned directly
            delete!(c.pub_inflight, message_id)
        end

        while isready(msg_channel)
            message = take!(msg_channel) # a Mosquitto.MessageCB object
            # for searching, take the more general topic without the number
            topic_match = findmatch(message.topic, collect(keys(c.callbacks)))
            @info "Received message for topic $(message.topic)"
            if isnothing(topic_match) || !haskey(c.callbacks, topic_match)
                continue
            end
            @info "Calling callback for message $topic_match"
            @invokelatest c.callbacks[topic_match](message.topic, message.payload) # not sure why invokelatest is necessary??? Gives me "The applicable method may be too new: running in world age 31547, while current world is 31548" without
            @info "Callback finished"
        end

        sleep(0.5)
    end
    @info "Stopped Mosquitto loop task"
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