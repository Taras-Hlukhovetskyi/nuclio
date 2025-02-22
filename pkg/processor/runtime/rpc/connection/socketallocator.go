/*
Copyright 2024 The Nuclio Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package connection

import (
	"time"

	"github.com/nuclio/errors"
)

const (
	socketPathTemplate = "/tmp/nuclio-rpc-%s.sock"
	connectionTimeout  = 2 * time.Minute
)

type SocketAllocator struct {
	*AbstractConnectionManager

	eventSockets []*EventSocket
}

func NewSocketAllocator(abstractConnectionManager *AbstractConnectionManager) *SocketAllocator {
	return &SocketAllocator{
		AbstractConnectionManager: abstractConnectionManager,
		eventSockets:              make([]*EventSocket, 0),
	}
}

// Prepare initializes the SocketAllocator by setting up control and event sockets
// according to the configuration.
//
// If SupportControlCommunication is enabled, a control communication socket is created,
// wrapped in a ControlMessageSocket, and integrated with the ControlMessageBroker for runtime operations.
//
// Creates a minimum number of event sockets (MinConnectionsNum).
func (sa *SocketAllocator) Prepare() error {
	if err := sa.prepareControlMessageSocket(); err != nil {
		return errors.Wrap(err, "Failed to prepare control message socket")
	}
	for i := 0; i < sa.MinConnectionsNum; i++ {
		eventConnection, err := sa.createSocketConnection()
		if err != nil {
			return errors.Wrap(err, "Failed to create event socket connection")
		}
		sa.eventSockets = append(sa.eventSockets,
			NewEventSocket(sa.Logger, eventConnection, sa))
	}
	return nil
}

func (sa *SocketAllocator) Start() error {
	if err := sa.startSockets(); err != nil {
		return errors.Wrap(err, "Failed to start socket allocator")
	}

	// wait for start if required to
	if sa.Configuration.WaitForStart {
		sa.Logger.Debug("Waiting for start")
		for _, socket := range sa.eventSockets {
			socket.WaitForStart()
		}
	}

	sa.Logger.Debug("Socker allocator started")
	return nil
}

func (sa *SocketAllocator) Stop() error {
	for _, eventSocket := range sa.eventSockets {
		socket := eventSocket
		go func() {
			socket.Stop()
		}()
	}
	sa.stopControlMessageSocket()
	return nil
}

func (sa *SocketAllocator) Allocate() (EventConnection, error) {
	// TODO: implement allocation logic when support multiple sockets
	return sa.eventSockets[0], nil
}

func (sa *SocketAllocator) GetAddressesForWrapperStart() ([]string, string) {
	eventAddresses := make([]string, 0)

	for _, socket := range sa.eventSockets {
		eventAddresses = append(eventAddresses, socket.Address)
	}

	controlAddress := ""
	if sa.controlMessageSocket != nil {
		controlAddress = sa.controlMessageSocket.Address
	}
	sa.Logger.DebugWith("Got socket addresses",
		"eventAddresses", eventAddresses,
		"controlAddress", controlAddress)
	return eventAddresses, controlAddress
}

func (sa *SocketAllocator) startSockets() error {
	var err error
	for _, socket := range sa.eventSockets {
		// TODO: when having multiple sockets supported, we might want to reconsider failing here
		if socket.Conn, err = socket.listener.Accept(); err != nil {
			return errors.Wrap(err, "Can't get connection from wrapper")
		}
		socket.SetEncoder(sa.Configuration.GetEventEncoderFunc(socket.Conn))
		go socket.AbstractEventConnection.RunHandler()
	}
	sa.Logger.Debug("Successfully established connection for event sockets")

	if err := sa.startControlMessageSocket(); err != nil {
		return errors.Wrap(err, "Failed to start control message socket")
	}
	return nil
}
