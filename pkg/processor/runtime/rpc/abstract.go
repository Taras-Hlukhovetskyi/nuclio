/*
Copyright 2023 The Nuclio Authors.

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

package rpc

import (
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/nuclio/nuclio/pkg/common"
	"github.com/nuclio/nuclio/pkg/common/status"
	"github.com/nuclio/nuclio/pkg/processor/runtime"
	"github.com/nuclio/nuclio/pkg/processor/runtime/rpc/connection"
	"github.com/nuclio/nuclio/pkg/processor/runtime/rpc/result"
	"github.com/nuclio/nuclio/pkg/processwaiter"

	"github.com/nuclio/errors"
	"github.com/nuclio/logger"
	"github.com/nuclio/nuclio-sdk-go"
)

// AbstractRuntime is a runtime that communicates via unix domain socket
type AbstractRuntime struct {
	runtime.AbstractRuntime
	configuration     *runtime.Configuration
	wrapperProcess    *os.Process
	runtime           Runtime
	stopChan          chan struct{}
	processWaiter     *processwaiter.ProcessWaiter
	connectionManager connection.ConnectionManager
}

// NewAbstractRuntime returns a new RPC runtime
func NewAbstractRuntime(logger logger.Logger,
	configuration *runtime.Configuration,
	runtimeInstance Runtime) (*AbstractRuntime, error) {
	var err error

	abstractRuntime, err := runtime.NewAbstractRuntime(logger, configuration)
	if err != nil {
		return nil, errors.Wrap(err, "Can't create AbstractRuntime")
	}

	newRuntime := &AbstractRuntime{
		AbstractRuntime: *abstractRuntime,
		configuration:   configuration,
		runtime:         runtimeInstance,
		stopChan:        make(chan struct{}, 1),
	}

	return newRuntime, nil
}

func (r *AbstractRuntime) Start() error {
	if err := r.startWrapper(); err != nil {
		r.SetStatus(status.Error)
		return errors.Wrap(err, "Failed to run wrapper")
	}

	r.SetStatus(status.Ready)
	return nil
}

// ProcessEvent processes an event
func (r *AbstractRuntime) ProcessEvent(event nuclio.Event, functionLogger logger.Logger) (interface{}, error) {
	processingResult, err := r.processItemAndWaitForResult(event, functionLogger)
	if err != nil {
		return nil, err
	}
	// this is a single event processing flow, so we only take the first item from the result
	return nuclio.Response{
		Body:        processingResult.Results[0].DecodedBody,
		ContentType: processingResult.Results[0].ContentType,
		Headers:     processingResult.Results[0].Headers,
		StatusCode:  processingResult.Results[0].StatusCode,
	}, processingResult.Results[0].Err
}

// ProcessBatch processes a batch of events
func (r *AbstractRuntime) ProcessBatch(batch []nuclio.Event, functionLogger logger.Logger) ([]*runtime.ResponseWithErrors, error) {
	processingResults, err := r.processItemAndWaitForResult(batch, functionLogger)
	if err != nil {
		return nil, err
	}
	responsesWithErrors := make([]*runtime.ResponseWithErrors, len(processingResults.Results))

	for index, processingResult := range processingResults.Results {
		if processingResult.EventId == "" {
			functionLogger.WarnWith("Received response with empty event_id, response won't be returned")
			continue
		}
		responsesWithErrors[index] = &runtime.ResponseWithErrors{
			Response: nuclio.Response{
				Body:        processingResult.DecodedBody,
				ContentType: processingResult.ContentType,
				Headers:     processingResult.Headers,
				StatusCode:  processingResult.StatusCode,
			},
			EventId:      processingResult.EventId,
			ProcessError: processingResult.Err,
		}
	}

	return responsesWithErrors, nil
}

// Stop stops the runtime
func (r *AbstractRuntime) Stop() error {
	r.Logger.WarnWith("Stopping",
		"status", r.GetStatus(),
		"wrapperProcess", r.wrapperProcess)

	// move to `stopped` state before actually stopping it
	// to avoid sending any events while stopping
	r.SetStatus(status.Stopped)

	if err := r.connectionManager.Stop(); err != nil {
		return errors.Wrap(err, "Failed to stop connection manager")
	}

	if r.wrapperProcess != nil {

		// stop waiting for process
		if err := r.processWaiter.Cancel(); err != nil {
			r.Logger.WarnWith("Failed to cancel process waiting")
		}

		r.Logger.WarnWith("Killing wrapper process", "wrapperProcessPid", r.wrapperProcess.Pid)
		if err := r.wrapperProcess.Kill(); err != nil {
			r.SetStatus(status.Error)
			return errors.Wrap(err, "Can't kill wrapper process")
		}
	}

	r.WaitForProcessTermination(10 * time.Second)

	r.wrapperProcess = nil
	r.Logger.Warn("Successfully stopped wrapper process")
	return nil
}

// Restart restarts the runtime
func (r *AbstractRuntime) Restart() error {
	if err := r.Stop(); err != nil {
		return err
	}

	if err := r.startWrapper(); err != nil {
		r.SetStatus(status.Error)
		return errors.Wrap(err, "Can't start wrapper process")
	}

	r.SetStatus(status.Ready)
	return nil
}

// GetSocketType returns the type of socket the runtime works with (unix/tcp)
func (r *AbstractRuntime) GetSocketType() connection.SocketType {
	return connection.UnixSocket
}

// WaitForStart returns whether the runtime supports sending an indication that it started
func (r *AbstractRuntime) WaitForStart() bool {
	return false
}

// SupportsRestart returns true if the runtime supports restart
func (r *AbstractRuntime) SupportsRestart() bool {
	return true
}

// SupportsControlCommunication returns true if the runtime supports control communication
func (r *AbstractRuntime) SupportsControlCommunication() bool {
	return false
}

func (r *AbstractRuntime) Signal(signal syscall.Signal) error {

	if r.wrapperProcess != nil {
		r.Logger.DebugWith("Signaling wrapper process",
			"pid", r.wrapperProcess.Pid,
			"signal", signal.String())

		if err := r.wrapperProcess.Signal(signal); err != nil {
			r.Logger.WarnWith("Failed to signal wrapper process",
				"pid", r.wrapperProcess.Pid,
				"signal", signal.String())
		}
	} else {
		r.Logger.DebugWith("No wrapper process exists, skipping signal")
	}

	return nil
}

// WaitForProcessTermination will best effort wait few seconds to stop channel, if timeout - assume closed
func (r *AbstractRuntime) WaitForProcessTermination(timeout time.Duration) {
	r.Logger.DebugWith("Waiting for process termination",
		"wid", r.Context.WorkerID,
		"process", r.wrapperProcess,
		"timeout", timeout.String())

	for {
		select {
		case <-r.stopChan:
			r.Logger.DebugWith("Process terminated",
				"wid", r.Context.WorkerID,
				"process", r.wrapperProcess)
			return
		case <-time.After(timeout):
			r.Logger.DebugWith("Timeout waiting for process termination, assuming closed",
				"wid", r.Context.WorkerID,
				"process", r.wrapperProcess)
			return
		}
	}
}

func (r *AbstractRuntime) processItemAndWaitForResult(item interface{}, functionLogger logger.Logger) (*result.BatchedResults, error) {

	if currentStatus := r.GetStatus(); currentStatus != status.Ready {
		return nil, errors.Errorf("Processor not ready (current status: %s)", currentStatus)
	}

	connectionInstance, err := r.connectionManager.Allocate()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to allocate connection")
	}
	processingResult, err := connectionInstance.ProcessEvent(item, functionLogger)

	return processingResult, err
}

func (r *AbstractRuntime) startWrapper() error {
	connectionManagerConfiguration := connection.NewManagerConfigration(
		r.runtime.SupportsControlCommunication(),
		r.runtime.WaitForStart(),
		r.runtime.GetSocketType(),
		r.runtime.GetEventEncoder,
		r.Statistics,
		r.configuration.WorkerID,
		r.configuration.Mode,
	)

	var err error
	r.connectionManager, err = connection.NewConnectionManager(r.Logger, *r.configuration, connectionManagerConfiguration)
	if err != nil {
		return errors.Wrap(err, "Failed to create connection manager")
	}
	if err = r.connectionManager.Prepare(); err != nil {
		return errors.Wrap(err, "Failed to prepare connections")
	}

	if r.processWaiter, err = processwaiter.NewProcessWaiter(); err != nil {
		return errors.Wrap(err, "Failed to create process waiter")
	}

	wrapperProcess, err := r.runtime.RunWrapper(r.connectionManager.GetAddressesForWrapperStart())
	if err != nil {
		return errors.Wrap(err, "Can't run wrapper")
	}

	r.wrapperProcess = wrapperProcess

	go r.watchWrapperProcess()

	if err := r.connectionManager.Start(); err != nil {
		return errors.Wrap(err, "Failed to start connection manager")
	}

	return nil
}

func (r *AbstractRuntime) watchWrapperProcess() {

	// whatever happens, clear wrapper process
	defer func() {
		r.stopChan <- struct{}{}
	}()

	// wait for the process
	processWaitResult := <-r.processWaiter.Wait(r.wrapperProcess, nil)

	// if we were simply canceled, do nothing
	if processWaitResult.Err == processwaiter.ErrCancelled {
		r.Logger.DebugWith("Process watch cancelled. Returning",
			"pid", r.wrapperProcess.Pid,
			"wid", r.Context.WorkerID)
		return
	}

	// if process exited gracefully (i.e. wasn't force killed), do nothing
	if processWaitResult.Err == nil && processWaitResult.ProcessState.Success() {
		r.Logger.DebugWith("Process watch done - process exited successfully")
		return
	}

	var errorMsg string
	if processWaitResult.Err != nil {
		errorMsg = processWaitResult.Err.Error()
	}

	r.Logger.ErrorWith(string(common.UnexpectedTerminationChildProcess),
		"error", errorMsg,
		"status", processWaitResult.ProcessState.String(),
		"exitCode", processWaitResult.ProcessState.ExitCode(),
		"pid", r.wrapperProcess.Pid,
	)

	var panicMessage string
	if processWaitResult.Err != nil {
		panicMessage = processWaitResult.Err.Error()
	} else {
		panicMessage = processWaitResult.ProcessState.String()
	}

	panic(fmt.Sprintf("Wrapper process for worker %d exited unexpectedly with: %s", r.Context.WorkerID, panicMessage))
}
