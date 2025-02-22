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

package runtime

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/nuclio/nuclio/pkg/functionconfig"
	"github.com/nuclio/nuclio/pkg/processor"
	"github.com/nuclio/nuclio/pkg/processor/controlcommunication"

	"github.com/nuclio/logger"
	"github.com/nuclio/nuclio-sdk-go"
)

type Statistics struct {
	DurationMilliSecondsSum   uint64
	DurationMilliSecondsCount uint64
}

func (s *Statistics) DiffFrom(prev *Statistics) Statistics {

	// atomically load the counters
	currDurationMilliSecondsSum := atomic.LoadUint64(&s.DurationMilliSecondsSum)
	currDurationMilliSecondsCount := atomic.LoadUint64(&s.DurationMilliSecondsCount)

	prevDurationMilliSecondsSum := atomic.LoadUint64(&prev.DurationMilliSecondsSum)
	prevDurationMilliSecondsCount := atomic.LoadUint64(&prev.DurationMilliSecondsCount)

	return Statistics{
		DurationMilliSecondsSum:   currDurationMilliSecondsSum - prevDurationMilliSecondsSum,
		DurationMilliSecondsCount: currDurationMilliSecondsCount - prevDurationMilliSecondsCount,
	}
}

type Configuration struct {
	*processor.Configuration
	FunctionLogger           logger.Logger
	WorkerID                 int
	TriggerName              string
	TriggerKind              string
	WorkerTerminationTimeout time.Duration
	ControlMessageBroker     *controlcommunication.AbstractControlMessageBroker
	Mode                     functionconfig.TriggerWorkMode
}

type ResponseWithErrors struct {
	nuclio.Response
	EventId         string
	SubmitError     error
	ProcessError    error
	NoResponseError error
}

var ErrNoResponseFromBatchResponse = errors.New("processor hasn't received corresponding response for the event")
