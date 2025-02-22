//go:build test_integration && test_local

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

package httpsuite

import (
	"encoding/base64"
	"net/http"
	"path"
	"strconv"
	"strings"
	"testing"

	"github.com/nuclio/nuclio/pkg/common/headers"
	"github.com/nuclio/nuclio/pkg/functionconfig"
	"github.com/nuclio/nuclio/pkg/platform"

	"github.com/stretchr/testify/suite"
	"github.com/valyala/fasthttp"
)

type HTTPTestSuite struct {
	TestSuite
	triggerName string
}

func (suite *HTTPTestSuite) SetupTest() {
	suite.TestSuite.SetupTest()
	suite.triggerName = "testHTTP"
}

func (suite *HTTPTestSuite) TestCORS() {
	exposeHeaders := "x-nuclio-something, x-nuclio-somethingelse"
	allowHeaders := "Accept, Content-Length, Content-Type, X-nuclio-log-level"
	allowMethods := "OPTIONS, GET, POST, HEAD, PUT"
	preflightMaxAgeSeconds := 10
	origin := "foo.bar"
	createFunctionOptions := suite.getHTTPDeployOptions()
	createFunctionOptions.FunctionConfig.Spec.Triggers[suite.triggerName].Attributes["cors"] = map[string]interface{}{
		"enabled":                true,
		"allowOrigins":           []string{origin},
		"allowHeaders":           strings.Split(allowHeaders, ", "),
		"allowMethods":           strings.Split(allowMethods, ", "),
		"exposeHeaders":          strings.Split(exposeHeaders, ", "),
		"preflightMaxAgeSeconds": preflightMaxAgeSeconds,
	}
	validPreflightResponseStatusCode := fasthttp.StatusOK
	invalidPreflightResponseStatusCode := fasthttp.StatusBadRequest
	suite.DeployFunctionAndRequests(createFunctionOptions,
		[]*Request{

			// Happy flow
			{
				RequestMethod: http.MethodOptions,
				RequestHeaders: map[string]interface{}{
					"Origin":                         origin,
					"Access-Control-Request-Method":  http.MethodPost,
					"Access-Control-Request-Headers": headers.LogLevel,
				},
				ExpectedResponseStatusCode: &validPreflightResponseStatusCode,
				ExpectedResponseHeadersValues: map[string][]string{
					"Access-Control-Allow-Methods": {allowMethods},
					"Access-Control-Allow-Headers": {allowHeaders},
					"Access-Control-Allow-Origin":  {origin},
					"Access-Control-Max-Age":       {strconv.Itoa(preflightMaxAgeSeconds)},
				},
			},
			{
				RequestMethod: http.MethodPost,
				RequestHeaders: map[string]interface{}{
					"Origin": origin,
				},
				ExpectedResponseHeadersValues: map[string][]string{
					"Access-Control-Expose-Headers": {exposeHeaders},
				},
			},

			// Disallowed request method
			{
				RequestMethod: http.MethodOptions,
				RequestHeaders: map[string]interface{}{
					"Origin":                        origin,
					"Access-Control-Request-Method": "ABC",
				},
				ExpectedResponseStatusCode: &invalidPreflightResponseStatusCode,
			},

			// Disallowed origin
			{
				RequestMethod: http.MethodOptions,
				RequestHeaders: map[string]interface{}{
					"Origin":                        "dummy-origin",
					"Access-Control-Request-Method": http.MethodPost,
				},
				ExpectedResponseStatusCode: &invalidPreflightResponseStatusCode,
			},
		})
}

func (suite *HTTPTestSuite) TestMaxRequestBodySize() {
	createFunctionOptions := suite.getHTTPDeployOptions()
	maxRequestBodySize := 64
	trigger := createFunctionOptions.FunctionConfig.Spec.Triggers[suite.triggerName]
	trigger.Kind = "http"
	trigger.Attributes["maxRequestBodySize"] = maxRequestBodySize
	statusOK := fasthttp.StatusOK
	statusBadRequest := fasthttp.StatusBadRequest
	suite.DeployFunctionAndRequests(createFunctionOptions,
		[]*Request{
			// Happy flows
			{
				RequestMethod:              "POST",
				RequestBody:                string(make([]byte, maxRequestBodySize-1)),
				ExpectedResponseStatusCode: &statusOK,
			},
			{
				RequestMethod:              "POST",
				RequestBody:                string(make([]byte, maxRequestBodySize)),
				ExpectedResponseStatusCode: &statusOK,
			},

			// Bad flow
			{
				RequestMethod:              "POST",
				RequestBody:                string(make([]byte, maxRequestBodySize+1)),
				ExpectedResponseStatusCode: &statusBadRequest,
			},
		})
}

func (suite *HTTPTestSuite) TestProcessErrorResponse() {

	// create a function which returns a faulty content type in the response
	functionSourceCode := `import nuclio_sdk

def handler(context, event):
	return nuclio_sdk.Response(
		body=str(123),
		headers={},
		content_type=123,
		status_code=200,
	)
`
	createFunctionOptions := suite.getHTTPDeployOptions()
	createFunctionOptions.FunctionConfig.Spec.Handler = "main:handler"
	createFunctionOptions.FunctionConfig.Spec.Build.Path = ""
	createFunctionOptions.FunctionConfig.Spec.Build.FunctionSourceCode = base64.StdEncoding.EncodeToString([]byte(functionSourceCode))

	statusInternalServerError := http.StatusInternalServerError
	suite.DeployFunctionAndRequests(createFunctionOptions,
		[]*Request{
			{
				RequestMethod:              "GET",
				ExpectedResponseStatusCode: &statusInternalServerError,
				ExpectedResponseBody:       "json: cannot unmarshal number into Go struct field Result.content_type of type string",
			},
		})
}

func (suite *HTTPTestSuite) TestBatchedProcessing() {
	functionName := "batch-function"
	functionPath := path.Join(suite.GetTestFunctionsDir(),
		"python",
		"batch",
		"batch-http-func.py")
	createFunctionOptions := suite.GetDeployOptions("event_recorder",
		suite.GetFunctionPath(path.Join("event_recorder_python")))

	createFunctionOptions.FunctionConfig.Spec.Runtime = "python"
	createFunctionOptions.FunctionConfig.Meta.Name = functionName
	createFunctionOptions.FunctionConfig.Spec.Build.Path = functionPath
	createFunctionOptions.FunctionConfig.Spec.Triggers = map[string]functionconfig.Trigger{
		suite.triggerName: {
			Kind:       "http",
			Attributes: map[string]interface{}{},
			Batch: &functionconfig.BatchConfiguration{
				Mode:      functionconfig.BatchModeEnable,
				BatchSize: 2,
				Timeout:   "1ms",
			},
		},
	}
	statusOK := fasthttp.StatusOK
	suite.DeployFunctionAndRequests(createFunctionOptions,
		[]*Request{
			// Happy flows
			{
				RequestMethod:              "POST",
				RequestBody:                "hello-0",
				ExpectedResponseStatusCode: &statusOK,
				ExpectedResponseBody:       "Response to in-batch event",
			},
			{
				RequestMethod:              "POST",
				RequestBody:                "hello-1",
				ExpectedResponseStatusCode: &statusOK,
				ExpectedResponseBody:       "Response to in-batch event",
			},
		})

}

func (suite *HTTPTestSuite) getHTTPDeployOptions() *platform.CreateFunctionOptions {
	createFunctionOptions := suite.GetDeployOptions("event_recorder",
		suite.GetFunctionPath(path.Join("event_recorder_python")))

	createFunctionOptions.FunctionConfig.Spec.Runtime = "python"
	createFunctionOptions.FunctionConfig.Meta.Name = "http-trigger-test"
	createFunctionOptions.FunctionConfig.Spec.Build.Path = path.Join(suite.GetTestFunctionsDir(),
		"common",
		"event-recorder",
		"python",
		"event_recorder.py")
	createFunctionOptions.FunctionConfig.Spec.Triggers = map[string]functionconfig.Trigger{
		suite.triggerName: {
			Kind:       "http",
			Attributes: map[string]interface{}{},
		},
	}
	return createFunctionOptions
}

func TestIntegrationSuite(t *testing.T) {
	if testing.Short() {
		return
	}

	suite.Run(t, new(HTTPTestSuite))
}
