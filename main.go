// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"

	"github.com/redpanda-data/benthos/v4/public/service"

	// Import full suite of FOSS connect plugins
	_ "github.com/redpanda-data/connect/public/bundle/free/v4"

	// Or, in order to import both FOSS and enterprise plugins, replace the
	// above with:
	// _ "github.com/redpanda-data/connect/public/bundle/enterprise/v4"

	// Add your plugin packages here
	_ "rpanda-connect-native-plugin-example/aws"
	_ "rpanda-connect-native-plugin-example/bookmark"
)

func main() {
	service.RunCLI(context.Background())
}
