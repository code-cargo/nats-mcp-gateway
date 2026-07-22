//   Copyright 2026 BoxBuild Inc DBA CodeCargo
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package main

import (
	"github.com/alecthomas/kong"

	"github.com/code-cargo/nats-mcp-gateway/cmd"
)

var version = "dev"

func main() {
	cli := cmd.CLI{}
	ctx := kong.Parse(&cli,
		kong.Name("natsmcp"),
		kong.Description("NATS MCP Gateway - front MCP servers over a NATS fabric"),
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{
			Compact: true,
		}),
		kong.Vars{
			"version": version,
		})

	cli.Gateway.Version = version

	err := ctx.Run(&cli.Globals)
	ctx.FatalIfErrorf(err)
}
