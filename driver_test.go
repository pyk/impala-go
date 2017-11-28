package impala

import (
	"reflect"
	"testing"
)

var dataDSN map[string]DSN = map[string]DSN{
	"impala://host=localhost&port=21050":       DSN{Host: "localhost", Port: 21050},
	"impala://host=h&port=1&username=u&pass=p": DSN{Host: "h", Port: 1, UserName: "u", Password: "p"},
}

func TestDSNParser(t *testing.T) {
	for input, expectedOutput := range dataDSN {
		output, _ := ParseDSN(input)

		if !reflect.DeepEqual(output, expectedOutput) {
			t.Errorf("parseDSN(%q) failed:\ngot:  %+v\nexpected: %+v", input,
				output, expectedOutput)
		}
	}
}
