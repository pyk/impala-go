package impala

import (
	"errors"
	"strconv"
	"strings"
)

// DSN stores structured Data Source Name
type DSN struct {
	Host     string
	Port     int
	UserName string
	Password string
}

// NewDSN returns DSN with default value
func NewDSN() DSN {
	return DSN{
		Host:     "localhost",
		Port:     21050,
		UserName: "",
		Password: "",
	}
}

// ParseDSN parse Impala Data Source Name
// Impala DSN format:
//
//     impala://param1=value1&paramN=valueN
//
// Available params:
//     host     - Hostname of the Impala server (default: "localhost")
//     port     - Port number of the Impala server (default: 21050)
//     username - Impala credentials (default: "")
//     pass     - Impala credentials (default: "")
func ParseDSN(source string) (DSN, error) {
	// Initialize DSN with default value
	dsn := NewDSN()

	// Make sure the keyword impala:// exists
	keywordImpala := "impala://"
	keywordIndex := strings.Index(source, keywordImpala)
	if keywordIndex != 0 {
		return DSN{}, errors.New("DSN: impala:// should be on the beginning")
	}
	// Parse the parameters
	keywordLen := len(keywordImpala)
	dsnParams := strings.Split(source[keywordLen:], "&")
	for _, s := range dsnParams {
		param := strings.Split(s, "=")
		if len(param) != 2 {
			return DSN{}, errors.New("DSN: params format should be name=value")
		}
		name := param[0]
		value := param[1]
		switch name {
		case "host":
			dsn.Host = value
		case "port":
			// Convert value to int
			port, err := strconv.Atoi(value)
			if err != nil {
				return DSN{}, errors.New("DSN: invalid port value")
			}
			dsn.Port = port
		case "username":
			dsn.UserName = value
		case "pass":
			dsn.Password = value
		default:
			return DSN{}, errors.New("DSN: invalid parameter name: " + name)
		}
	}

	return dsn, nil
}

// type Driver struct{}

// func (d Driver) Open()
