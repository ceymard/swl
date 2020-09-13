package swllib

import (
	"fmt"
	"strings"
)

type SQLTableRequest struct {
	Colname string
	Query   string
}

func ParseSQLTableRequests(args []string) []SQLTableRequest {
	var (
		res = make([]SQLTableRequest, 0, 24)
	)

	for _, s := range args {
		var (
			query string
		)
		query = s
		if strings.Contains(s, ":") {
			var parts = strings.SplitN(s, ":", 2)
			s = parts[0]
			query = parts[1]
			if strings.Index(strings.ToLower(strings.TrimSpace(query)), "select") != 0 {
				query = fmt.Sprintf(`SELECT * FROM "%s"`, query)
			}
		} else {
			query = fmt.Sprintf(`SELECT * FROM "%s"`, query)
		}

		res = append(res, SQLTableRequest{
			Colname: s,
			Query:   query,
		})
	}

	return res

}

type SqlRows interface {
	Scan(args ...interface{}) error
}

func SqlRowsToMap(cols []string, rows SqlRows) (map[string]interface{}, error) {
	// Create a slice of interface{}'s to represent each column,
	// and a second slice to contain pointers to each item in the columns slice.
	columns := make([]interface{}, len(cols))
	columnPointers := make([]interface{}, len(cols))
	for i, l := 0, len(columns); i < l; i++ {
		columnPointers[i] = &columns[i]
	}

	// Scan the result into the column pointers...
	if err := rows.Scan(columnPointers...); err != nil {
		return nil, err
	}

	// Create our map, and retrieve the value for each column from the pointers slice,
	// storing it in the map with the name of the column as the key.
	m := make(map[string]interface{})
	for i, colName := range cols {
		val := columnPointers[i].(*interface{})
		m[colName] = *val
	}

	return m, nil
}
