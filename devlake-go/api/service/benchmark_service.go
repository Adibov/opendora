package service

import (
	"github.com/devoteamnl/opendora/api/models"
	"github.com/devoteamnl/opendora/api/sql_client"
	"github.com/devoteamnl/opendora/api/sql_client/sql_queries"
)

type BenchmarkService struct {
	Client sql_client.ClientInterface
}

func (service BenchmarkService) ServeRequest(params ServiceParameters) (models.BenchmarkResponse, error) {

	typeQueryMap := map[string]string{
		"df":   sql_queries.BenchmarkDfSql,
		"mltc": sql_queries.BenchmarkMltcSql,
		"cfr":  sql_queries.BenchmarkCfrSql,
		"mttr": sql_queries.BenchmarkMttrSql,
	}

	query := typeQueryMap[params.TypeQuery]

	return service.Client.QueryBenchmark(query, sql_client.QueryParams{To: params.To, From: params.From, Project: params.Project})
}
