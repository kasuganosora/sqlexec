package main

import (
	"fmt"
	"log"
	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

func main() {
	db, err := api.NewDB(&api.DBConfig{
		CacheEnabled:  false,
		DefaultLogger: api.NewDefaultLogger(api.LogInfo),
		DebugMode:     false,
	})
	if err != nil {
		log.Fatalf("Failed to create DB: %v", err)
	}

	ds := createTestDataSource("testdb", []domain.ColumnInfo{
		{Name: "id", Type: "int", Primary: true},
		{Name: "name", Type: "varchar(100)", Nullable: false},
	})

	err = db.RegisterDataSource("testdb", ds)
	if err != nil {
		log.Fatalf("Failed to register testdb: %v", err)
	}

	session := db.Session()
	defer session.Close()

	fmt.Println("Testing USE statement...")
	_, err = session.Execute("USE testdb")
	if err != nil {
		log.Fatalf("USE failed: %v", err)
	}
	fmt.Println("USE succeeded!")

	fmt.Println("Testing simple query...")
	rows, err := session.QueryAll("SELECT * FROM test_table")
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("Query succeeded! Found %d rows\n", len(rows))
}

func createTestDataSource(name string, columns []domain.ColumnInfo) domain.DataSource {
	mds := api.NewMockDataSourceWithTableInfo(name, columns)
	err := mds.Connect(nil)
	if err != nil {
		log.Fatalf("Failed to connect datasource: %v", err)
	}
	return mds
}
