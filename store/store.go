package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"

	"cloud.google.com/go/bigtable"
	"github.com/bmooso/bigtable/request"
	"github.com/google/uuid"
)

type (
	MessageStore struct {
		client      *bigtable.Client
		adminClient *bigtable.AdminClient
		ctx         context.Context
		tableName   string
	}

	RowMetaData struct {
		ColumnFamilyName string
		Key              string
	}
)

func NewMessageStore(ctx context.Context, tableName string, project *string, instance *string) (*MessageStore, error) {

	cfName := "cf1"

	// Set up admin client, tables, and column families.
	// NewAdminClient uses Application Default Credentials to authenticate.
	adminClient, err := bigtable.NewAdminClient(ctx, *project, *instance)
	if err != nil {
		log.Fatalf("Could not create admin client: %v", err)
		return nil, err
	}

	initTable(ctx, adminClient, tableName, cfName)

	client, err := bigtable.NewClient(ctx, *project, *instance)
	if err != nil {
		log.Fatalf("Could not create data operations client: %v", err)
		return nil, err
	}

	return &MessageStore{
		client:      client,
		adminClient: adminClient,
		ctx:         ctx,
		tableName:   tableName,
	}, nil
}

func (ms MessageStore) Delete(rmd RowMetaData, columnName string, id string) error {

	row, _ := ms.Read(rmd, columnName, id)

	if _, ok := row[rmd.ColumnFamilyName]; !ok {
		return fmt.Errorf("Unable to find record to delete")
	}

	item := row[rmd.ColumnFamilyName][0]

	var msg request.Message

	if err := json.Unmarshal(item.Value, &msg); err != nil {
		return err
	}

	fmt.Println("Message: ", msg)

	err := ms.changeRow(rmd.ColumnFamilyName, fmt.Sprintf("deleted#%s", item.Row), columnName, item.Value)

	if err != nil {
		return err
	}

	return ms.deleteRow(fmt.Sprintf("%v%v", rmd.Key, id))

}

func deletedKey(r RowMetaData) RowMetaData {
	return RowMetaData{
		ColumnFamilyName: r.ColumnFamilyName,
		Key:              fmt.Sprintf("deleted#%s", r.Key),
	}
}

func (ms MessageStore) ReadSingle(rmd RowMetaData, columnName string, id string) error {
	row, err := ms.Read(rmd, columnName, id)
	if err != nil {
		return err
	}

	if _, ok := row[rmd.ColumnFamilyName]; !ok {
		return fmt.Errorf("Unable to find record")
	}

	item := row[rmd.ColumnFamilyName][0]
	log.Printf("\t%s = %s;%v\n", item.Row, string(item.Value), item.Timestamp)

	return nil

}

func (ms MessageStore) Read(rmd RowMetaData, columnName string, id string) (bigtable.Row, error) {
	tbl := ms.client.Open(ms.tableName)
	return tbl.ReadRow(ms.ctx, fmt.Sprintf("%s%s", rmd.Key, id), bigtable.RowFilter(bigtable.ColumnFilter(columnName)))
}

func (ms MessageStore) ReadAll(rmd RowMetaData, columnName string) (map[string][]byte, error) {
	tbl := ms.client.Open(ms.tableName)

	m := make(map[string][]byte)

	err := tbl.ReadRows(ms.ctx, bigtable.PrefixRange(rmd.Key), func(row bigtable.Row) bool {
		item := row[rmd.ColumnFamilyName][0]

		z := len(item.Row) - 36
		m[item.Row[z:]] = item.Value
		return true
	}, bigtable.RowFilter(bigtable.ColumnFilter(columnName)))

	if err != nil {
		return nil, err
	}

	return m, nil
}

func (ms MessageStore) ReadAllDeleted(rmd RowMetaData, columnName string) error {
	tbl := ms.client.Open(ms.tableName)

	return tbl.ReadRows(ms.ctx, bigtable.PrefixRange("deleted#"), func(row bigtable.Row) bool {
		item := row[rmd.ColumnFamilyName][0]
		log.Printf("\t%s = %s;%v\n", item.Row, string(item.Value), item.Timestamp)
		return true
	}, bigtable.RowFilter(bigtable.ColumnFilter(columnName)))
}

func (ms MessageStore) Update(rmd RowMetaData, id string, data interface{}) error {
	payload, _ := json.Marshal(data)

	col := getType(data)

	return ms.changeRow(rmd.ColumnFamilyName, fmt.Sprintf("%v%v", rmd.Key, id), col, payload)
}

func (ms MessageStore) CreateNew(rmd RowMetaData, data interface{}) error {
	payload, _ := json.Marshal(data)

	id := uuid.New().String()

	col := getType(data)

	return ms.changeRow(rmd.ColumnFamilyName, fmt.Sprintf("%v%v", rmd.Key, id), col, payload)
}

func (ms MessageStore) changeRow(columnFamilyName string, key string, columnName string, data []byte) error {

	tbl := ms.client.Open(ms.tableName)

	mut := bigtable.NewMutation()

	mut.Set(columnFamilyName, columnName, bigtable.Now(), data)
	return tbl.Apply(ms.ctx, key, mut)
}

func (ms MessageStore) deleteRow(key string) error {

	tbl := ms.client.Open(ms.tableName)

	mut := bigtable.NewMutation()

	mut.DeleteRow()

	return tbl.Apply(ms.ctx, key, mut)
}

func (ms MessageStore) TearDown() error {
	if err := ms.client.Close(); err != nil {
		log.Fatalf("Could not close data operations client: %v", err)
	}

	log.Printf("Deleting the table")
	if err := ms.adminClient.DeleteTable(ms.ctx, ms.tableName); err != nil {
		log.Fatalf("Could not delete table %s: %v", ms.tableName, err)
	}

	if err := ms.adminClient.Close(); err != nil {
		log.Fatalf("Could not close admin client: %v", err)
	}

	return fmt.Errorf("Shutting down server")
}

func getType(myvar interface{}) string {
	if t := reflect.TypeOf(myvar); t.Kind() == reflect.Ptr {
		return t.Elem().Name()
	} else {
		return t.Name()
	}
}

func initTable(ctx context.Context, adminClient *bigtable.AdminClient, tableName string, columnFamilyName string) {

	tables, err := adminClient.Tables(ctx)
	if err != nil {
		log.Fatalf("Could not fetch table list: %v", err)
	}

	if !sliceContains(tables, tableName) {
		log.Printf("Creating table %s", tableName)
		if err := adminClient.CreateTable(ctx, tableName); err != nil {
			log.Fatalf("Could not create table %s: %v", tableName, err)
		}
	}

	tblInfo, err := adminClient.TableInfo(ctx, tableName)
	if err != nil {
		log.Fatalf("Could not read info for table %s: %v", tableName, err)
	}

	if !sliceContains(tblInfo.Families, columnFamilyName) {
		if err := adminClient.CreateColumnFamily(ctx, tableName, columnFamilyName); err != nil {
			log.Fatalf("Could not create column family %s: %v", columnFamilyName, err)
		}
	}
}

// sliceContains reports whether the provided string is present in the given slice of strings.
func sliceContains(list []string, target string) bool {
	for _, s := range list {
		if s == target {
			return true
		}
	}
	return false
}
